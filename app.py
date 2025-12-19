#!/usr/bin/env python3
"""
bot_flask_webhook.py

Flask + Telebot webhook version of the Postgres-backed bot.

Environment variables (minimum):
- TELEGRAM_TOKEN (required)
- DATABASE_URL (required)  e.g. postgresql://user:pass@host/db
- ADMIN_ID (optional; default 7301067810)
- WEBHOOK_URL (optional; if not provided the script will try to build it from RENDER_EXTERNAL_URL)
- SEARCH_FILE (optional; default 'logs.txt')
- SEARCH_DIR (optional; if set, reads all .txt files in this dir)
- ... other env var options from prior versions (see below)

This file starts a Flask app (webhook endpoints) and background threads for:
- loader (reload logs)
- cleanup (expired keys/users)
- watchlist alerts
"""
import os
import io
import re
import time
import random
import logging
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional

from flask import Flask, request, jsonify
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from sqlalchemy import (
    create_engine, Column, String, Integer, DateTime, Boolean, Text, func
)
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session

# ---------------- ENV / CONFIG ----------------
TOKEN = os.environ.get("TELEGRAM_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7301067810"))

# Webhook URL where Telegram will POST updates. Should be https://yourapp/.../<TOKEN>
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # optional; fallback to RENDER_EXTERNAL_URL if available

SEARCH_FILE = os.environ.get("SEARCH_FILE", "logs.txt")
SEARCH_DIR = os.environ.get("SEARCH_DIR", "")  # if set, read .txt files in this directory instead of SEARCH_FILE
RELOAD_INTERVAL = int(os.environ.get("RELOAD_INTERVAL_SECS", "300"))
SAMPLE_LINES_LIMIT = int(os.environ.get("SAMPLE_LINES_LIMIT", "1000"))
MIN_SEARCH_INTERVAL_SECS = int(os.environ.get("MIN_SEARCH_INTERVAL_SECS", "5"))
MAX_RESULTS_PER_KEYWORD = int(os.environ.get("MAX_RESULTS_PER_KEYWORD", "500"))
MAX_KEYWORDS_PER_SEARCH = int(os.environ.get("MAX_KEYWORDS_PER_SEARCH", "8"))
MAX_SEARCHES_PER_DAY = int(os.environ.get("MAX_SEARCHES_PER_DAY", "50"))

DB_CLEANUP_INTERVAL = int(os.environ.get("DB_CLEANUP_INTERVAL_SECS", "3600"))
AUTO_CLEAN_EXPIRED_KEYS = os.environ.get("AUTO_CLEAN_EXPIRED_KEYS", "true").lower() in ("1", "true", "yes")
WATCH_ALERT_CHAT_ID = int(os.environ.get("WATCH_ALERT_CHAT_ID", str(ADMIN_ID)))
ALERT_ON_ERROR = os.environ.get("ALERT_ON_ERROR", "true").lower() in ("1", "true", "yes")
SET_WEBHOOK_ON_STARTUP = os.environ.get("SET_WEBHOOK_ON_STARTUP", "true").lower() in ("1", "true", "yes")
RUN_BACKGROUND_JOBS = os.environ.get("RUN_BACKGROUND_JOBS", "true").lower() in ("1", "true", "yes")

if not TOKEN:
    raise SystemExit("TELEGRAM_TOKEN is required.")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL is required.")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("bot_flask_webhook")

# ---------------- Flask + Telebot ----------------
app = Flask(__name__)
bot = telebot.TeleBot(TOKEN)

# ---------------- Database (SQLAlchemy) ----------------
Base = declarative_base()
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

# ---------------- Models ----------------
class Key(Base):
    __tablename__ = "keys"
    key = Column(String, primary_key=True, index=True)
    expires = Column(DateTime, nullable=False)
    redeemed_by = Column(Integer, nullable=True)

class User(Base):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True, index=True)
    expires = Column(DateTime, nullable=False)
    banned = Column(Boolean, default=False)

class Usage(Base):
    __tablename__ = "usage"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, index=True)
    date = Column(DateTime, index=True)  # midnight UTC
    count = Column(Integer, default=0)

class SearchHistory(Base):
    __tablename__ = "search_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, index=True)
    keyword = Column(Text)
    is_regex = Column(Boolean, default=False)
    results_count = Column(Integer, default=0)
    ts = Column(DateTime, default=datetime.utcnow)

class Watchlist(Base):
    __tablename__ = "watchlist"
    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(Text, unique=True)
    is_regex = Column(Boolean, default=False)

# Ensure tables exist
Base.metadata.create_all(bind=engine)

# ---------------- In-memory search pool ----------------
total_lines: List[str] = []
lines_lock = threading.Lock()
previous_sent_lines: Dict[str, Set[str]] = {}
last_search_time: Dict[int, float] = {}

# ---------------- DB helpers ----------------
def db_session():
    return SessionLocal()

def create_key_in_db(key_str: str, expires_dt: datetime) -> bool:
    s = db_session()
    try:
        k = Key(key=key_str, expires=expires_dt, redeemed_by=None)
        s.add(k)
        s.commit()
        return True
    except Exception:
        s.rollback()
        logger.exception("create_key_in_db error")
        return False
    finally:
        s.close()

def find_key_record(key_str: str) -> Optional[Key]:
    s = db_session()
    try:
        return s.get(Key, key_str)
    finally:
        s.close()

def delete_key_record(key_str: str):
    s = db_session()
    try:
        rec = s.get(Key, key_str)
        if rec:
            s.delete(rec)
            s.commit()
    except Exception:
        s.rollback()
        logger.exception("delete_key_record error")
    finally:
        s.close()

def upsert_user(user_id: int, expires_dt: datetime):
    s = db_session()
    try:
        u = s.get(User, user_id)
        if u:
            u.expires = expires_dt
            u.banned = False
        else:
            u = User(user_id=user_id, expires=expires_dt, banned=False)
            s.add(u)
        s.commit()
    except Exception:
        s.rollback()
        logger.exception("upsert_user error")
    finally:
        s.close()

def get_user_record(user_id: int) -> Optional[User]:
    s = db_session()
    try:
        return s.get(User, user_id)
    finally:
        s.close()

def remove_user(user_id: int):
    s = db_session()
    try:
        u = s.get(User, user_id)
        if u:
            s.delete(u)
            s.commit()
    except Exception:
        s.rollback()
        logger.exception("remove_user error")
    finally:
        s.close()

def ban_user(user_id: int):
    s = db_session()
    try:
        u = s.get(User, user_id)
        if u:
            u.banned = True
        else:
            u = User(user_id=user_id, expires=datetime.utcnow() - timedelta(days=1), banned=True)
            s.add(u)
        s.commit()
    except Exception:
        s.rollback()
        logger.exception("ban_user error")
    finally:
        s.close()

def unban_user(user_id: int):
    s = db_session()
    try:
        u = s.get(User, user_id)
        if u:
            u.banned = False
            s.commit()
    except Exception:
        s.rollback()
        logger.exception("unban_user error")
    finally:
        s.close()

# Usage tracking
def increment_usage(user_id: int) -> int:
    s = db_session()
    try:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        rec = s.query(Usage).filter(Usage.user_id == user_id, Usage.date == today).first()
        if not rec:
            rec = Usage(user_id=user_id, date=today, count=1)
            s.add(rec)
        else:
            rec.count += 1
        s.commit()
        return rec.count
    except Exception:
        s.rollback()
        logger.exception("increment_usage error")
        return 999999
    finally:
        s.close()

def get_usage_count(user_id: int) -> int:
    s = db_session()
    try:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        rec = s.query(Usage).filter(Usage.user_id == user_id, Usage.date == today).first()
        return rec.count if rec else 0
    finally:
        s.close()

def record_search_history(user_id: int, keyword: str, is_regex: bool, results_count: int):
    s = db_session()
    try:
        rec = SearchHistory(user_id=user_id, keyword=keyword, is_regex=is_regex, results_count=results_count)
        s.add(rec)
        s.commit()
    except Exception:
        s.rollback()
        logger.exception("record_search_history error")
    finally:
        s.close()

def get_search_history(user_id: int, limit: int = 10) -> List[SearchHistory]:
    s = db_session()
    try:
        return s.query(SearchHistory).filter(SearchHistory.user_id == user_id).order_by(SearchHistory.ts.desc()).limit(limit).all()
    finally:
        s.close()

def add_watch_keyword(keyword: str, is_regex: bool) -> bool:
    s = db_session()
    try:
        k = Watchlist(keyword=keyword, is_regex=is_regex)
        s.add(k)
        s.commit()
        return True
    except Exception:
        s.rollback()
        logger.exception("add_watch_keyword error")
        return False
    finally:
        s.close()

def remove_watch_keyword(keyword: str) -> bool:
    s = db_session()
    try:
        rec = s.query(Watchlist).filter(Watchlist.keyword == keyword).first()
        if rec:
            s.delete(rec)
            s.commit()
            return True
        return False
    except Exception:
        s.rollback()
        logger.exception("remove_watch_keyword error")
        return False
    finally:
        s.close()

def list_watch_keywords() -> List[Watchlist]:
    s = db_session()
    try:
        return s.query(Watchlist).all()
    finally:
        s.close()

def count_unused_keys() -> int:
    s = db_session()
    try:
        return s.query(Key).filter(Key.redeemed_by == None).count()
    finally:
        s.close()

def count_active_users() -> int:
    s = db_session()
    try:
        return s.query(User).count()
    finally:
        s.close()

def cleanup_expired_users_and_keys():
    s = db_session()
    try:
        now = datetime.utcnow()
        expired_users = s.query(User).filter(User.expires < now).all()
        n_users = len(expired_users)
        for u in expired_users:
            s.delete(u)
        n_keys = 0
        if AUTO_CLEAN_EXPIRED_KEYS:
            expired_keys = s.query(Key).filter(Key.expires < now, Key.redeemed_by == None).all()
            n_keys = len(expired_keys)
            for k in expired_keys:
                s.delete(k)
        s.commit()
        logger.info("Cleanup removed %d expired users and %d expired keys", n_users, n_keys)
        return n_users, n_keys
    except Exception:
        s.rollback()
        logger.exception("cleanup error")
        return 0, 0
    finally:
        s.close()

# ---------------- File loader (single file or directory) ----------------
def load_lines_once():
    new_lines = []
    try:
        if SEARCH_DIR:
            if not os.path.exists(SEARCH_DIR):
                logger.warning("SEARCH_DIR not found: %s", SEARCH_DIR)
            else:
                for fn in os.listdir(SEARCH_DIR):
                    if fn.lower().endswith(".txt"):
                        path = os.path.join(SEARCH_DIR, fn)
                        try:
                            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                                for ln in f:
                                    ln = ln.strip()
                                    if ln:
                                        new_lines.append(ln)
                        except Exception:
                            logger.exception("Error reading file %s", path)
        else:
            if not os.path.exists(SEARCH_FILE):
                logger.warning("SEARCH_FILE not found: %s", SEARCH_FILE)
            else:
                with open(SEARCH_FILE, "r", encoding="utf-8", errors="ignore") as f:
                    for ln in f:
                        ln = ln.strip()
                        if ln:
                            new_lines.append(ln)
    except Exception:
        logger.exception("load_lines_once error")
        return

    with lines_lock:
        existing = set(total_lines)
        added = 0
        for ln in new_lines:
            if ln not in existing:
                total_lines.append(ln)
                added += 1
    logger.info("Loaded lines (+%d) total=%d", added, len(total_lines))

def periodic_loader():
    while True:
        try:
            load_lines_once()
        except Exception:
            logger.exception("periodic_loader error")
        time.sleep(RELOAD_INTERVAL)

# Start loader thread if allowed
if RUN_BACKGROUND_JOBS:
    loader_thread = threading.Thread(target=periodic_loader, daemon=True)
    loader_thread.start()
    load_lines_once()

# ---------------- Watcher thread ----------------
def watch_job():
    logger.info("Watch job started")
    seen_alerts: Set[str] = set()
    while True:
        try:
            items = list_watch_keywords()
            if not items:
                time.sleep(30)
                continue
            with lines_lock:
                pool = list(total_lines)
            for item in items:
                kw = item.keyword
                is_re = item.is_regex
                for ln in pool:
                    hit = False
                    try:
                        if is_re:
                            if re.search(kw, ln, re.IGNORECASE):
                                hit = True
                        else:
                            if kw.lower() in ln.lower():
                                hit = True
                    except re.error:
                        logger.warning("Invalid regex in watchlist: %s", kw)
                        continue
                    if hit:
                        key = f"{item.id}:{hash(ln)}"
                        if key not in seen_alerts:
                            seen_alerts.add(key)
                            try:
                                bot.send_message(WATCH_ALERT_CHAT_ID, f"🔔 Watch alert: `{kw}` matched:\n`{ln}`", parse_mode="Markdown")
                            except Exception:
                                logger.exception("Failed sending watch alert")
            time.sleep(15)
        except Exception:
            logger.exception("watch_job error")
            time.sleep(10)

if RUN_BACKGROUND_JOBS:
    watch_thread = threading.Thread(target=watch_job, daemon=True)
    watch_thread.start()

# ---------------- Utilities ----------------
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def rate_limited(user_id: int) -> (bool, int):
    now = time.time()
    last = last_search_time.get(user_id, 0)
    delta = now - last
    if delta < MIN_SEARCH_INTERVAL_SECS:
        return True, int(MIN_SEARCH_INTERVAL_SECS - delta)
    return False, 0

def update_search_time(user_id: int):
    last_search_time[user_id] = time.time()

def remaining_quota(user_id: int) -> int:
    used = get_usage_count(user_id)
    return max(0, MAX_SEARCHES_PER_DAY - used)

# ---------------- Search logic ----------------
def compile_keywords(raw_keywords: List[str]) -> List[Dict]:
    out = []
    for rk in raw_keywords:
        rk = rk.strip()
        if not rk:
            continue
        is_re = False
        pattern = rk
        if rk.startswith("re:"):
            is_re = True
            pattern = rk[3:]
        elif rk.startswith("-r "):
            is_re = True
            pattern = rk[3:]
        compiled = None
        if is_re:
            try:
                compiled = re.compile(pattern, re.IGNORECASE)
            except re.error:
                compiled = None
        out.append({"raw": rk, "is_regex": is_re, "pattern": compiled or pattern})
    return out

def search_keywords_struct(keywords_struct: List[Dict]) -> Dict[str, List[str]]:
    results: Dict[str, List[str]] = {}
    with lines_lock:
        pool = list(total_lines)
    if not pool:
        return results
    sample_count = min(SAMPLE_LINES_LIMIT, len(pool))
    sample = random.sample(pool, sample_count)
    sample_lower = [s.lower() for s in sample]

    for kwobj in keywords_struct:
        raw = kwobj["raw"]
        is_re = kwobj["is_regex"]
        pat = kwobj["pattern"]
        found: List[str] = []
        seen = previous_sent_lines.get(raw.lower(), set())
        if is_re and isinstance(pat, re.Pattern):
            for ln in sample:
                if ln in seen:
                    continue
                try:
                    if pat.search(ln):
                        found.append(ln)
                except Exception:
                    continue
                if len(found) >= MAX_RESULTS_PER_KEYWORD:
                    break
        else:
            substr = pat.lower() if isinstance(pat, str) else str(pat).lower()
            for ln_lower, ln in zip(sample_lower, sample):
                if ln in seen:
                    continue
                if substr in ln_lower:
                    found.append(ln)
                if len(found) >= MAX_RESULTS_PER_KEYWORD:
                    break
        results[raw] = found
    return results

def send_results_in_memory(chat_id: int, user_id: int, results: Dict[str, List[str]]):
    total_found = sum(len(v) for v in results.values())
    if total_found == 0:
        bot.send_message(chat_id, "❌ No new results found for the provided keyword(s).")
        return
    parts = []
    for kw, lines in results.items():
        parts.append(f"=== RESULTS for: {kw} ({len(lines)} lines) ===")
        parts.extend(lines)
        parts.append("")
    content = "\n".join(parts)
    bio = io.BytesIO(content.encode("utf-8"))
    bio.name = f"Results_{user_id}_{int(time.time())}.txt"
    bio.seek(0)
    try:
        bot.send_document(chat_id, bio, caption=f"📄 Search results — total {total_found} lines (capped).")
    except Exception:
        logger.exception("send_document error")
        try:
            bot.send_message(chat_id, "⚠️ Error sending results.")
        except Exception:
            logger.exception("Failed to notify user after send error")
    with lines_lock:
        for kw, lines in results.items():
            key = kw.lower()
            if key not in previous_sent_lines:
                previous_sent_lines[key] = set()
            for ln in lines:
                previous_sent_lines[key].add(ln)
                if ln in total_lines:
                    total_lines.remove(ln)

# ---------------- Telegram Handlers (commands) ----------------
@bot.message_handler(commands=["createkey"])
def cmd_createkey(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(message, "Usage: /createkey <days> <count>")
            return
        _, days_s, count_s = parts
        days = int(days_s); count = int(count_s)
        new = []
        for _ in range(count):
            key = f"KEY-{random.randint(100000,999999)}"
            expires = datetime.utcnow() + timedelta(days=days)
            ok = create_key_in_db(key, expires)
            if ok:
                new.append(f"{key} (expires {expires.strftime('%Y-%m-%d %H:%M:%S')} UTC)")
        if new:
            bot.reply_to(message, "✅ Keys created:\n" + "\n".join(new))
        else:
            bot.reply_to(message, "⚠️ Failed to create keys.")
    except Exception:
        logger.exception("cmd_createkey error")
        bot.reply_to(message, "⚠️ Error creating keys.")

@bot.message_handler(commands=["redeem"])
def cmd_redeem(message):
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "Usage: /redeem <KEYCODE>")
            return
        _, provided = parts
        rec = find_key_record(provided)
        if rec and rec.redeemed_by is None:
            upsert_user(message.from_user.id, rec.expires)
            delete_key_record(provided)
            bot.reply_to(message, f"✅ Redeemed until {rec.expires.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        else:
            bot.reply_to(message, "❌ Invalid or already redeemed key.")
    except Exception:
        logger.exception("cmd_redeem error")
        bot.reply_to(message, "⚠️ Error redeeming key.")

@bot.message_handler(commands=["start"])
def cmd_start(message):
    rec = get_user_record(message.from_user.id)
    if not rec or datetime.utcnow() > rec.expires or rec.banned:
        bot.send_message(message.chat.id, "❌ You need a valid key (or you are banned). Use /redeem <key>.", parse_mode="Markdown")
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🎮 Generate Account", callback_data="generate"))
    markup.add(InlineKeyboardButton("📞 Contact Owner", url="https://t.me/Onlyjoshacc"))
    markup.add(InlineKeyboardButton("ℹ️ Info", callback_data="info"))
    markup.add(InlineKeyboardButton("📊 See Status", callback_data="status"))
    bot.send_message(message.chat.id, "Welcome! Choose an option below.", reply_markup=markup)

@bot.message_handler(commands=["search"])
def cmd_search(message):
    try:
        user_id = message.from_user.id
        rec = get_user_record(user_id)
        if not rec or datetime.utcnow() > rec.expires or (rec.banned if rec else False):
            bot.reply_to(message, "❌ You need a valid key (or you are banned). Use /redeem <key>.")
            return
        rest = message.text[len("/search"):].strip()
        if not rest:
            bot.reply_to(message, "Usage: /search <keyword1[,keyword2,...]> (use re:prefix for regex)")
            return
        limited, secs = rate_limited(user_id)
        if limited:
            bot.reply_to(message, f"⏳ Slow down — try again in {secs} seconds.")
            return
        used = get_usage_count(user_id)
        if used >= MAX_SEARCHES_PER_DAY:
            bot.reply_to(message, f"❌ Daily quota reached ({MAX_SEARCHES_PER_DAY}). Try tomorrow or contact admin.")
            return
        raw_keywords = [k.strip() for k in rest.split(",") if k.strip()]
        if not raw_keywords:
            bot.reply_to(message, "Provide at least one keyword.")
            return
        if len(raw_keywords) > MAX_KEYWORDS_PER_SEARCH:
            bot.reply_to(message, f"❗Too many keywords. Max {MAX_KEYWORDS_PER_SEARCH}.")
            return
        kw_struct = compile_keywords(raw_keywords)
        for k in kw_struct:
            if k["is_regex"] and not isinstance(k["pattern"], re.Pattern):
                bot.reply_to(message, f"❌ Invalid regex: `{k['raw']}`", parse_mode="Markdown")
                return
        results = search_keywords_struct(kw_struct)
        increment_usage(user_id)
        for raw in raw_keywords:
            rc = len(results.get(raw, []))
            record_search_history(user_id, raw, raw.startswith("re:") or raw.startswith("-r "), rc)
        update_search_time(user_id)
        send_results_in_memory(message.chat.id, user_id, results)
    except Exception as e:
        logger.exception("cmd_search error")
        bot.reply_to(message, "⚠️ Error during search.")
        if ALERT_ON_ERROR:
            try:
                bot.send_message(ADMIN_ID, f"⚠️ Bot error: {e}")
            except Exception:
                logger.exception("Failed to notify admin about error")

# Additional admin commands: history, quota, ban/unban, stats, listkeys, listusers, reload, cleanup, watch
@bot.message_handler(commands=["history"])
def cmd_history(message):
    try:
        parts = message.text.split()
        if len(parts) == 1:
            target = message.from_user.id
        else:
            if not is_admin(message.from_user.id):
                bot.reply_to(message, "❌ Admin only to view others' history.")
                return
            try:
                target = int(parts[1])
            except ValueError:
                bot.reply_to(message, "Usage: /history or /history <user_id>")
                return
        rows = get_search_history(target, limit=20)
        if not rows:
            bot.reply_to(message, "No history found.")
            return
        txt = []
        for r in rows:
            ts = r.ts.strftime("%Y-%m-%d %H:%M:%S")
            txt.append(f"{ts} — {'REGEX' if r.is_regex else 'KW'} `{r.keyword}` — {r.results_count} results")
        bot.reply_to(message, "Search history:\n" + "\n".join(txt), parse_mode="Markdown")
    except Exception:
        logger.exception("cmd_history error")
        bot.reply_to(message, "⚠️ Error fetching history.")

@bot.message_handler(commands=["quota"])
def cmd_quota(message):
    try:
        parts = message.text.split()
        if len(parts) == 1:
            target = message.from_user.id
        else:
            if not is_admin(message.from_user.id):
                bot.reply_to(message, "❌ Admin only to view others' quota.")
                return
            try:
                target = int(parts[1])
            except ValueError:
                bot.reply_to(message, "Usage: /quota or /quota <user_id>")
                return
        used = get_usage_count(target)
        left = max(0, MAX_SEARCHES_PER_DAY - used)
        bot.reply_to(message, f"🔢 User {target} — used {used} / {MAX_SEARCHES_PER_DAY} — remaining {left}")
    except Exception:
        logger.exception("cmd_quota error")
        bot.reply_to(message, "⚠️ Error fetching quota.")

@bot.message_handler(commands=["ban"])
def cmd_ban(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "Usage: /ban <user_id>")
            return
        uid = int(parts[1])
        ban_user(uid)
        bot.reply_to(message, f"✅ User {uid} banned.")
    except Exception:
        logger.exception("cmd_ban error")
        bot.reply_to(message, "⚠️ Error banning user.")

@bot.message_handler(commands=["unban"])
def cmd_unban(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "Usage: /unban <user_id>")
            return
        uid = int(parts[1])
        unban_user(uid)
        bot.reply_to(message, f"✅ User {uid} unbanned.")
    except Exception:
        logger.exception("cmd_unban error")
        bot.reply_to(message, "⚠️ Error unbanning user.")

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        with lines_lock:
            total_count = len(total_lines)
        users = count_active_users()
        keys = count_unused_keys()
        bot.reply_to(message, f"📊 Stats:\n• Lines: {total_count}\n• Active users: {users}\n• Unused keys: {keys}")
    except Exception:
        logger.exception("cmd_stats error")
        bot.reply_to(message, "⚠️ Error fetching stats.")

@bot.message_handler(commands=["listkeys"])
def cmd_listkeys(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        total = count_unused_keys()
        s = db_session()
        try:
            sample_rows = s.query(Key).filter(Key.redeemed_by == None).limit(10).all()
            sample = [f"{r.key} (exp {r.expires.strftime('%Y-%m-%d')})" for r in sample_rows]
        finally:
            s.close()
        safe_sample = "\n".join(sample) if sample else "(none)"
        bot.reply_to(message, f"🔑 Unused keys: {total}\nSample (up to 10):\n{safe_sample}")
    except Exception:
        logger.exception("cmd_listkeys error")
        bot.reply_to(message, "⚠️ Error listing keys.")

@bot.message_handler(commands=["listusers"])
def cmd_listusers(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        users = count_active_users()
        bot.reply_to(message, f"👥 Active users in DB: {users}")
    except Exception:
        logger.exception("cmd_listusers error")
        bot.reply_to(message, "⚠️ Error listing users.")

@bot.message_handler(commands=["reload"])
def cmd_reload(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        load_lines_once()
        bot.reply_to(message, f"✅ Reloaded. Total lines: {len(total_lines)}")
    except Exception:
        logger.exception("cmd_reload error")
        bot.reply_to(message, "⚠️ Error reloading.")

@bot.message_handler(commands=["cleanup"])
def cmd_cleanup(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        u_removed, k_removed = cleanup_expired_users_and_keys()
        bot.reply_to(message, f"✅ Cleanup done. Removed users: {u_removed}, keys: {k_removed}")
    except Exception:
        logger.exception("cmd_cleanup error")
        bot.reply_to(message, "⚠️ Error performing cleanup.")

@bot.message_handler(commands=["watch"])
def cmd_watch(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Admin only.")
        return
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /watch add|remove|list <keyword>")
            return
        op = parts[1].lower()
        if op == "list":
            items = list_watch_keywords()
            if not items:
                bot.reply_to(message, "Watchlist empty.")
                return
            lines = []
            for it in items:
                lines.append(f"{it.id}: {'REGEX' if it.is_regex else 'KW'} `{it.keyword}`")
            bot.reply_to(message, "Watchlist:\n" + "\n".join(lines), parse_mode="Markdown")
            return
        if len(parts) != 3:
            bot.reply_to(message, "Usage: /watch add|remove <keyword>")
            return
        kw = parts[2].strip()
        is_re = False
        if kw.startswith("re:") or kw.startswith("-r "):
            is_re = True
            kw = kw[3:]
        if op == "add":
            ok = add_watch_keyword(kw, is_re)
            bot.reply_to(message, "✅ Added to watchlist." if ok else "⚠️ Failed to add (maybe exists).")
        elif op == "remove":
            ok = remove_watch_keyword(kw)
            bot.reply_to(message, "✅ Removed from watchlist." if ok else "⚠️ Not found.")
        else:
            bot.reply_to(message, "Usage: /watch add|remove|list <keyword>")
    except Exception:
        logger.exception("cmd_watch error")
        bot.reply_to(message, "⚠️ Error managing watchlist.")

# Inline callback compatibility
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    try:
        chat_id = call.message.chat.id
        data = call.data
        if data == "status":
            with lines_lock:
                total_count = len(total_lines)
            try:
                members = bot.get_chat_members_count(chat_id)
            except Exception:
                members = "N/A"
            bot.send_message(chat_id, f"📊 Bot status:\n• Lines: {total_count}\n• Members: {members}")
        elif data == "generate":
            choose_keyword(chat_id)
        elif data == "info":
            bot.send_message(chat_id, "Bot: searches logs for keywords. Dev: @Onlyjoshacc\nVersion: webhook")
        elif data == "own_keyword":
            msg = bot.send_message(chat_id, "Send the keyword you want to search for (comma-separated allowed):")
            bot.register_next_step_handler(msg, search_keyword_user_input)
        elif data in ["garena.com","mtacc","roblox.com","facebook.com","supercell.com","netflix.com","tiktok.com"]:
            # emulate /search <keyword>
            fake = type("M", (), {"chat": call.message.chat, "from_user": call.from_user, "text": f"/search {data}"})
            cmd_search(fake)
    except Exception:
        logger.exception("callback_query error")

def choose_keyword(chat_id):
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("🕹️ Garena", callback_data="garena.com"), InlineKeyboardButton("🎮 MTACC", callback_data="mtacc"))
    markup.row(InlineKeyboardButton("🤖 Roblox", callback_data="roblox.com"), InlineKeyboardButton("📘 Facebook", callback_data="facebook.com"))
    markup.row(InlineKeyboardButton("🔥 Supercell", callback_data="supercell.com"), InlineKeyboardButton("🎬 Netflix", callback_data="netflix.com"))
    markup.row(InlineKeyboardButton("🎵 TikTok", callback_data="tiktok.com"), InlineKeyboardButton("🔍 Own Keyword", callback_data="own_keyword"))
    bot.send_message(chat_id, "Please choose a keyword:", reply_markup=markup)

def search_keyword_user_input(message):
    text = message.text.strip()
    if not text:
        bot.reply_to(message, "No keyword provided.")
        return
    fake_msg = message
    fake_msg.text = f"/search {text}"
    cmd_search(fake_msg)

# ---------------- Flask endpoints (webhook) ----------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({"ok": True, "msg": "Bot webhook alive"}), 200

@app.route(f"/{TOKEN}", methods=["POST"])
def telegram_webhook():
    try:
        json_str = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception:
        logger.exception("Failed processing update")
        if ALERT_ON_ERROR:
            try:
                bot.send_message(ADMIN_ID, "⚠️ Error processing an incoming update (see logs).")
            except Exception:
                logger.exception("Failed to notify admin about update-processing error")
    return "", 200

@app.route("/set_webhook", methods=["GET"])
def set_webhook_route():
    # sets webhook to WEBHOOK_URL (must be full URL). Returns webhook info.
    target = os.environ.get("WEBHOOK_URL") or WEBHOOK_URL or os.environ.get("RENDER_EXTERNAL_URL")
    if not target:
        return jsonify({"ok": False, "error": "WEBHOOK_URL or RENDER_EXTERNAL_URL not set"}), 400
    # ensure it ends with /<TOKEN>
    if not target.endswith(f"/{TOKEN}"):
        if target.endswith("/"):
            target = target + TOKEN
        else:
            target = target + f"/{TOKEN}"
    try:
        bot.remove_webhook()
        res = bot.set_webhook(url=target)
        return jsonify({"ok": True, "webhook_set": res, "url": target}), 200
    except Exception as e:
        logger.exception("set_webhook error")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/remove_webhook", methods=["GET"])
def remove_webhook_route():
    try:
        bot.remove_webhook()
        return jsonify({"ok": True, "removed": True}), 200
    except Exception as e:
        logger.exception("remove_webhook error")
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------------- Set webhook on startup if requested ----------------
def auto_set_webhook():
    target = os.environ.get("WEBHOOK_URL") or WEBHOOK_URL or os.environ.get("RENDER_EXTERNAL_URL")
    if not target:
        logger.warning("WEBHOOK_URL/RENDER_EXTERNAL_URL not provided; webhook not set automatically.")
        return
    if not target.endswith(f"/{TOKEN}"):
        if target.endswith("/"):
            target = target + TOKEN
        else:
            target = target + f"/{TOKEN}"
    try:
        bot.remove_webhook()
        ok = bot.set_webhook(url=target)
        logger.info("Webhook set: %s -> %s", ok, target)
    except Exception:
        logger.exception("auto_set_webhook failed")
        if ALERT_ON_ERROR:
            try:
                bot.send_message(ADMIN_ID, "⚠️ Failed to set webhook on startup (check logs).")
            except Exception:
                logger.exception("Failed to notify admin about webhook set failure")

if SET_WEBHOOK_ON_STARTUP:
    # safe to call at import; in Render you can also call /set_webhook route after deploy
    try:
        auto_set_webhook()
    except Exception:
        logger.exception("auto_set_webhook raised exception")

# ---------------- Run (when executed directly) ----------------
if __name__ == "__main__":
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", 5000))
    logger.info("Starting Flask webhook server on %s:%d", host, port)
    if RUN_BACKGROUND_JOBS and not (loader_thread and watch_thread):
        # threads already started earlier in module-level code
        pass
    # run Flask dev server; on Render production they will run via gunicorn (so __main__ won't run)
    app.run(host=host, port=port, debug=False)