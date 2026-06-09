import os
import io
import uuid
import requests
import boto3
import urllib.parse
import re
import zipfile
import time
import threading
import random
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session, Response, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_, cast, Float
from sqlalchemy.orm import joinedload


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

app = Flask(__name__)

# ------------------ CONFIG ------------------
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True, 'pool_recycle': 300}
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')
db = SQLAlchemy(app)

WEBSITE_BASE_URL = os.environ.get('WEBSITE_BASE_URL', 'https://malayalamsubtitles.onrender.com')

# ------------------ CLOUDFLARE R2 ------------------
r2_endpoint = os.environ.get('R2_ENDPOINT_URL')
r2_access_key = os.environ.get('R2_ACCESS_KEY_ID')
r2_secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
r2_bucket = os.environ.get('R2_BUCKET_NAME')
r2_public_url = os.environ.get('R2_PUBLIC_URL')
s3_client = None
if r2_endpoint and r2_access_key and r2_secret_key:
    s3_client = boto3.client('s3',
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key
    )

# ------------------ MODELS ------------------
class Movie(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    media_type = db.Column(db.String(10))
    title = db.Column(db.String(200))
    season = db.Column(db.Integer, nullable=True)
    episode = db.Column(db.Integer, nullable=True)
    year = db.Column(db.String(4))
    rating = db.Column(db.String(10))
    poster_url = db.Column(db.String(500))
    english_srt = db.Column(db.Text)
    views = db.Column(db.Integer, default=0)
    category = db.Column(db.String(200), default='General')
    plot = db.Column(db.Text, nullable=True)
    runtime = db.Column(db.String(50), nullable=True)
    imdb_id = db.Column(db.String(20))
    slug = db.Column(db.String(300), unique=True)
    created_at = db.Column(db.DateTime, server_default=func.now())

class TranslationCache(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    movie_id       = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language       = db.Column(db.String(10))
    translated_srt = db.Column(db.Text)
    downloads      = db.Column(db.Integer, default=0)
    quality_score  = db.Column(db.Integer, nullable=True)
    quality_flags  = db.Column(db.Text, nullable=True)
    movie          = db.relationship('Movie', backref=db.backref('translations', cascade='all, delete-orphan'))

class TranslationJob(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    movie_id   = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language   = db.Column(db.String(10))
    status     = db.Column(db.String(20), default='Pending')
    progress   = db.Column(db.Integer, default=0)
    priority   = db.Column(db.Integer, default=2)
    queued_at  = db.Column(db.DateTime, server_default=func.now())
    movie      = db.relationship('Movie', backref=db.backref('jobs', cascade='all, delete-orphan'))

class SiteStat(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    total_visitors = db.Column(db.Integer, default=0)

class SchedulerLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    run_at = db.Column(db.DateTime, server_default=func.now())
    result = db.Column(db.String(50))
    message = db.Column(db.Text)

class AdminLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, server_default=func.now())
    action = db.Column(db.String(200))
    details = db.Column(db.Text)
    ip_address = db.Column(db.String(45))

class SearchLog(db.Model):
    id            = db.Column(db.Integer, primary_key=True)
    search_query  = db.Column(db.String(300))
    results_count = db.Column(db.Integer, default=0)
    searched_at   = db.Column(db.DateTime, server_default=func.now())

class DownloadLog(db.Model):
    id            = db.Column(db.Integer, primary_key=True)
    movie_id      = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language      = db.Column(db.String(10))
    downloaded_at = db.Column(db.DateTime, server_default=func.now())
    movie         = db.relationship('Movie', backref=db.backref('download_logs', cascade='all, delete-orphan'))

class UploadPlan(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    title          = db.Column(db.String(200))
    imdb_id        = db.Column(db.String(20))
    media_type     = db.Column(db.String(10), default='movie')
    scheduled_date = db.Column(db.Date)
    status         = db.Column(db.String(20), default='Planned')
    notes          = db.Column(db.Text)
    poster_url     = db.Column(db.String(500))
    priority       = db.Column(db.Integer, default=2)
    created_at     = db.Column(db.DateTime, server_default=func.now())

class TelegramSubscription(db.Model):
    """Per-title subtitle-ready notification subscriptions."""
    id          = db.Column(db.Integer, primary_key=True)
    chat_id     = db.Column(db.String(50), index=True)
    movie_title = db.Column(db.String(200))
    imdb_id     = db.Column(db.String(20))
    language    = db.Column(db.String(10), default='ml')
    notified    = db.Column(db.Boolean, default=False)
    created_at  = db.Column(db.DateTime, server_default=func.now())

class SubtitleRating(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language = db.Column(db.String(10))
    rating = db.Column(db.Integer)
    comment = db.Column(db.Text, nullable=True)
    session_id = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, server_default=func.now())
    movie = db.relationship('Movie', backref=db.backref('sub_ratings', cascade='all, delete-orphan'))

class Favorite(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(100), index=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    created_at = db.Column(db.DateTime, server_default=func.now())
    movie = db.relationship('Movie', backref=db.backref('favorites', cascade='all, delete-orphan'))

class SubtitleRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200))
    details = db.Column(db.Text)
    votes = db.Column(db.Integer, default=1)
    status = db.Column(db.String(20), default='Pending')
    created_at = db.Column(db.DateTime, server_default=func.now())

class RequestVote(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.Integer, db.ForeignKey('subtitle_request.id', ondelete='CASCADE'))
    session_id = db.Column(db.String(100))

# ── NEW: Bot models ──────────────────────────────────────────────────────────

class TelegramUser(db.Model):
    """Registered bot users."""
    id            = db.Column(db.Integer, primary_key=True)
    chat_id       = db.Column(db.String(50), unique=True, index=True)
    username      = db.Column(db.String(100), nullable=True)
    first_name    = db.Column(db.String(100), default='')
    last_name     = db.Column(db.String(100), nullable=True)
    language_code = db.Column(db.String(10), default='ml')
    joined_at     = db.Column(db.DateTime, server_default=func.now())
    last_seen     = db.Column(db.DateTime, nullable=True)

class BroadcastLog(db.Model):
    """Log of every broadcast sent (subtitle notification or custom)."""
    id             = db.Column(db.Integer, primary_key=True)
    movie_id       = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='SET NULL'), nullable=True)
    custom_message = db.Column(db.Text, nullable=True)
    sent_count     = db.Column(db.Integer, default=0)
    failed_count   = db.Column(db.Integer, default=0)
    sent_by        = db.Column(db.String(50), nullable=True)
    broadcast_at   = db.Column(db.DateTime, server_default=func.now())

# ── DB init ──────────────────────────────────────────────────────────────────
with app.app_context():
    db.create_all()
    if not SiteStat.query.first():
        db.session.add(SiteStat(total_visitors=0))
        db.session.commit()

# ------------------ SESSION ID HELPER ------------------
def get_session_id():
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
    return session['user_id']

# ══════════════════════════════════════════════════════════════
# SEO DESCRIPTION GENERATOR
# ══════════════════════════════════════════════════════════════

def generate_seo_description(movie) -> str:
    genres = (movie.category or '').replace('SilentMode', '').strip(', ')
    genre_str = ', '.join(g.strip() for g in genres.split(',') if g.strip())[:60]
    year_str   = f"({movie.year}) " if movie.year else ""
    rating_str = (f"IMDb {movie.rating} " if movie.rating
                  and movie.rating not in ('N/A', '0') else "")
    langs = "Malayalam, Tamil and Hindi"

    if movie.media_type == 'series':
        base = (f"Download free {langs} subtitles for {movie.title} "
                f"Season {movie.season or 1}. {genre_str} series. "
                f"High-quality SRT with perfect sync. Free download.")
    else:
        base = (f"Download free {langs} subtitles for "
                f"{movie.title} {year_str}— {rating_str}"
                f"{genre_str} movie. Perfect sync SRT. Free download.")

    anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
    if anthropic_key and movie.plot:
        try:
            import anthropic
            client  = anthropic.Anthropic(api_key=anthropic_key)
            prompt  = (
                f"Write a 1-sentence SEO meta description (max 155 chars) for a subtitle "
                f"download page. The page is for: '{movie.title}' {year_str}({genre_str}). "
                f"Plot summary: {movie.plot[:200]}. "
                f"Focus on: Malayalam/Tamil/Hindi subtitle download, free, SRT format. "
                f"Be concise and keyword-rich. Return ONLY the description text."
            )
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=100,
                messages=[{"role": "user", "content": prompt}]
            )
            enhanced = resp.content[0].text.strip().strip('"').strip("'")
            if 10 < len(enhanced) < 160:
                return enhanced
        except Exception as e:
            print(f"Claude SEO gen failed: {e}")

    return base[:300]


# ══════════════════════════════════════════════════════════════
# TRANSLATION QUALITY CHECKER
# ══════════════════════════════════════════════════════════════

def check_translation_quality(english_srt: str, translated_srt: str, language: str) -> tuple:
    flags = []
    score = 100

    en_blocks = re.split(r'\n\s*\n', english_srt.strip())
    tr_blocks = re.split(r'\n\s*\n', translated_srt.strip())
    en_count  = len(en_blocks)
    tr_count  = len(tr_blocks)

    if en_count > 0:
        ratio = tr_count / en_count
        if ratio < 0.70:
            flags.append(f"Only {int(ratio*100)}% of subtitle blocks translated (expected ≥70%)")
            score -= 35
        elif ratio < 0.85:
            flags.append(f"Partial translation: {int(ratio*100)}% blocks covered")
            score -= 15

    en_bytes = len(english_srt.encode('utf-8'))
    tr_bytes = len(translated_srt.encode('utf-8'))
    if en_bytes > 0:
        sz_ratio = tr_bytes / en_bytes
        if sz_ratio < 0.35:
            flags.append(f"File size too small ({sz_ratio:.2f}x English) — possible truncation")
            score -= 25
        elif sz_ratio < 0.55:
            flags.append(f"File size low ({sz_ratio:.2f}x English)")
            score -= 10

    if language != 'en':
        sample_start = max(0, tr_count // 2 - 10)
        sample = '\n'.join(tr_blocks[sample_start:sample_start + 20])
        en_words = re.findall(r'\b[a-zA-Z]{5,}\b', sample)
        ignore   = {'season', 'episode', 'subtitle', 'english', 'really', 'about',
                    'would', 'could', 'should', 'their', 'there', 'where', 'which',
                    'other', 'before', 'after', 'these', 'those', 'being'}
        suspect  = [w for w in en_words if w.lower() not in ignore]
        if len(suspect) > 30:
            flags.append(f"High English word leakage ({len(suspect)} words) in sample — check sync")
            score -= 15
        elif len(suspect) > 15:
            flags.append(f"Some English words in translation ({len(suspect)}) — minor issue")
            score -= 5

    return max(0, min(100, score)), flags


# ══════════════════════════════════════════════════════════════
# HF WORKER HEALTH CHECK
# ══════════════════════════════════════════════════════════════

_hf_health_cache = {"status": "unknown", "data": {}, "checked_at": 0}

def check_hf_health(force: bool = False) -> dict:
    now = time.time()
    if not force and now - _hf_health_cache["checked_at"] < 30:
        return _hf_health_cache

    if not HF_WORKER_URL:
        _hf_health_cache.update({"status": "not_configured", "data": {}, "checked_at": now})
        return _hf_health_cache

    base = HF_WORKER_URL.rsplit('/api/', 1)[0]
    status_url = f"{base}/api/status"
    try:
        t0   = time.time()
        resp = requests.get(status_url, timeout=8)
        ms   = round((time.time() - t0) * 1000)
        if resp.status_code == 200:
            data = resp.json()
            data['response_ms'] = ms
            _hf_health_cache.update({"status": "online", "data": data, "checked_at": now})
        else:
            _hf_health_cache.update({
                "status": "error", "checked_at": now,
                "data": {"http_status": resp.status_code, "response_ms": ms}
            })
    except requests.exceptions.Timeout:
        _hf_health_cache.update({"status": "timeout", "data": {}, "checked_at": now})
    except Exception as e:
        _hf_health_cache.update({"status": "offline", "data": {"error": str(e)}, "checked_at": now})

    return _hf_health_cache


# ------------------ CACHED CATEGORIES ------------------
_categories_cache = {'data': [], 'last_update': 0}

def get_categories_list():
    now = time.time()
    if now - _categories_cache['last_update'] > 3600:
        all_cats = set()
        for m in Movie.query.with_entities(Movie.category).all():
            if m.category:
                for c in m.category.split(','):
                    cleaned = c.strip()
                    if cleaned and cleaned != "SilentMode":
                        all_cats.add(cleaned)
        _categories_cache['data'] = sorted(all_cats)
        _categories_cache['last_update'] = now
    return _categories_cache['data']

# ------------------ SEO HELPER ------------------
def build_seo_meta(movie):
    clean_cat = (movie.category or '').replace(', SilentMode', '').replace('SilentMode', '').strip(', ')
    genres = ', '.join([c.strip() for c in clean_cat.split(',') if c.strip()]) or 'General'
    plot_short = ''
    if movie.plot:
        plot_short = movie.plot[:155] + '…' if len(movie.plot) > 155 else movie.plot

    if movie.media_type == 'series':
        page_title = f"{movie.title} S{movie.season or 1:02d}E{movie.episode or 1:02d} Malayalam Subtitles | MalayalamSubs"
        og_title   = f"{movie.title} – Season {movie.season or 1}, Episode {movie.episode or 1} Subtitles"
        description = (f"Download Malayalam, Tamil & Hindi subtitles for {movie.title} "
                       f"Season {movie.season or 1} Episode {movie.episode or 1}. "
                       f"{plot_short}")
        encoded = urllib.parse.quote(movie.title)
        canonical = f"{WEBSITE_BASE_URL}/series/{encoded}/{movie.season or 1}"
    else:
        year_str   = f" ({movie.year})" if movie.year else ""
        page_title = f"{movie.title}{year_str} Malayalam Subtitles Download | MalayalamSubs"
        og_title   = f"{movie.title}{year_str} – Malayalam, Tamil & Hindi Subtitles"
        description = (f"Download {genres} subtitles for {movie.title}{year_str} in Malayalam, Tamil and Hindi. "
                       f"{plot_short}")
        canonical = f"{WEBSITE_BASE_URL}/movie/{movie.slug}"

    description = description[:160]

    return {
        'title':          page_title,
        'description':    description,
        'og_title':       og_title,
        'og_description': description,
        'og_image':       movie.poster_url or f"{WEBSITE_BASE_URL}/static/og-default.jpg",
        'og_url':         canonical,
        'og_type':        'video.tv_show' if movie.media_type == 'series' else 'video.movie',
        'canonical':      canonical,
        'keywords':       f"{movie.title}, malayalam subtitles, {genres} subtitles, download srt",
        'schema': {
            "@context": "https://schema.org",
            "@type": "Movie" if movie.media_type == 'movie' else "TVEpisode",
            "name": movie.title,
            "datePublished": movie.year,
            "image": movie.poster_url,
            "description": description,
            "genre": genres,
            "aggregateRating": {
                "@type": "AggregateRating",
                "ratingValue": movie.rating,
                "bestRating": "10",
                "ratingCount": "1"
            } if movie.rating and movie.rating not in ('N/A', '0') else None
        }
    }

# ------------------ TRIGGER TRANSLATION ------------------
HF_WORKER_URL  = os.environ.get('HF_WORKER_URL', '')
HF_SECRET      = os.environ.get('HF_SECRET', 'shared-secret')
TELEGRAM_SECRET= os.environ.get('TELEGRAM_SECRET', '')

MAX_CONCURRENT     = int(os.environ.get('MAX_CONCURRENT_JOBS', '5'))
SERIES_BATCH_SIZE  = int(os.environ.get('SERIES_BATCH_SIZE', '3'))
AVG_MINS_PER_JOB   = float(os.environ.get('AVG_TRANSLATION_MINS', '4'))

# ── Bot config ───────────────────────────────────────────────────────────────
BOT_TOKEN      = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_API   = f"https://api.telegram.org/bot{BOT_TOKEN}"
ADMIN_CHAT_IDS = [int(x) for x in os.environ.get('TELEGRAM_ADMIN_IDS', '').split(',') if x.strip()]

def _send_to_hf(job, movie):
    try:
        resp = requests.post(HF_WORKER_URL, json={
            "movie_id":         movie.id,
            "english_srt_url":  movie.english_srt,
            "language":         job.language
        }, timeout=10)
        if resp.status_code == 200:
            job.status   = 'Processing'
            job.progress = 0
            print(f"✅ Dispatched {job.language.upper()} job {job.id} (movie {movie.id}, priority {job.priority})")
            return True
        else:
            job.status = 'Failed'
            print(f"❌ HF rejected job {job.id}: HTTP {resp.status_code}")
            return False
    except Exception as e:
        job.status = 'Failed'
        print(f"❌ HF error job {job.id}: {e}")
        return False


def dispatch_pending_jobs():
    if not HF_WORKER_URL:
        return

    with app.app_context():
        processing_now = TranslationJob.query.filter_by(status='Processing').count()
        free_slots = max(0, MAX_CONCURRENT - processing_now)
        if free_slots == 0:
            return

        pending = (TranslationJob.query
                   .filter_by(status='Pending')
                   .join(Movie, TranslationJob.movie_id == Movie.id)
                   .order_by(TranslationJob.priority.asc(),
                             TranslationJob.queued_at.asc())
                   .limit(free_slots + SERIES_BATCH_SIZE)
                   .all())

        dispatched_series = 0
        dispatched_total  = 0

        for job in pending:
            if dispatched_total >= free_slots:
                break
            movie = Movie.query.get(job.movie_id)
            if not movie or not movie.english_srt:
                job.status = 'Failed'
                db.session.commit()
                continue

            is_series = movie.media_type == 'series'

            if is_series and dispatched_series >= SERIES_BATCH_SIZE:
                continue

            ok = _send_to_hf(job, movie)
            db.session.commit()

            if ok:
                dispatched_total += 1
                if is_series:
                    dispatched_series += 1


def trigger_hf_translation(movie_id: int, english_srt_url: str):
    if not HF_WORKER_URL:
        print("⚠️ HF_WORKER_URL not set")
        return

    with app.app_context():
        movie    = Movie.query.get(movie_id)
        priority = 1 if (movie and movie.media_type == 'movie') else 2

        for lang in ['ml', 'ta', 'hi']:
            job = TranslationJob.query.filter_by(movie_id=movie_id, language=lang).first()
            if not job:
                job = TranslationJob(
                    movie_id=movie_id, language=lang,
                    status='Pending', progress=0, priority=priority
                )
                db.session.add(job)
            else:
                if job.status in ('Failed', 'Completed'):
                    job.status   = 'Pending'
                    job.progress = 0
                    job.priority = priority
            db.session.commit()

        threading.Thread(target=dispatch_pending_jobs, daemon=True).start()


# ══════════════════════════════════════════════════════════════
# TELEGRAM BOT HELPERS
# ══════════════════════════════════════════════════════════════

def bot_send(chat_id, text, parse_mode="HTML", reply_markup=None):
    if not BOT_TOKEN:
        return None
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        print(f"bot_send error: {e}")
        return None


def bot_answer_callback(cq_id, text=""):
    try:
        requests.post(f"{TELEGRAM_API}/answerCallbackQuery",
                      json={"callback_query_id": cq_id, "text": text}, timeout=10)
    except Exception:
        pass


def bot_keyboard(buttons):
    """buttons: list of list of (label, callback_data_or_url)"""
    keyboard = []
    for row in buttons:
        kb_row = []
        for label, data in row:
            if data.startswith("http"):
                kb_row.append({"text": label, "url": data})
            else:
                kb_row.append({"text": label, "callback_data": data})
        keyboard.append(kb_row)
    return {"inline_keyboard": keyboard}


def bot_get_or_create_user(tg_user: dict):
    chat_id = str(tg_user["id"])
    user = TelegramUser.query.filter_by(chat_id=chat_id).first()
    if not user:
        user = TelegramUser(
            chat_id=chat_id,
            username=tg_user.get("username"),
            first_name=tg_user.get("first_name", ""),
            last_name=tg_user.get("last_name", ""),
            language_code=tg_user.get("language_code", "ml"),
            joined_at=datetime.utcnow(),
        )
        db.session.add(user)
    else:
        user.username   = tg_user.get("username")
        user.first_name = tg_user.get("first_name", user.first_name)
        user.last_name  = tg_user.get("last_name", user.last_name)
        user.last_seen  = datetime.utcnow()
    db.session.commit()
    return user


# ══════════════════════════════════════════════════════════════
# BOT COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════

def bot_handle_start(chat_id, tg_user):
    bot_get_or_create_user(tg_user)
    name = tg_user.get("first_name", "there")
    text = (
        f"👋 <b>Welcome to MalSubs, {name}!</b>\n\n"
        "I help you find Malayalam, Tamil, and Hindi subtitles.\n\n"
        "<b>Commands:</b>\n"
        "/search &lt;title&gt; — Search subtitles\n"
        "/request &lt;title&gt; — Request a subtitle\n"
        "/subscribe &lt;title&gt; — Get notified when ready\n"
        "/trending — Top downloads this week\n"
        "/new — Latest uploads\n"
        "/myreqs — Your requests\n"
        "/language — Set preferred language\n"
        "/status — Site stats\n\n"
        f"🌐 <a href='{WEBSITE_BASE_URL}'>Visit MalSubs</a>"
    )
    keyboard = bot_keyboard([
        [("🔍 Search", "action:search_prompt"), ("🆕 New Uploads", "action:new")],
        [("🔥 Trending", "action:trending"), ("📋 Requests", "action:requests")],
    ])
    bot_send(chat_id, text, reply_markup=keyboard)


def bot_handle_search(chat_id, query: str, tg_user):
    bot_get_or_create_user(tg_user)
    if not query.strip():
        bot_send(chat_id, "Please provide a title.\nExample: <code>/search Premalu</code>")
        return

    results = (Movie.query
               .filter(Movie.title.ilike(f"%{query}%"))
               .order_by(Movie.views.desc())
               .limit(6).all())

    # Deduplicate series
    seen, deduped = set(), []
    for m in results:
        k = m.title if m.media_type == 'series' else m.id
        if k not in seen:
            seen.add(k)
            deduped.append(m)

    if not deduped:
        keyboard = bot_keyboard([
            [(f"📬 Request '{query[:30]}'", f"req:{query[:50]}")]
        ])
        bot_send(chat_id,
                 f"😔 No subtitles found for <b>{query}</b>.\nWant to request it?",
                 reply_markup=keyboard)
        return

    text = f"🔍 <b>Results for \"{query}\"</b>\n\n"
    buttons = []
    for m in deduped[:5]:
        icon = "📺" if m.media_type == 'series' else "🎬"
        langs = ('EN ' if m.english_srt else '') + ' '.join(
            t.language.upper() for t in m.translations)
        text += f"{icon} <b>{m.title}</b> ({m.year or 'N/A'}) — {langs.strip() or 'Processing'}\n\n"
        url = (f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(m.title)}"
               if m.media_type == 'series'
               else f"{WEBSITE_BASE_URL}/movie/{m.slug or m.id}")
        buttons.append([(f"⬇️ {m.title[:28]}", url)])

    bot_send(chat_id, text, reply_markup=bot_keyboard(buttons))


def bot_handle_request(chat_id, title: str, tg_user):
    bot_get_or_create_user(tg_user)
    if not title.strip():
        bot_send(chat_id, "Please provide a title.\nExample: <code>/request Manjummel Boys</code>")
        return

    week_ago  = datetime.utcnow() - timedelta(days=7)
    duplicate = SubtitleRequest.query.filter(
        SubtitleRequest.title.ilike(title.strip()),
        SubtitleRequest.created_at >= week_ago
    ).first()

    if duplicate:
        bot_send(chat_id,
                 f"📋 <b>{duplicate.title}</b> is already in the request queue "
                 f"(status: <i>{duplicate.status}</i>).\n"
                 f"We'll notify you when it's ready!")
        return

    req = SubtitleRequest(
        title=title.strip(),
        details="Requested via Telegram bot",
        votes=1
    )
    db.session.add(req)
    db.session.commit()

    for admin_id in ADMIN_CHAT_IDS:
        bot_send(admin_id,
                 f"📬 <b>New subtitle request</b>\n\n"
                 f"Title: <b>{req.title}</b>\n"
                 f"From: @{tg_user.get('username') or tg_user.get('first_name')} "
                 f"(ID: {tg_user['id']})")

    bot_send(chat_id,
             f"✅ <b>Request submitted!</b>\n\n"
             f"We'll work on <b>{req.title}</b> and post when it's ready.\n\n"
             f"<a href='{WEBSITE_BASE_URL}/requests'>View all requests</a>")


def bot_handle_subscribe(chat_id, title: str, tg_user):
    """Subscribe to be notified when a specific title's subtitle is ready."""
    bot_get_or_create_user(tg_user)
    if not title.strip():
        bot_send(chat_id,
                 "Provide the title you want to follow.\n"
                 "Example: <code>/subscribe Interstellar</code>")
        return

    existing = TelegramSubscription.query.filter_by(
        chat_id=str(chat_id), movie_title=title.strip(), notified=False
    ).first()
    if existing:
        bot_send(chat_id,
                 f"✅ You're already subscribed for <b>{title}</b>.\n"
                 f"We'll notify you when the subtitle is ready!")
        return

    # Check if subtitle already exists
    movie = Movie.query.filter(Movie.title.ilike(f"%{title.strip()}%")).first()
    if movie and movie.translations:
        url = (f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(movie.title)}"
               if movie.media_type == 'series'
               else f"{WEBSITE_BASE_URL}/movie/{movie.slug or movie.id}")
        keyboard = bot_keyboard([[(f"⬇️ Download Now", url)]])
        bot_send(chat_id,
                 f"🎉 <b>{movie.title}</b> subtitles are already available!",
                 reply_markup=keyboard)
        return

    sub = TelegramSubscription(
        chat_id=str(chat_id),
        movie_title=title.strip(),
        language="ml",
        notified=False,
        created_at=datetime.utcnow()
    )
    db.session.add(sub)
    db.session.commit()
    bot_send(chat_id,
             f"🔔 <b>Subscribed!</b>\n\n"
             f"You'll be notified when <b>{title}</b> subtitles are uploaded.")


def bot_handle_trending(chat_id, tg_user):
    bot_get_or_create_user(tg_user)
    week_ago = datetime.utcnow() - timedelta(days=7)
    rows = (db.session.query(DownloadLog.movie_id,
                             func.count(DownloadLog.id).label('cnt'))
            .filter(DownloadLog.downloaded_at >= week_ago)
            .group_by(DownloadLog.movie_id)
            .order_by(func.count(DownloadLog.id).desc())
            .limit(10).all())

    if not rows:
        # Fallback to most viewed
        movies = Movie.query.order_by(Movie.views.desc()).limit(8).all()
    else:
        movie_ids = [r.movie_id for r in rows]
        movies_map = {m.id: m for m in Movie.query.filter(Movie.id.in_(movie_ids)).all()}
        movies = [movies_map[r.movie_id] for r in rows if r.movie_id in movies_map]

    seen, deduped = set(), []
    for m in movies:
        k = m.title if m.media_type == 'series' else m.id
        if k not in seen:
            seen.add(k)
            deduped.append(m)

    text = "🔥 <b>Trending This Week</b>\n\n"
    buttons = []
    for i, m in enumerate(deduped[:8], 1):
        icon = "📺" if m.media_type == 'series' else "🎬"
        text += f"{i}. {icon} <b>{m.title}</b> ({m.year or 'N/A'})\n\n"
        if i <= 4:
            url = (f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(m.title)}"
                   if m.media_type == 'series'
                   else f"{WEBSITE_BASE_URL}/movie/{m.slug or m.id}")
            buttons.append([(f"⬇️ {m.title[:28]}", url)])

    bot_send(chat_id, text, reply_markup=bot_keyboard(buttons) if buttons else None)


def bot_handle_new(chat_id, tg_user):
    bot_get_or_create_user(tg_user)
    results = (Movie.query
               .filter(Movie.english_srt.isnot(None))
               .order_by(Movie.id.desc())
               .limit(20).all())

    seen, deduped = set(), []
    for m in results:
        k = m.title if m.media_type == 'series' else m.id
        if k not in seen:
            seen.add(k)
            deduped.append(m)

    text = "🆕 <b>Latest Uploads</b>\n\n"
    buttons = []
    for i, m in enumerate(deduped[:8], 1):
        icon = "📺" if m.media_type == 'series' else "🎬"
        date_str = m.created_at.strftime("%b %d") if m.created_at else ""
        text += f"{i}. {icon} <b>{m.title}</b> ({m.year or 'N/A'}) — {date_str}\n\n"
        if i <= 4:
            url = (f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(m.title)}"
                   if m.media_type == 'series'
                   else f"{WEBSITE_BASE_URL}/movie/{m.slug or m.id}")
            buttons.append([(f"⬇️ {m.title[:28]}", url)])

    bot_send(chat_id, text, reply_markup=bot_keyboard(buttons) if buttons else None)


def bot_handle_myreqs(chat_id, tg_user):
    bot_get_or_create_user(tg_user)
    subs = (TelegramSubscription.query
            .filter_by(chat_id=str(chat_id))
            .order_by(TelegramSubscription.created_at.desc())
            .limit(10).all())
    reqs = (SubtitleRequest.query
            .filter(SubtitleRequest.details.ilike('%Telegram%'))
            .order_by(SubtitleRequest.created_at.desc())
            .limit(10).all())

    if not subs and not reqs:
        bot_send(chat_id,
                 "You haven't made any requests yet.\n\n"
                 "Use /request &lt;title&gt; to request a subtitle.")
        return

    text = "📋 <b>Your Activity</b>\n\n"
    if subs:
        text += "<b>Subscriptions:</b>\n"
        for s in subs:
            icon = "✅" if s.notified else "🔔"
            text += f"{icon} {s.movie_title} ({s.language.upper()})\n"
        text += "\n"

    if reqs:
        status_emoji = {"Pending": "⏳", "InProgress": "🔧", "Done": "✅"}
        text += "<b>Requests:</b>\n"
        for r in reqs[:5]:
            emoji = status_emoji.get(r.status, "❓")
            text += f"{emoji} <b>{r.title}</b> — <i>{r.status}</i>\n"

    bot_send(chat_id, text)


def bot_handle_language(chat_id, tg_user):
    user = bot_get_or_create_user(tg_user)
    current = user.language_code or "ml"
    labels = {"ml": "Malayalam 🇮🇳", "ta": "Tamil 🔴", "hi": "Hindi 🟠", "all": "All 🌍"}
    text = (f"🌐 <b>Preferred Language</b>\n\n"
            f"Current: <b>{labels.get(current, current.upper())}</b>\n\n"
            f"Select your preferred subtitle language:")
    keyboard = bot_keyboard([
        [("🇮🇳 Malayalam", "lang:ml"), ("🔴 Tamil", "lang:ta")],
        [("🟠 Hindi", "lang:hi"), ("🌍 All", "lang:all")],
    ])
    bot_send(chat_id, text, reply_markup=keyboard)


def bot_handle_status(chat_id):
    total_movies  = Movie.query.filter_by(media_type='movie').count()
    total_series  = db.session.query(Movie.title).filter_by(media_type='series').distinct().count()
    total_subs    = TranslationCache.query.count()
    pending_jobs  = TranslationJob.query.filter_by(status='Pending').count()
    total_users   = TelegramUser.query.count()
    active_subs   = TelegramSubscription.query.filter_by(notified=False).count()

    hf = check_hf_health()
    hf_status = {"online": "✅ Online", "offline": "❌ Offline",
                 "timeout": "⏱ Timeout", "error": "⚠️ Error",
                 "not_configured": "❓ Not configured"}.get(hf["status"], "❓ Unknown")

    text = (
        f"📊 <b>MalSubs Status</b>\n\n"
        f"🎬 Movies: <b>{total_movies}</b>\n"
        f"📺 Series: <b>{total_series}</b>\n"
        f"🌐 Translations: <b>{total_subs}</b>\n"
        f"⏳ Pending jobs: <b>{pending_jobs}</b>\n\n"
        f"👥 Bot users: <b>{total_users}</b>\n"
        f"🔔 Waiting subs: <b>{active_subs}</b>\n\n"
        f"🤖 AI Worker: <b>{hf_status}</b>\n"
        f"🌐 <a href='{WEBSITE_BASE_URL}'>Visit MalSubs</a>"
    )
    bot_send(chat_id, text)


def bot_handle_callback(callback_query):
    chat_id  = str(callback_query["from"]["id"])
    tg_user  = callback_query["from"]
    data     = callback_query.get("data", "")
    cq_id    = callback_query["id"]

    bot_answer_callback(cq_id)

    if data == "action:search_prompt":
        bot_send(chat_id, "Send me a title:\nExample: <code>/search Premalu</code>")
    elif data == "action:new":
        bot_handle_new(chat_id, tg_user)
    elif data == "action:trending":
        bot_handle_trending(chat_id, tg_user)
    elif data == "action:requests":
        bot_send(chat_id, f"🙋 <a href='{WEBSITE_BASE_URL}/requests'>View & vote on subtitle requests</a>")
    elif data.startswith("lang:"):
        lang  = data.split(":")[1]
        user  = bot_get_or_create_user(tg_user)
        user.language_code = lang
        db.session.commit()
        labels = {"ml": "Malayalam 🇮🇳", "ta": "Tamil 🔴", "hi": "Hindi 🟠", "all": "All 🌍"}
        bot_send(chat_id, f"✅ Preferred language set to <b>{labels.get(lang, lang)}</b>.")
    elif data.startswith("req:"):
        bot_handle_request(chat_id, data[4:], tg_user)


# ══════════════════════════════════════════════════════════════
# BROADCAST
# ══════════════════════════════════════════════════════════════

def broadcast_new_subtitle(movie):
    """
    Notify:
    1. Users subscribed to this specific title (TelegramSubscription).
    2. All TelegramUsers who haven't opted out (global broadcast).
    Called automatically after a new Movie upload commits.
    """
    if not BOT_TOKEN:
        return
    if "SilentMode" in (movie.category or ''):
        return

    url = (f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(movie.title)}"
           if movie.media_type == 'series'
           else f"{WEBSITE_BASE_URL}/movie/{movie.slug or movie.id}")
    icon = "📺" if movie.media_type == 'series' else "🎬"
    text = (
        f"🆕 <b>New Subtitle Added!</b>\n\n"
        f"{icon} <b>{movie.title}</b> ({movie.year or 'N/A'})\n"
        f"Languages: Malayalam 🇮🇳 Tamil 🔴 Hindi 🟠\n\n"
        f"<a href='{url}'>⬇️ Download Now</a>"
    )
    keyboard = bot_keyboard([[(f"⬇️ Download", url)]])

    # 1. Per-title subscribers
    title_subs = TelegramSubscription.query.filter(
        TelegramSubscription.notified == False,
        or_(
            TelegramSubscription.imdb_id == movie.imdb_id,
            TelegramSubscription.movie_title.ilike(f'%{movie.title}%')
        )
    ).all()

    title_sent = 0
    for sub in title_subs:
        result = bot_send(sub.chat_id, text, reply_markup=keyboard)
        if result and result.get("ok"):
            sub.notified = True
            title_sent += 1
        elif result and result.get("error_code") in (403, 400):
            sub.notified = True  # dead chat, mark done
    db.session.commit()

    # 2. Global broadcast to all TelegramUsers (all-upload feed)
    all_users = TelegramUser.query.all()
    notified_chats = {sub.chat_id for sub in title_subs}
    global_sent = global_failed = 0
    for user in all_users:
        if user.chat_id in notified_chats:
            continue  # already sent above
        result = bot_send(user.chat_id, text, reply_markup=keyboard)
        if result and result.get("ok"):
            global_sent += 1
        else:
            global_failed += 1

    total_sent   = title_sent + global_sent
    total_failed = global_failed

    log = BroadcastLog(
        movie_id=movie.id,
        sent_count=total_sent,
        failed_count=total_failed,
        broadcast_at=datetime.utcnow()
    )
    db.session.add(log)
    db.session.commit()
    print(f"📣 Broadcast: {total_sent} sent, {total_failed} failed (movie {movie.id})")


def send_admin_broadcast(message: str, sent_by=None):
    """Custom broadcast to all TelegramUsers. Returns (sent, failed)."""
    if not BOT_TOKEN:
        return 0, 0
    users = TelegramUser.query.all()
    sent = failed = 0
    for user in users:
        result = bot_send(user.chat_id, message)
        if result and result.get("ok"):
            sent += 1
        else:
            failed += 1
    log = BroadcastLog(
        custom_message=message[:500],
        sent_count=sent,
        failed_count=failed,
        sent_by=str(sent_by) if sent_by else None,
        broadcast_at=datetime.utcnow()
    )
    db.session.add(log)
    db.session.commit()
    return sent, failed


# ══════════════════════════════════════════════════════════════
# BOT WEBHOOK + ADMIN API ROUTES
# ══════════════════════════════════════════════════════════════

@app.route('/bot/webhook', methods=['POST'])
def bot_webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": False}), 400

    try:
        if "callback_query" in data:
            bot_handle_callback(data["callback_query"])
            return jsonify({"ok": True})

        message = data.get("message") or data.get("edited_message")
        if not message:
            return jsonify({"ok": True})

        chat_id = str(message["chat"]["id"])
        tg_user = message.get("from", {})
        text    = (message.get("text") or "").strip()

        if not text:
            return jsonify({"ok": True})

        if text.startswith("/start"):
            bot_handle_start(chat_id, tg_user)
        elif text.startswith("/search"):
            bot_handle_search(chat_id, text[7:].strip(), tg_user)
        elif text.startswith("/request"):
            bot_handle_request(chat_id, text[8:].strip(), tg_user)
        elif text.startswith("/subscribe"):
            bot_handle_subscribe(chat_id, text[10:].strip(), tg_user)
        elif text.startswith("/trending"):
            bot_handle_trending(chat_id, tg_user)
        elif text.startswith("/new"):
            bot_handle_new(chat_id, tg_user)
        elif text.startswith("/myreqs"):
            bot_handle_myreqs(chat_id, tg_user)
        elif text.startswith("/language"):
            bot_handle_language(chat_id, tg_user)
        elif text.startswith("/status"):
            bot_handle_status(chat_id)
        elif text.startswith("/broadcast") and int(chat_id) in ADMIN_CHAT_IDS:
            msg = text[10:].strip()
            if msg:
                sent, failed = send_admin_broadcast(msg, sent_by=chat_id)
                bot_send(chat_id, f"📣 Sent to <b>{sent}</b> users. Failed: {failed}.")
            else:
                bot_send(chat_id, "Usage: /broadcast &lt;message&gt;")
        elif text.startswith("/"):
            bot_send(chat_id, "❓ Unknown command. Try /start")
        else:
            # Plain text → treat as search
            bot_handle_search(chat_id, text, tg_user)

    except Exception as e:
        print(f"bot_webhook error: {e}")

    return jsonify({"ok": True})


@app.route('/bot/set-webhook', methods=['POST'])
@login_required
def bot_set_webhook():
    webhook_url = (request.json or {}).get("url") or f"{WEBSITE_BASE_URL}/bot/webhook"
    r = requests.post(f"{TELEGRAM_API}/setWebhook",
                      json={"url": webhook_url}, timeout=10)
    return jsonify(r.json())


# ── Bot admin API (used by bot_dashboard.html) ────────────────────────────────

@app.route('/bot/api/stats')
@login_required
def bot_api_stats():
    total_users   = TelegramUser.query.count()
    active_subs   = TelegramSubscription.query.filter_by(notified=False).count()
    pending_reqs  = SubtitleRequest.query.filter_by(status='Pending').count()
    new_today     = TelegramUser.query.filter(
        func.date(TelegramUser.joined_at) == datetime.utcnow().date()
    ).count()
    broadcasts    = BroadcastLog.query.order_by(BroadcastLog.broadcast_at.desc()).limit(5).all()

    return jsonify({
        "total_users":        total_users,
        "active_subscribers": active_subs,
        "pending_requests":   pending_reqs,
        "new_users_today":    new_today,
        "recent_broadcasts": [
            {
                "id":             b.id,
                "movie_id":       b.movie_id,
                "custom_message": b.custom_message,
                "sent":           b.sent_count,
                "failed":         b.failed_count,
                "at":             b.broadcast_at.isoformat() if b.broadcast_at else None,
            }
            for b in broadcasts
        ],
    })


@app.route('/bot/api/users')
@login_required
def bot_api_users():
    page    = request.args.get("page", 1, type=int)
    per_page = 50
    users   = TelegramUser.query.order_by(TelegramUser.joined_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False)
    return jsonify({
        "users": [
            {
                "chat_id":       u.chat_id,
                "username":      u.username,
                "first_name":    u.first_name,
                "last_name":     u.last_name,
                "language_code": u.language_code,
                "joined_at":     u.joined_at.isoformat() if u.joined_at else None,
                "last_seen":     u.last_seen.isoformat() if u.last_seen else None,
            }
            for u in users.items
        ],
        "total": users.total,
        "pages": users.pages,
        "page":  page,
    })


@app.route('/bot/api/requests')
@login_required
def bot_api_requests():
    status_filter = request.args.get("status", "Pending")
    reqs = (SubtitleRequest.query
            .filter_by(status=status_filter)
            .order_by(SubtitleRequest.created_at.desc())
            .limit(100).all())
    return jsonify({
        "requests": [
            {
                "id":           r.id,
                "title":        r.title,
                "status":       r.status,
                "votes":        r.votes,
                "created_at":   r.created_at.isoformat() if r.created_at else None,
            }
            for r in reqs
        ]
    })


@app.route('/bot/api/requests/<int:req_id>', methods=['PATCH'])
@login_required
def bot_api_update_request(req_id):
    data = request.get_json() or {}
    req  = SubtitleRequest.query.get_or_404(req_id)
    if "status" in data:
        req.status = data["status"]
        db.session.commit()

        # Notify per-title subscribers if done
        if data["status"] == "Done":
            subs = TelegramSubscription.query.filter(
                TelegramSubscription.notified == False,
                TelegramSubscription.movie_title.ilike(f'%{req.title}%')
            ).all()
            for sub in subs:
                bot_send(sub.chat_id,
                         f"🎉 <b>Subtitle Ready!</b>\n\n"
                         f"<b>{req.title}</b> is now available.\n\n"
                         f"<a href='{WEBSITE_BASE_URL}'>⬇️ Download at MalSubs</a>")
                sub.notified = True
            db.session.commit()

    return jsonify({"ok": True, "status": req.status})


@app.route('/bot/api/broadcast', methods=['POST'])
@login_required
def bot_api_broadcast():
    data    = request.get_json() or {}
    message = data.get("message", "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message required"}), 400
    sent, failed = send_admin_broadcast(message)
    return jsonify({"ok": True, "sent": sent, "failed": failed})


@app.route('/admin/bot-dashboard')
@login_required
def bot_dashboard():
    """Serve the bot management dashboard HTML."""
    return send_from_directory('.', 'bot_dashboard.html')


# ── Translation Stats API ─────────────────────────────────────────────────────
@app.route('/api/translation_stats')
@login_required
def translation_stats():
    pending    = TranslationJob.query.filter_by(status='Pending').count()
    processing = TranslationJob.query.filter_by(status='Processing').count()
    failed     = TranslationJob.query.filter_by(status='Failed').count()
    completed  = TranslationJob.query.filter(
                     TranslationJob.status.in_(['Completed', 'Success'])).count()

    active     = pending + processing
    grand_total= active + completed + failed
    pct_done   = round(completed / grand_total * 100, 1) if grand_total else 100.0

    movie_pending = (db.session.query(func.count(TranslationJob.id))
                    .join(Movie, TranslationJob.movie_id == Movie.id)
                    .filter(TranslationJob.status == 'Pending',
                            Movie.media_type == 'movie')
                    .scalar() or 0)
    series_pending = (db.session.query(func.count(TranslationJob.id))
                     .join(Movie, TranslationJob.movie_id == Movie.id)
                     .filter(TranslationJob.status == 'Pending',
                             Movie.media_type == 'series')
                     .scalar() or 0)

    est_minutes = round(active * AVG_MINS_PER_JOB / max(1, MAX_CONCURRENT))
    est_text    = (f"{est_minutes // 60}h {est_minutes % 60}m"
                  if est_minutes >= 60 else f"{est_minutes}m")

    lang_pending = {}
    for lang in ['ml', 'ta', 'hi']:
        lang_pending[lang] = TranslationJob.query.filter_by(
            status='Pending', language=lang).count()

    return jsonify({
        'pending':        pending,
        'processing':     processing,
        'failed':         failed,
        'completed':      completed,
        'total':          grand_total,
        'active':         active,
        'pct_done':       pct_done,
        'est_minutes':    est_minutes,
        'est_text':       est_text,
        'movie_pending':  movie_pending,
        'series_pending': series_pending,
        'lang_pending':   lang_pending,
        'max_concurrent': MAX_CONCURRENT,
        'batch_size':     SERIES_BATCH_SIZE,
    })


# ------------------ CALLBACK ENDPOINT ------------------
@app.route('/api/translation_callback', methods=['POST'])
def translation_callback():
    data   = request.json
    secret = request.headers.get('Authorization', '').replace('Bearer ', '')
    if secret != HF_SECRET:
        return jsonify({"error": "unauthorized"}), 401

    movie_id = data.get('movie_id')
    language = data.get('language')
    status   = data.get('status')
    progress = data.get('progress', 0)

    job = TranslationJob.query.filter_by(movie_id=movie_id, language=language).first()
    if job:
        job.status   = status
        job.progress = progress
        db.session.commit()

    if status in ('Completed', 'Success') and movie_id:
        all_jobs = TranslationJob.query.filter_by(movie_id=movie_id).all()
        all_done = all(j.status in ('Completed', 'Success')
                       for j in all_jobs if j.language in ['ml', 'ta', 'hi'])
        if all_done:
            try:
                requests.get(
                    url_for('trigger_telegram', movie_id=movie_id, _external=True),
                    params={'secret': TELEGRAM_SECRET},
                    timeout=5
                )
            except Exception as e:
                print(f"⚠️ Auto Telegram post failed: {e}")

    if status in ('Completed', 'Success') and movie_id and language:
        def run_quality_check(mid, lang):
            with app.app_context():
                try:
                    cache = TranslationCache.query.filter_by(movie_id=mid, language=lang).first()
                    movie = Movie.query.get(mid)
                    if not cache or not movie or not movie.english_srt:
                        return
                    if movie.english_srt.startswith('http'):
                        en_resp = requests.get(movie.english_srt, timeout=20)
                        english_srt = en_resp.text
                    else:
                        english_srt = movie.english_srt
                    if cache.translated_srt.startswith('http'):
                        tr_resp = requests.get(cache.translated_srt, timeout=20)
                        translated_srt = tr_resp.text
                    else:
                        translated_srt = cache.translated_srt
                    score, flags = check_translation_quality(english_srt, translated_srt, lang)
                    import json as _json
                    cache.quality_score = score
                    cache.quality_flags = _json.dumps(flags)
                    db.session.commit()
                    print(f"✅ Quality check: movie {mid} [{lang}] = {score}/100")
                    notify_telegram_subscribers(mid, lang)
                except Exception as e:
                    print(f"Quality check failed: {e}")
        threading.Thread(target=run_quality_check, args=(movie_id, language), daemon=True).start()

    threading.Thread(target=dispatch_pending_jobs, daemon=True).start()
    return jsonify({"ok": True})

# ------------------ AUTH ------------------
def log_admin_action(action, details):
    log = AdminLog(action=action, details=details, ip_address=request.remote_addr)
    db.session.add(log)
    db.session.commit()

# ===========================================================================
#  USER ROUTES
# ===========================================================================

GENRE_META = {
    'Action':          {'icon': '💥', 'color': '#c62828', 'desc': 'High-octane Action movies and series'},
    'Adventure':       {'icon': '🗺️', 'color': '#e65100', 'desc': 'Epic Adventure movies and series'},
    'Animation':       {'icon': '🎨', 'color': '#1565c0', 'desc': 'Animated movies and series for all ages'},
    'Comedy':          {'icon': '😂', 'color': '#f9a825', 'desc': 'Hilarious Comedy movies and series'},
    'Crime':           {'icon': '🔍', 'color': '#4a148c', 'desc': 'Gripping Crime and detective stories'},
    'Documentary':     {'icon': '🎬', 'color': '#2e7d32', 'desc': 'Fascinating Documentary films and series'},
    'Drama':           {'icon': '🎭', 'color': '#6a1b9a', 'desc': 'Powerful Drama movies and series'},
    'Family':          {'icon': '👨‍👩‍👧', 'color': '#0277bd', 'desc': 'Family-friendly movies and series'},
    'Fantasy':         {'icon': '🧙', 'color': '#283593', 'desc': 'Magical Fantasy movies and series'},
    'History':         {'icon': '📜', 'color': '#5d4037', 'desc': 'Historical movies and epic sagas'},
    'Horror':          {'icon': '👻', 'color': '#b71c1c', 'desc': 'Scary Horror movies and series'},
    'Music':           {'icon': '🎵', 'color': '#00695c', 'desc': 'Music movies and concert films'},
    'Mystery':         {'icon': '🔮', 'color': '#37474f', 'desc': 'Mysterious and suspenseful titles'},
    'Romance':         {'icon': '❤️',  'color': '#c2185b', 'desc': 'Romantic movies and love stories'},
    'Science Fiction': {'icon': '🚀', 'color': '#0d47a1', 'desc': 'Mind-bending Sci-Fi movies and series'},
    'Thriller':        {'icon': '😱', 'color': '#212121', 'desc': 'Edge-of-your-seat Thriller titles'},
    'War':             {'icon': '⚔️',  'color': '#827717', 'desc': 'Powerful War movies and series'},
    'Western':         {'icon': '🤠', 'color': '#4e342e', 'desc': 'Classic Western movies and series'},
}

def deduplicate_results(query_results):
    seen_series = {}
    movies_list = []
    for m in query_results:
        if m.media_type == 'series':
            if m.title not in seen_series or m.id > seen_series[m.title].id:
                seen_series[m.title] = m
        else:
            movies_list.append(m)
    combined = movies_list + list(seen_series.values())
    combined.sort(key=lambda m: m.id, reverse=True)
    return combined

@app.route('/subtitles/<genre>')
def genre_page(genre):
    display_genre = next(
        (g for g in GENRE_META if g.lower() == genre.lower()),
        genre.title()
    )
    meta = GENRE_META.get(display_genre, {
        'icon': '🎬', 'color': '#e50914', 'desc': f'{display_genre} movies and series'
    })

    page     = request.args.get('page', 1, type=int)
    sort     = request.args.get('sort', 'newest')
    mtype    = request.args.get('type', '')
    per_page = 24

    sort_map = {
        'newest':  Movie.id.desc(),
        'rating':  cast(func.nullif(Movie.rating, 'N/A'), Float).desc().nulls_last(),
        'popular': Movie.views.desc(),
        'title':   Movie.title.asc(),
    }

    base_q = Movie.query.filter(Movie.category.ilike(f'%{display_genre}%'))
    if mtype in ('movie', 'series'):
        base_q = base_q.filter(Movie.media_type == mtype)

    all_results  = base_q.order_by(sort_map.get(sort, Movie.id.desc())).all()
    deduped      = deduplicate_results(all_results)
    total        = len(deduped)
    start        = (page - 1) * per_page
    items        = deduped[start:start + per_page]
    total_pages  = (total + per_page - 1) // per_page

    related = [g for g in GENRE_META if g != display_genre][:8]

    seo_title = f"{display_genre} Subtitles – Download Malayalam, Tamil & Hindi | MalSubs"
    seo_desc  = (f"Download free Malayalam, Tamil and Hindi subtitles for "
                 f"{total} {display_genre} movies and series. {meta['desc']}.")

    return render_template('genre.html',
                           genre=display_genre, meta=meta, items=items,
                           total=total, page=page, total_pages=total_pages,
                           per_page=per_page, sort=sort, mtype=mtype,
                           related_genres=related, categories=get_categories_list(),
                           seo_title=seo_title, seo_desc=seo_desc)

@app.route('/')
def index():
    stat = SiteStat.query.first()
    stat.total_visitors += 1
    db.session.commit()

    search_query    = request.args.get('q', '')
    category_query  = request.args.get('cat', '')
    page            = request.args.get('page', 1, type=int)
    categories_list = get_categories_list()

    if search_query:
        pagination = Movie.query.filter(
            (Movie.title.ilike(f'%{search_query}%')) |
            (Movie.category.ilike(f'%{search_query}%'))
        ).order_by(Movie.id.desc()).paginate(page=page, per_page=12, error_out=False)
        try:
            db.session.add(SearchLog(search_query=search_query[:300],
                                     results_count=pagination.total))
            db.session.commit()
        except Exception:
            db.session.rollback()
        return render_template('index.html', pagination=pagination,
                               search_query=search_query,
                               categories=categories_list, mode="search")

    if category_query:
        return redirect(url_for('genre_page', genre=category_query))

    trending_movies  = Movie.query.filter_by(media_type='movie').order_by(Movie.views.desc()).limit(12).all()
    trending_series  = Movie.query.from_statement(
        db.text("SELECT DISTINCT ON (title) * FROM movie WHERE media_type='series' ORDER BY title, views DESC, id DESC LIMIT 12")
    ).all()
    popular_movies   = Movie.query.filter_by(media_type='movie').order_by(Movie.views.desc()).limit(12).all()
    top_rated_movies = Movie.query.filter_by(media_type='movie')\
                           .order_by(cast(func.nullif(Movie.rating, 'N/A'), Float).desc().nulls_last())\
                           .limit(12).all()
    top_rated_series = Movie.query.from_statement(
        db.text("SELECT DISTINCT ON (title) * FROM movie WHERE media_type='series' ORDER BY title, NULLIF(rating,'N/A')::float DESC NULLS LAST, id DESC LIMIT 12")
    ).all()
    recent_uploads_subq = db.session.query(
        Movie.title, func.max(Movie.id).label('max_id')
    ).group_by(Movie.title).subquery()
    recent_uploads = Movie.query.join(
        recent_uploads_subq, Movie.id == recent_uploads_subq.c.max_id
    ).order_by(Movie.id.desc()).limit(12).all()

    top_requests = SubtitleRequest.query\
        .filter_by(status='Pending')\
        .order_by(SubtitleRequest.votes.desc())\
        .limit(5).all()

    return render_template('index.html', mode="home", categories=categories_list,
                           trending_movies=trending_movies, trending_series=trending_series,
                           popular_movies=popular_movies, top_rated_movies=top_rated_movies,
                           top_rated_series=top_rated_series, recent_uploads=recent_uploads,
                           top_requests=top_requests, search_query='', pagination=None)

@app.route('/robots.txt')
def robots_txt():
    rules = "User-agent: *\nDisallow: /admin\nDisallow: /login\nDisallow: /delete/\nAllow: /\n"
    return rules, 200, {'Content-Type': 'text/plain'}

@app.route('/keep-alive')
def keep_alive():
    return "Server is awake!", 200

@app.route('/movie/<int:movie_id>')
def old_movie_redirect(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    if not movie.slug:
        movie.slug = _unique_slug(movie.title, movie.year)
        db.session.commit()
    return redirect(url_for('movie_hub', slug=movie.slug), code=301)

@app.route('/movie/<slug>')
def movie_hub(slug):
    movie = Movie.query.filter_by(slug=slug).first_or_404()
    movie.views += 1
    db.session.commit()

    sid = get_session_id()
    rv  = session.get('recently_viewed', [])
    if movie.id in rv:
        rv.remove(movie.id)
    rv.insert(0, movie.id)
    session['recently_viewed'] = rv[:20]

    ready_languages = [c.language for c in movie.translations]
    primary_genre   = movie.category.split(',')[0].strip() if movie.category else 'General'
    related_movies  = Movie.query.filter(
        Movie.category.ilike(f'%{primary_genre}%'), Movie.id != movie.id
    ).limit(4).all()

    lang_ratings = {}
    for lang in ['en'] + ready_languages:
        rows = SubtitleRating.query.filter_by(movie_id=movie.id, language=lang).all()
        if rows:
            avg = round(sum(r.rating for r in rows) / len(rows), 1)
            lang_ratings[lang] = {'avg': avg, 'count': len(rows), 'comments': rows[-5:]}

    is_favorite = Favorite.query.filter_by(session_id=sid, movie_id=movie.id).first() is not None
    seo = build_seo_meta(movie)

    return render_template('movie.html', movie=movie,
                           ready_languages=ready_languages,
                           related_movies=related_movies,
                           lang_ratings=lang_ratings,
                           is_favorite=is_favorite, seo=seo)

@app.route('/series/<string:title>')
def series_overview(title):
    episodes = Movie.query\
        .filter_by(media_type='series', title=title)\
        .options(joinedload(Movie.translations))\
        .order_by(Movie.season.asc(), Movie.episode.asc())\
        .all()
    if not episodes:
        return "Series not found", 404
    show_data = episodes[0]
    seasons   = {}
    for ep in episodes:
        s = ep.season or 1
        seasons.setdefault(s, []).append(ep)
    sorted_seasons = sorted(seasons.items())
    try:
        seo = build_seo_meta(show_data)
    except Exception:
        seo = None
    return render_template('series_overview.html', title=title, seasons=seasons,
                           sorted_seasons=sorted_seasons, show_data=show_data,
                           seo=seo, categories=get_categories_list())

@app.route('/series/<string:title>/<int:season>')
def series_page(title, season):
    eps = Movie.query\
        .filter_by(media_type='series', title=title, season=season)\
        .options(joinedload(Movie.translations))\
        .order_by(Movie.episode.asc())\
        .all()
    if not eps:
        return "Season not found", 404
    show_data   = eps[0]
    all_seasons = db.session.query(Movie.season)\
        .filter_by(media_type='series', title=title)\
        .distinct().order_by(Movie.season.asc()).all()
    all_seasons = [r[0] for r in all_seasons if r[0]]
    try:
        seo = build_seo_meta(show_data)
    except Exception:
        seo = None
    return render_template('series.html', title=title, season=season,
                           episodes=eps, all_seasons=all_seasons,
                           show_data=show_data, seo=seo,
                           categories=get_categories_list())

@app.route('/download/<int:movie_id>/<language>')
def download(movie_id, language):
    movie = Movie.query.get_or_404(movie_id)
    if language == 'en':
        srt_data = movie.english_srt
    else:
        cache    = TranslationCache.query.filter_by(movie_id=movie_id, language=language).first_or_404()
        srt_data = cache.translated_srt
        cache.downloads = (cache.downloads or 0) + 1
        db.session.commit()

    try:
        db.session.add(DownloadLog(movie_id=movie_id, language=language))
        db.session.commit()
    except Exception:
        db.session.rollback()

    if srt_data.startswith('http'):
        try:
            response = requests.get(srt_data, timeout=15)
            response.raise_for_status()
            file_content = response.content
        except Exception as e:
            return f"Error fetching subtitle file: {e}", 500
    else:
        file_content = srt_data.encode('utf-8')

    mem_file = io.BytesIO(file_content)
    mem_file.seek(0)

    lang_map    = {'en': 'English', 'ml': 'Malayalam', 'ta': 'Tamil', 'hi': 'Hindi'}
    full_lang   = lang_map.get(language, language.upper())
    safe_title  = re.sub(r'[^\w\s-]', '', movie.title)
    clean_title = re.sub(r'[-\s]+', '.', safe_title).strip('.')

    if movie.media_type == 'series':
        s = movie.season or 1
        e = movie.episode or 1
        final_name = f"{clean_title}.S{s:02d}E{e:02d}.{full_lang}.srt"
    else:
        year_str   = f".{movie.year}" if movie.year else ""
        final_name = f"{clean_title}{year_str}.{full_lang}.srt"

    return send_file(mem_file, as_attachment=True, download_name=final_name,
                     mimetype='application/x-subrip')

@app.route('/search')
def advanced_search():
    q          = request.args.get('q', '').strip()
    media_type = request.args.get('type', '')
    year_from  = request.args.get('year_from', type=int)
    year_to    = request.args.get('year_to', type=int)
    rating_min = request.args.get('rating_min', type=float)
    rating_max = request.args.get('rating_max', type=float)
    category   = request.args.get('category', '')
    lang       = request.args.get('lang', '')
    imdb       = request.args.get('imdb', '').strip()
    sort       = request.args.get('sort', 'newest')
    page       = request.args.get('page', 1, type=int)

    base_q = Movie.query
    if q:
        search_filters = [Movie.title.ilike(f'%{q}%'), Movie.plot.ilike(f'%{q}%')]
        if hasattr(Movie, 'imdb_id'):
            search_filters.append(Movie.imdb_id.ilike(f'%{q}%'))
        base_q = base_q.filter(or_(*search_filters))
    if media_type in ('movie', 'series'):
        base_q = base_q.filter(Movie.media_type == media_type)
    if category:
        base_q = base_q.filter(Movie.category.ilike(f'%{category}%'))
    if year_from:
        base_q = base_q.filter(Movie.year >= str(year_from))
    if year_to:
        base_q = base_q.filter(Movie.year <= str(year_to))
    if rating_min is not None:
        base_q = base_q.filter(cast(func.nullif(Movie.rating, 'N/A'), Float) >= rating_min)
    if rating_max is not None:
        base_q = base_q.filter(cast(func.nullif(Movie.rating, 'N/A'), Float) <= rating_max)
    if imdb and hasattr(Movie, 'imdb_id'):
        base_q = base_q.filter(Movie.imdb_id.ilike(f'%{imdb}%'))
    if lang:
        if lang == 'en':
            base_q = base_q.filter(Movie.english_srt.isnot(None), Movie.english_srt != '')
        else:
            base_q = base_q.filter(Movie.translations.any(TranslationCache.language == lang))

    sort_mapping = {
        'newest':      Movie.id.desc(),
        'oldest':      Movie.id.asc(),
        'downloads':   Movie.views.desc(),
        'rating_desc': cast(func.nullif(Movie.rating, 'N/A'), Float).desc().nulls_last(),
        'rating_asc':  cast(func.nullif(Movie.rating, 'N/A'), Float).asc().nulls_last(),
        'title_asc':   Movie.title.asc(),
        'title_desc':  Movie.title.desc()
    }
    base_q     = base_q.order_by(sort_mapping.get(sort, Movie.id.desc()))
    pagination = base_q.paginate(page=page, per_page=12, error_out=False)

    return render_template('search.html', pagination=pagination,
                           categories=get_categories_list(),
                           current_filters={
                               'q': q, 'type': media_type,
                               'year_from': year_from, 'year_to': year_to,
                               'rating_min': rating_min, 'rating_max': rating_max,
                               'category': category, 'lang': lang,
                               'imdb': imdb, 'sort': sort
                           })

@app.route('/favorites')
def favorites_page():
    sid    = get_session_id()
    favs   = Favorite.query.filter_by(session_id=sid).order_by(Favorite.created_at.desc()).all()
    movies = [f.movie for f in favs if f.movie]
    return render_template('favorites.html', movies=movies)

@app.route('/api/toggle_favorite', methods=['POST'])
def toggle_favorite():
    sid      = get_session_id()
    movie_id = request.json.get('movie_id')
    if not movie_id:
        return jsonify({"error": "movie_id required"}), 400
    existing = Favorite.query.filter_by(session_id=sid, movie_id=movie_id).first()
    if existing:
        db.session.delete(existing)
        db.session.commit()
        return jsonify({"status": "removed"})
    else:
        db.session.add(Favorite(session_id=sid, movie_id=movie_id))
        db.session.commit()
        return jsonify({"status": "added"})

@app.route('/api/favorites_list')
def favorites_list():
    sid = get_session_id()
    ids = [f.movie_id for f in Favorite.query.filter_by(session_id=sid).all()]
    return jsonify({"favorites": ids})

@app.route('/api/recently_viewed')
def recently_viewed():
    ids = session.get('recently_viewed', [])[:10]
    if not ids:
        return jsonify({"movies": []})
    movies = {m.id: m for m in Movie.query.filter(Movie.id.in_(ids)).all()}
    result = []
    for mid in ids:
        m = movies.get(mid)
        if m:
            result.append({
                "id": m.id, "title": m.title, "year": m.year,
                "rating": m.rating, "poster": m.poster_url,
                "slug": m.slug, "type": m.media_type
            })
    return jsonify({"movies": result})

@app.route('/api/rate_subtitle', methods=['POST'])
def rate_subtitle():
    sid      = get_session_id()
    data     = request.json
    movie_id = data.get('movie_id')
    language = data.get('language')
    rating   = data.get('rating')
    comment  = data.get('comment', '').strip()[:500]

    if not all([movie_id, language, rating]):
        return jsonify({"error": "movie_id, language and rating are required"}), 400
    if int(rating) not in range(1, 6):
        return jsonify({"error": "rating must be 1–5"}), 400

    existing = SubtitleRating.query.filter_by(
        session_id=sid, movie_id=movie_id, language=language
    ).first()
    if existing:
        existing.rating  = int(rating)
        existing.comment = comment
    else:
        db.session.add(SubtitleRating(
            movie_id=movie_id, language=language,
            rating=int(rating), comment=comment, session_id=sid
        ))
    db.session.commit()
    return jsonify({"ok": True})

@app.route('/api/ratings/<int:movie_id>/<language>')
def get_ratings(movie_id, language):
    rows = SubtitleRating.query.filter_by(movie_id=movie_id, language=language)\
                               .order_by(SubtitleRating.created_at.desc()).all()
    if not rows:
        return jsonify({"avg": None, "count": 0, "comments": []})
    avg = round(sum(r.rating for r in rows) / len(rows), 1)
    comments = [{"rating": r.rating, "comment": r.comment,
                 "date": r.created_at.strftime('%b %d, %Y') if r.created_at else ''}
                for r in rows[:10] if r.comment]
    return jsonify({"avg": avg, "count": len(rows), "comments": comments})

@app.route('/requests')
def subtitle_requests_page():
    sort  = request.args.get('sort', 'votes')
    order = SubtitleRequest.created_at.desc() if sort == 'newest' else SubtitleRequest.votes.desc()
    page  = request.args.get('page', 1, type=int)
    pagination = SubtitleRequest.query.filter(
        SubtitleRequest.status != 'Done'
    ).order_by(order).paginate(page=page, per_page=20, error_out=False)
    sid   = get_session_id()
    voted = {v.request_id for v in RequestVote.query.filter_by(session_id=sid).all()}
    return render_template('requests.html', pagination=pagination, voted=voted, sort=sort)

@app.route('/api/submit_request', methods=['POST'])
def submit_request():
    data    = request.json
    title   = (data.get('title') or '').strip()[:200]
    details = (data.get('details') or '').strip()[:1000]

    if not title:
        return jsonify({"error": "Title is required"}), 400

    sid      = get_session_id()
    week_ago = datetime.utcnow() - timedelta(days=7)
    duplicate = SubtitleRequest.query.filter(
        SubtitleRequest.title.ilike(title),
        SubtitleRequest.created_at >= week_ago
    ).first()
    if duplicate:
        already = RequestVote.query.filter_by(session_id=sid, request_id=duplicate.id).first()
        if not already:
            duplicate.votes += 1
            db.session.add(RequestVote(request_id=duplicate.id, session_id=sid))
            db.session.commit()
            return jsonify({"ok": True, "message": "Similar request found – your vote was added!", "id": duplicate.id})
        return jsonify({"ok": True, "message": "Request already exists.", "id": duplicate.id})

    new_req = SubtitleRequest(title=title, details=details, votes=1)
    db.session.add(new_req)
    db.session.flush()
    db.session.add(RequestVote(request_id=new_req.id, session_id=sid))
    db.session.commit()

    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    CHANNEL_ID     = os.environ.get('TELEGRAM_CHANNEL_ID')
    if TELEGRAM_TOKEN and CHANNEL_ID:
        try:
            msg = (f"🔔 *New Subtitle Request*\n\n"
                   f"🎬 *Title:* {title}\n"
                   f"📝 *Details:* {details or 'None'}\n\n"
                   f"_Vote here:_ {WEBSITE_BASE_URL}/requests")
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": CHANNEL_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=5
            )
        except Exception:
            pass

    return jsonify({"ok": True, "message": "Request submitted!", "id": new_req.id})

@app.route('/api/vote_request/<int:request_id>', methods=['POST'])
def vote_request(request_id):
    sid  = get_session_id()
    req  = SubtitleRequest.query.get_or_404(request_id)
    already = RequestVote.query.filter_by(session_id=sid, request_id=request_id).first()
    if already:
        return jsonify({"error": "Already voted"}), 409
    req.votes += 1
    db.session.add(RequestVote(request_id=request_id, session_id=sid))
    db.session.commit()
    return jsonify({"ok": True, "votes": req.votes})

@app.route('/api/movie_info/<int:movie_id>')
def movie_info(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    return jsonify({
        'id': movie.id, 'title': movie.title, 'year': movie.year,
        'rating': movie.rating, 'poster_url': movie.poster_url,
        'slug': movie.slug, 'type': movie.media_type
    })

# ===========================================================================
#  ADMIN
# ===========================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if (request.form['username'] == os.environ.get('ADMIN_USER', 'admin') and
                request.form['password'] == os.environ.get('ADMIN_PASS', 'change-me')):
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Invalid credentials.')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    TranslationJob.query.filter(TranslationJob.status.in_(['Completed', 'Success'])).delete(synchronize_session=False)
    db.session.commit()

    stat          = SiteStat.query.first()
    total_dl      = db.session.query(func.sum(TranslationCache.downloads)).scalar() or 0
    total_subs    = TranslationCache.query.count()
    total_movies  = Movie.query.filter_by(media_type='movie').count()
    total_series  = Movie.query.filter(Movie.media_type == 'series').distinct(Movie.title).count()
    page          = request.args.get('page', 1, type=int)
    all_media_paginated = Movie.query.order_by(Movie.id.desc()).paginate(page=page, per_page=15, error_out=False)
    pop_lang      = db.session.query(TranslationCache.language, func.count(TranslationCache.id))\
                              .group_by(TranslationCache.language)\
                              .order_by(func.count(TranslationCache.id).desc()).first()
    recent_jobs   = TranslationJob.query.order_by(TranslationJob.id.desc()).limit(15).all()
    failed_jobs_count = TranslationJob.query.filter_by(status='Failed').count()

    try:
        db_size = db.session.execute(db.text("SELECT pg_size_pretty(pg_database_size(current_database()))")).scalar() or "Unknown"
    except Exception:
        db_size = "Error reading DB"

    r2_size_str, r2_file_count = "Not Connected", 0
    if s3_client and r2_bucket:
        try:
            total_bytes = 0
            paginator   = s3_client.get_paginator('list_objects_v2')
            for page_obj in paginator.paginate(Bucket=r2_bucket):
                for obj in page_obj.get('Contents', []):
                    total_bytes   += obj['Size']
                    r2_file_count += 1
            if total_bytes < 1024 * 1024:
                r2_size_str = f"{total_bytes / 1024:.2f} KB"
            elif total_bytes < 1024 ** 3:
                r2_size_str = f"{total_bytes / (1024 * 1024):.2f} MB"
            else:
                r2_size_str = f"{total_bytes / (1024 ** 3):.2f} GB"
        except Exception:
            r2_size_str = "Read Error"

    scheduler_logs   = SchedulerLog.query.order_by(SchedulerLog.id.desc()).limit(5).all()
    pending_requests = SubtitleRequest.query.filter_by(status='Pending').count()
    total_ratings    = SubtitleRating.query.count()
    bot_users        = TelegramUser.query.count()
    bot_subs         = TelegramSubscription.query.filter_by(notified=False).count()

    return render_template('dashboard.html',
                           visitors=stat.total_visitors,
                           downloads=total_dl,
                           total_subtitles=total_subs,
                           popular_lang=pop_lang,
                           all_media=all_media_paginated,
                           jobs=recent_jobs,
                           db_size=db_size,
                           r2_size_str=r2_size_str,
                           r2_file_count=r2_file_count,
                           total_movies=total_movies,
                           total_series=total_series,
                           failed_jobs_count=failed_jobs_count,
                           scheduler_logs=scheduler_logs,
                           pending_requests=pending_requests,
                           total_ratings=total_ratings,
                           bot_users=bot_users,
                           bot_subs=bot_subs)

@app.route('/api/analytics_data')
@login_required
def analytics_data():
    twelve_months_ago = datetime.utcnow() - timedelta(days=365)
    monthly_raw = db.session.execute(db.text("""
        SELECT TO_CHAR(created_at, 'YYYY-MM') AS month, COUNT(*) AS cnt
        FROM movie
        WHERE created_at >= :cutoff
        GROUP BY month ORDER BY month ASC
    """), {"cutoff": twelve_months_ago}).fetchall()
    uploads_by_month = {"labels": [r[0] for r in monthly_raw],
                        "data":   [int(r[1]) for r in monthly_raw]}

    lang_raw = db.session.query(
        TranslationCache.language, func.sum(TranslationCache.downloads)
    ).group_by(TranslationCache.language).all()
    lang_map = {'ml': 'Malayalam', 'ta': 'Tamil', 'hi': 'Hindi', 'en': 'English'}
    downloads_by_lang = {
        "labels": [lang_map.get(r[0], r[0]) for r in lang_raw],
        "data":   [int(r[1] or 0) for r in lang_raw]
    }

    top_views = Movie.query.order_by(Movie.views.desc()).limit(10).all()
    views_top10 = {
        "labels": [m.title[:30] for m in top_views],
        "data":   [m.views for m in top_views]
    }

    job_statuses = db.session.query(
        TranslationJob.status, func.count(TranslationJob.id)
    ).group_by(TranslationJob.status).all()
    jobs_summary = {
        "labels": [r[0] for r in job_statuses],
        "data":   [int(r[1]) for r in job_statuses]
    }

    top_reqs = SubtitleRequest.query.order_by(SubtitleRequest.votes.desc()).limit(10).all()
    requests_top10 = {
        "labels": [r.title[:30] for r in top_reqs],
        "data":   [r.votes for r in top_reqs]
    }

    return jsonify({
        "uploads_by_month":  uploads_by_month,
        "downloads_by_lang": downloads_by_lang,
        "views_top10":       views_top10,
        "jobs_summary":      jobs_summary,
        "requests_top10":    requests_top10
    })

@app.route('/admin/requests')
@login_required
def admin_requests():
    page          = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status', '')
    q = SubtitleRequest.query
    if status_filter:
        q = q.filter_by(status=status_filter)
    pagination = q.order_by(SubtitleRequest.votes.desc()).paginate(page=page, per_page=25, error_out=False)
    return render_template('admin_requests.html', pagination=pagination, status_filter=status_filter)

@app.route('/admin/update_request/<int:req_id>', methods=['POST'])
@login_required
def update_request_status(req_id):
    req = SubtitleRequest.query.get_or_404(req_id)
    new_status = request.form.get('status')
    if new_status in ('Pending', 'InProgress', 'Done'):
        req.status = new_status
        db.session.commit()
        log_admin_action('UpdateRequest', f"Request ID {req_id} → {new_status}")
    return redirect(url_for('admin_requests'))

@app.route('/admin/delete_request/<int:req_id>')
@login_required
def delete_request(req_id):
    req = SubtitleRequest.query.get_or_404(req_id)
    db.session.delete(req)
    db.session.commit()
    return redirect(url_for('admin_requests'))

@app.route('/admin/reset_jobs')
@login_required
def reset_jobs():
    for job in TranslationJob.query.filter_by(status='Processing').all():
        job.status = 'Pending'
    db.session.commit()
    for job in TranslationJob.query.filter_by(status='Pending').all():
        movie = Movie.query.get(job.movie_id)
        if movie and movie.english_srt:
            threading.Thread(target=trigger_hf_translation, args=(movie.id, movie.english_srt)).start()
    return redirect(url_for('dashboard'))

@app.route('/admin/retry_failed')
@login_required
def retry_failed():
    failed = TranslationJob.query.filter_by(status='Failed').all()
    for job in failed:
        job.status   = 'Pending'
        job.progress = 0
        movie = Movie.query.get(job.movie_id)
        if movie and movie.english_srt:
            threading.Thread(target=trigger_hf_translation, args=(movie.id, movie.english_srt)).start()
    db.session.commit()
    return redirect(url_for('dashboard'))

@app.route('/admin/queue_all_missing')
@login_required
def queue_all_missing():
    movies = Movie.query.filter(
        Movie.english_srt.isnot(None),
        Movie.english_srt != '',
        ~Movie.translations.any()
    ).all()
    for movie in movies:
        threading.Thread(target=trigger_hf_translation, args=(movie.id, movie.english_srt)).start()
    return redirect(url_for('dashboard'))

@app.route('/admin/export_csv')
@login_required
def export_csv():
    import csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Title', 'Year', 'Type', 'IMDb ID', 'Languages', 'Downloads', 'Avg Rating'])
    for m in Movie.query.order_by(Movie.id.desc()).all():
        langs   = ', '.join([c.language for c in m.translations])
        ratings = SubtitleRating.query.filter_by(movie_id=m.id).all()
        avg_r   = round(sum(r.rating for r in ratings) / len(ratings), 1) if ratings else ''
        writer.writerow([m.title, m.year, m.media_type, m.imdb_id, langs, m.views, avg_r])
    output.seek(0)
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=media_export.csv"})

@app.route('/admin/queue_translations/<int:movie_id>')
@login_required
def queue_translations(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    if movie.english_srt:
        threading.Thread(target=trigger_hf_translation, args=(movie.id, movie.english_srt)).start()
    return redirect(url_for('dashboard'))

@app.route('/admin/delete_job/<int:job_id>')
@login_required
def delete_job(job_id):
    job = TranslationJob.query.get_or_404(job_id)
    db.session.delete(job)
    db.session.commit()
    return redirect(url_for('dashboard'))

@app.route('/admin/series')
@login_required
def admin_series():
    from collections import defaultdict
    all_eps = (Movie.query
               .filter_by(media_type='series')
               .options(joinedload(Movie.translations))
               .order_by(Movie.title.asc(), Movie.season.asc(), Movie.episode.asc())
               .all())

    series_map = defaultdict(list)
    for ep in all_eps:
        series_map[ep.title].append(ep)

    series_list = []
    for title, eps in series_map.items():
        seasons = defaultdict(list)
        for ep in eps:
            seasons[ep.season or 1].append(ep)

        total_eps     = len(eps)
        season_count  = len(seasons)
        representative = eps[0]
        en_count   = sum(1 for e in eps if e.english_srt)
        full_count = sum(1 for e in eps if e.english_srt and len(e.translations) >= 3)

        if full_count == total_eps:
            health = 'full'
        elif en_count == total_eps:
            health = 'partial'
        elif en_count > 0:
            health = 'incomplete'
        else:
            health = 'none'

        gaps = []
        for s_num, s_eps in seasons.items():
            ep_nums = sorted(e.episode for e in s_eps if e.episode)
            if ep_nums:
                for i in range(ep_nums[0], ep_nums[-1] + 1):
                    if i not in ep_nums:
                        gaps.append(f"S{s_num:02d}E{i:02d}")

        series_list.append({
            'title': title, 'rep': representative,
            'total_eps': total_eps, 'season_count': season_count,
            'seasons': dict(seasons), 'en_count': en_count,
            'full_count': full_count, 'health': health,
            'gaps': gaps, 'imdb_id': representative.imdb_id,
        })

    series_list.sort(key=lambda s: max(e.id for e in s['seasons'].get(1, s['rep'] and [s['rep']] or [])), reverse=True)
    return render_template('admin_series.html', series_list=series_list, categories=get_categories_list())

@app.route('/admin/series/delete_all/<path:title>', methods=['POST'])
@login_required
def delete_series_all(title):
    eps   = Movie.query.filter_by(media_type='series', title=title).all()
    count = len(eps)
    for ep in eps:
        db.session.delete(ep)
    db.session.commit()
    log_admin_action('DeleteSeries', f"Deleted all {count} episodes of '{title}'")
    return jsonify({'ok': True, 'deleted': count})

@app.route('/admin/series/fetch_missing/<path:title>', methods=['POST'])
@login_required
def fetch_missing_series(title):
    eps = (Movie.query
           .filter_by(media_type='series', title=title)
           .filter(Movie.english_srt.isnot(None), Movie.english_srt != '')
           .all())
    queued = 0
    for ep in eps:
        if not ep.translations:
            threading.Thread(target=trigger_hf_translation, args=(ep.id, ep.english_srt)).start()
            queued += 1
    return jsonify({'ok': True, 'queued': queued})

@app.route('/admin/duplicates')
@login_required
def admin_duplicates():
    imdb_dups = []
    imdb_counts = (db.session.query(Movie.imdb_id, Movie.media_type,
                                    func.count(Movie.id).label('cnt'))
                   .filter(Movie.imdb_id.isnot(None), Movie.imdb_id != '')
                   .group_by(Movie.imdb_id, Movie.media_type)
                   .having(func.count(Movie.id) > 1).all())
    for row in imdb_counts:
        movies = Movie.query.filter_by(imdb_id=row.imdb_id, media_type=row.media_type).all()
        if len(movies) > 1:
            imdb_dups.append({'key': row.imdb_id, 'type': row.media_type, 'movies': movies})

    title_dups = []
    title_counts = (db.session.query(Movie.title, Movie.year, func.count(Movie.id).label('cnt'))
                    .filter(Movie.media_type == 'movie')
                    .group_by(Movie.title, Movie.year)
                    .having(func.count(Movie.id) > 1).all())
    for row in title_counts:
        movies   = Movie.query.filter_by(title=row.title, year=row.year, media_type='movie').all()
        imdb_ids = {m.imdb_id for m in movies if m.imdb_id}
        if len(movies) > 1 and (len(imdb_ids) > 1 or not imdb_ids):
            title_dups.append({'key': f"{row.title} ({row.year})", 'movies': movies})

    return render_template('admin_duplicates.html',
                           imdb_dups=imdb_dups, title_dups=title_dups,
                           total=len(imdb_dups) + len(title_dups),
                           categories=get_categories_list())

@app.route('/admin/bulk_delete', methods=['POST'])
@login_required
def bulk_delete():
    data = request.json
    ids  = data.get('ids', [])
    if not ids:
        return jsonify({'error': 'No IDs provided'}), 400
    deleted = 0
    for mid in ids:
        m = Movie.query.get(int(mid))
        if m:
            db.session.delete(m)
            deleted += 1
    db.session.commit()
    log_admin_action('BulkDelete', f"Deleted {deleted} entries: {ids}")
    return jsonify({'ok': True, 'deleted': deleted})

@app.route('/delete/<int:movie_id>')
@login_required
def delete_media(movie_id):
    media = Movie.query.get_or_404(movie_id)
    db.session.delete(media)
    db.session.commit()
    log_admin_action('Delete', f"Deleted ID {movie_id}")
    return redirect(url_for('dashboard'))

@app.route('/admin/edit/<int:movie_id>', methods=['GET', 'POST'])
@login_required
def edit_media(movie_id):
    media = Movie.query.get_or_404(movie_id)
    if request.method == 'POST':
        media.title      = request.form.get('title')
        media.year       = request.form.get('year')
        media.rating     = request.form.get('rating')
        media.poster_url = request.form.get('poster_url')
        media.category   = request.form.get('category')
        media.plot       = request.form.get('plot')
        media.runtime    = request.form.get('runtime')
        if media.media_type == 'series':
            media.season  = request.form.get('season')
            media.episode = request.form.get('episode')
        media.slug = _unique_slug(media.title, media.year, exclude_id=media.id)

        new_srt = request.files.get('new_srt')
        if new_srt and new_srt.filename:
            content      = new_srt.read().decode('utf-8', errors='ignore')
            storage_data = content
            if s3_client and r2_bucket:
                safe_title  = media.title.replace(" ", "_").replace("/", "").lower()
                ep_tag      = f"_s{media.season}e{media.episode}" if media.media_type == 'series' else ""
                r2_filename = f"english_{safe_title}{ep_tag}_edit_{os.urandom(4).hex()}.srt"
                try:
                    s3_client.put_object(Bucket=r2_bucket, Key=r2_filename,
                                         Body=content.encode('utf-8'),
                                         ContentType='application/x-subrip')
                    storage_data = f"{r2_public_url}/{r2_filename}"
                except Exception as e:
                    print(f"R2 Upload Failed on Edit: {e}")
            media.english_srt = storage_data
            TranslationCache.query.filter_by(movie_id=media.id).delete()
            TranslationJob.query.filter_by(movie_id=media.id).delete()
            db.session.commit()
            threading.Thread(target=trigger_hf_translation, args=(media.id, storage_data)).start()

        db.session.commit()
        log_admin_action('Edit', f"Edited ID {movie_id}")
        return redirect(url_for('dashboard'))
    return render_template('edit.html', media=media)

@app.route('/api/tmdb_search')
@login_required
def tmdb_search():
    query     = request.args.get('query')
    tmdb_type = request.args.get('type')
    api_key   = os.environ.get('TMDB_API_KEY')
    url       = f"https://api.themoviedb.org/3/search/{tmdb_type}?api_key={api_key}&query={urllib.parse.quote(query)}"
    try:
        return jsonify(requests.get(url).json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/tmdb_details')
@login_required
def tmdb_details():
    tmdb_id   = request.args.get('id')
    tmdb_type = request.args.get('type')
    api_key   = os.environ.get('TMDB_API_KEY')
    url       = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}?api_key={api_key}&append_to_response=external_ids"
    try:
        return jsonify(requests.get(url).json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/tmdb_season_episodes')
@login_required
def tmdb_season_episodes():
    imdb_id    = (request.args.get('imdb_id') or '').strip()
    season_num = request.args.get('season', type=int)
    api_key    = os.environ.get('TMDB_API_KEY')

    if not imdb_id or not season_num or not api_key:
        return jsonify({"error": "Missing imdb_id, season or TMDB_API_KEY"}), 400
    if not imdb_id.startswith('tt'):
        imdb_id = 'tt' + imdb_id

    try:
        find = requests.get(
            f"https://api.themoviedb.org/3/find/{imdb_id}"
            f"?api_key={api_key}&external_source=imdb_id", timeout=10).json()
        tv = find.get('tv_results', [])
        if not tv:
            return jsonify({"error": "Series not found on TMDB"}), 404
        series_id = tv[0]['id']
        s_data    = requests.get(
            f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_num}"
            f"?api_key={api_key}", timeout=10).json()
        if 'episodes' not in s_data:
            return jsonify({"error": f"Season {season_num} not found on TMDB"}), 404
        episodes = [{"episode_number": e["episode_number"], "name": e.get("name", "")}
                    for e in s_data["episodes"]]
        return jsonify({"season": season_num, "episodes": episodes, "total": len(episodes)})
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

def tmdb_episode_exists(imdb_id: str, season: int, episode: int) -> bool:
    ok, _ = tmdb_episode_exists_strict(imdb_id, season, episode)
    return ok

def tmdb_episode_exists_strict(imdb_id: str, season: int, episode: int):
    api_key = os.environ.get('TMDB_API_KEY')
    if not api_key:
        return True, None
    try:
        find_data  = requests.get(
            f"https://api.themoviedb.org/3/find/{imdb_id}"
            f"?api_key={api_key}&external_source=imdb_id", timeout=10).json()
        tv_results = find_data.get('tv_results', [])
        if not tv_results:
            return True, None
        series_id   = tv_results[0]['id']
        season_resp = requests.get(
            f"https://api.themoviedb.org/3/tv/{series_id}/season/{season}"
            f"?api_key={api_key}", timeout=10)
        if season_resp.status_code != 200:
            return False, 0
        episodes = season_resp.json().get('episodes', [])
        total    = len(episodes)
        ep_nums  = [e.get('episode_number') for e in episodes]
        return episode in ep_nums, total
    except Exception:
        return True, None

def generate_slug(title, year):
    base = re.sub(r'[^\w\s-]', '', (title or '').lower().strip())
    base = re.sub(r'[-\s]+', '-', base)
    return f"{base}-{year}" if year else base

def _unique_slug(title, year, exclude_id=None):
    base = generate_slug(title, year)
    slug = base
    q    = Movie.query.filter_by(slug=slug)
    if exclude_id:
        q = q.filter(Movie.id != exclude_id)
    while q.first():
        slug = f"{base}-{os.urandom(2).hex()}"
        q    = Movie.query.filter_by(slug=slug)
        if exclude_id:
            q = q.filter(Movie.id != exclude_id)
    return slug

@app.route('/api/auto_fetch_srt', methods=['POST'])
@login_required
def auto_fetch_srt():
    data       = request.json
    imdb_id    = (data.get('imdb_id') or '').strip()
    media_type = data.get('media_type', 'movie')
    season     = data.get('season')
    episode    = data.get('episode')

    SUBDL_API_KEY = os.environ.get('SUBDL_API_KEY')
    OS_API_KEY    = os.environ.get('OS_API_KEY')

    if not imdb_id or imdb_id in ('undefined', 'null', ''):
        return jsonify({"error": "Missing IMDb ID. Auto-fill from TMDB first."}), 400
    if not imdb_id.startswith('tt'):
        imdb_id = 'tt' + imdb_id
    clean_imdb = imdb_id.replace('tt', '')

    try:
        season  = int(season)  if season  not in (None, '', 'null') else None
    except (ValueError, TypeError):
        season = None
    try:
        episode = int(episode) if episode not in (None, '', 'null') else None
    except (ValueError, TypeError):
        episode = None

    if media_type == 'series':
        if not season or not episode:
            return jsonify({"error": "Season and Episode number are required for series."}), 400
        exists, total_eps = tmdb_episode_exists_strict(imdb_id, season, episode)
        if not exists:
            extra = f" Season {season} only has {total_eps} episode(s)." if total_eps else ""
            return jsonify({"error": f"S{season:02d}E{episode:02d} does not exist.{extra}"}), 400

    ua      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"
    headers = {"User-Agent": ua}
    errors  = []

    def release_matches(name, s, e):
        for pat in [r'[Ss](\d{1,2})[Ee](\d{1,2})',
                    r'[Ss]eason\s*(\d+).*?[Ee]pisode\s*(\d+)',
                    r'(\d{1,2})x(\d{2})']:
            m = re.search(pat, name or '')
            if m:
                try:
                    if int(m.group(1)) == s and int(m.group(2)) == e:
                        return True
                except (ValueError, TypeError):
                    pass
        return False

    def extract_srt(raw_bytes, s=None, e=None):
        try:
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as z:
                srts = [f for f in z.namelist() if f.lower().endswith('.srt')]
                if not srts:
                    return None
                if s and e:
                    for fn in srts:
                        if release_matches(fn, s, e):
                            return z.read(fn).decode('utf-8', errors='ignore')
                return z.read(srts[0]).decode('utf-8', errors='ignore')
        except Exception as ex:
            errors.append(f"ZIP error: {ex}")
            return None

    if SUBDL_API_KEY:
        try:
            qtype = 'tv' if media_type == 'series' else 'movie'
            url   = (f"https://api.subdl.com/api/v1/subtitles"
                     f"?api_key={SUBDL_API_KEY}&imdb_id={imdb_id}"
                     f"&type={qtype}&languages=EN")
            if media_type == 'series':
                url += f"&season_number={season}&episode_number={episode}"
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                errors.append(f"Subdl HTTP {resp.status_code}")
            else:
                subs = resp.json().get('subtitles', []) if resp.json().get('status') else []
                if media_type == 'series' and subs:
                    matched = []
                    for sub in subs:
                        ss, se = sub.get('season'), sub.get('episode')
                        try:
                            if ss is not None and se is not None:
                                if int(ss) == season and int(se) == episode:
                                    matched.append(sub); continue
                        except (ValueError, TypeError):
                            pass
                        if release_matches(sub.get('release_name', ''), season, episode):
                            matched.append(sub)
                    if not matched:
                        errors.append(f"Subdl: {len(subs)} result(s) but none match S{season:02d}E{episode:02d}")
                        subs = []
                    else:
                        subs = matched

                def score_sub(s):
                    if s.get('hearing_impaired'): return (-1, 0, 0)
                    return (2 if s.get('format','').lower()=='srt' else 0,
                            int(s.get('downloads', 0) or 0),
                            float(s.get('rating', 0) or 0))

                ranked = sorted([(score_sub(s), s) for s in subs if score_sub(s)[0] >= 0],
                                key=lambda x: x[0], reverse=True)
                for _, best in ranked:
                    dl = requests.get("https://dl.subdl.com" + best['url'], headers=headers, timeout=20)
                    if dl.status_code != 200: continue
                    raw = dl.content
                    txt = (extract_srt(raw, season, episode) if raw[:2] == b'PK'
                           else raw.decode('utf-8', errors='ignore'))
                    if txt and txt.strip():
                        return jsonify({"success": True, "srt_text": txt, "source": "Subdl"})
                if ranked:
                    errors.append("Subdl: could not extract valid SRT from download")
        except Exception as ex:
            errors.append(f"Subdl crash: {ex}")

    if OS_API_KEY:
        try:
            os_h = {"Api-Key": OS_API_KEY,
                    "Content-Type": "application/json",
                    "User-Agent": "malayalamsubtitles_app v1.0"}
            if media_type == 'series':
                url = (f"https://api.opensubtitles.com/api/v1/subtitles"
                       f"?parent_imdb_id={clean_imdb}"
                       f"&season_number={season}&episode_number={episode}&languages=en")
            else:
                url = (f"https://api.opensubtitles.com/api/v1/subtitles"
                       f"?imdb_id={clean_imdb}&languages=en")
            sr = requests.get(url, headers=os_h, timeout=15)
            if sr.status_code != 200:
                errors.append(f"OS HTTP {sr.status_code}")
            else:
                items = sr.json().get('data', [])
                if media_type == 'series':
                    def os_match(item):
                        attr = item.get('attributes', {})
                        try:
                            if (int(attr.get('season_number', -1)) == season and
                                    int(attr.get('episode_number', -1)) == episode):
                                return True
                        except (ValueError, TypeError):
                            pass
                        for f in attr.get('files', []):
                            if release_matches(f.get('file_name',''), season, episode):
                                return True
                        return release_matches(attr.get('release',''), season, episode)
                    items = [i for i in items if os_match(i)]
                if not items:
                    errors.append(f"OS: no results for S{season:02d}E{episode:02d}" if media_type == 'series' else "OS: no results")
                else:
                    fid  = items[0]['attributes']['files'][0]['file_id']
                    dl_r = requests.post("https://api.opensubtitles.com/api/v1/download",
                                         headers=os_h, json={"file_id": fid}, timeout=15)
                    if dl_r.status_code == 200:
                        link = dl_r.json().get('link','')
                        if link:
                            raw = requests.get(link, headers=headers, timeout=20).content
                            txt = (extract_srt(raw, season, episode) if raw[:2] == b'PK'
                                   else raw.decode('utf-8', errors='ignore'))
                            if txt and txt.strip():
                                return jsonify({"success": True, "srt_text": txt, "source": "OpenSubtitles"})
                        errors.append("OS: empty download link")
                    else:
                        errors.append(f"OS download HTTP {dl_r.status_code}")
        except Exception as ex:
            errors.append(f"OS crash: {ex}")

    return jsonify({"error": " | ".join(errors) or "Not found on any source"}), 404

@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    if request.method == 'POST':
        media_type    = request.form.get('media_type', 'movie')
        title         = request.form.get('title', 'Unknown Title')
        season_raw    = request.form.get('season', '')
        year          = request.form.get('year', '')
        rating        = request.form.get('rating', '0')
        poster_url    = request.form.get('poster_url', '')
        silent_upload = request.form.get('silent_upload')
        plot          = request.form.get('plot', '')
        runtime       = request.form.get('runtime', '')

        categories      = request.form.getlist('category')
        category_string = ", ".join(categories)
        if silent_upload == 'yes':
            category_string += ", SilentMode"

        fetched_srts     = request.form.getlist('fetched_srts[]')
        fetched_episodes = request.form.getlist('fetched_episodes[]')
        files            = request.files.getlist('files[]')
        manual_episodes  = request.form.getlist('episodes[]')

        valid_files      = [f for f in files if f and f.filename]
        valid_manual_eps = [ep for ep in manual_episodes if ep.strip()]

        def ep_from_filename(name):
            m = re.search(r'[Ss]\d{1,2}[Ee](\d{1,2})', name)
            if m: return str(int(m.group(1)))
            m = re.search(r'[Ee](\d{1,2})', name)
            if m: return str(int(m.group(1)))
            m = re.search(r'(\d{1,2})(?=\.srt)', name, re.IGNORECASE)
            if m: return str(int(m.group(1)))
            return None

        items_to_process = []

        for i, text in enumerate(fetched_srts):
            if text.strip():
                ep = fetched_episodes[i] if i < len(fetched_episodes) else str(i + 1)
                items_to_process.append((text, ep))

        ep_counter = len(items_to_process) + 1
        for i, srt_file in enumerate(valid_files):
            fname = srt_file.filename or ''
            raw   = srt_file.read()

            if fname.lower().endswith('.zip') or raw[:2] == b'PK':
                try:
                    with zipfile.ZipFile(io.BytesIO(raw)) as z:
                        srt_names = sorted([n for n in z.namelist() if n.lower().endswith('.srt')])
                        for srt_name in srt_names:
                            srt_content = z.read(srt_name).decode('utf-8', errors='ignore')
                            if not srt_content.strip(): continue
                            detected_ep = ep_from_filename(srt_name)
                            if not detected_ep:
                                detected_ep = (valid_manual_eps[ep_counter - 1]
                                               if ep_counter - 1 < len(valid_manual_eps)
                                               else str(ep_counter))
                                ep_counter += 1
                            items_to_process.append((srt_content, detected_ep))
                except Exception as ze:
                    print(f"ZIP extract error: {ze}")
            else:
                srt_content = raw.decode('utf-8', errors='ignore')
                ep = valid_manual_eps[i] if i < len(valid_manual_eps) else str(i + 1)
                items_to_process.append((srt_content, ep))

        try:
            safe_season = int(season_raw) if season_raw and str(season_raw).strip() else None
        except ValueError:
            safe_season = 1

        last_movie = None
        for content, ep_str in items_to_process:
            safe_content = content.replace('\x00', '')
            storage_data = safe_content

            try:
                current_ep = int(ep_str) if media_type == 'series' and ep_str and str(ep_str).strip() else None
            except ValueError:
                current_ep = 1

            if s3_client and r2_bucket:
                safe_title_str = title.replace(" ", "_").replace("/", "").lower()
                ep_tag         = f"_s{safe_season}e{current_ep}" if media_type == 'series' else ""
                r2_filename    = f"english_{safe_title_str}{ep_tag}_{os.urandom(4).hex()}.srt"
                try:
                    s3_client.put_object(Bucket=r2_bucket, Key=r2_filename,
                                         Body=safe_content.encode('utf-8'),
                                         ContentType='application/x-subrip')
                    storage_data = f"{r2_public_url}/{r2_filename}"
                except Exception as e:
                    print(f"R2 Upload Failed: {e}")

            new_media = Movie(
                media_type=media_type, title=title,
                season=safe_season if media_type == 'series' else None,
                episode=current_ep, year=year, rating=rating,
                poster_url=poster_url, english_srt=storage_data,
                category=category_string, plot=plot, runtime=runtime
            )
            db.session.add(new_media)
            db.session.flush()
            new_media.slug = _unique_slug(title, year)

            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                return f"Database Error during Movie Insert: {str(e)}", 500

            log_admin_action('Upload', f"Added {title} ({year})")
            threading.Thread(target=trigger_hf_translation, args=(new_media.id, storage_data)).start()

            # ── Broadcast to bot subscribers on upload ─────────────────────
            # Only broadcast for movies (series gets too noisy per-episode)
            if media_type == 'movie' and silent_upload != 'yes':
                threading.Thread(target=broadcast_new_subtitle, args=(new_media,), daemon=True).start()

            last_movie = new_media

        # For series: broadcast once after all episodes are uploaded
        if media_type == 'series' and last_movie and silent_upload != 'yes':
            threading.Thread(target=broadcast_new_subtitle, args=(last_movie,), daemon=True).start()

        return redirect(url_for('dashboard'))

    return render_template('admin.html')

# ══════════════════════════════════════════════════════════════
# TELEGRAM SUBSCRIBER NOTIFICATIONS (existing per-title system)
# ══════════════════════════════════════════════════════════════

def notify_telegram_subscribers(movie_id: int, language: str):
    movie = Movie.query.get(movie_id)
    if not movie: return
    subs = TelegramSubscription.query.filter_by(notified=False, language=language).filter(
        db.or_(TelegramSubscription.imdb_id == movie.imdb_id,
               TelegramSubscription.movie_title.ilike(f'%{movie.title}%'))).all()
    TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not TOKEN or not subs: return
    lang_names = {'ml': 'Malayalam', 'ta': 'Tamil', 'hi': 'Hindi', 'en': 'English'}
    link = f"{WEBSITE_BASE_URL}/movie/{movie.slug or movie.id}"
    for sub in subs:
        try:
            lang_label = lang_names.get(language, language.upper())
            year_part  = f" ({movie.year})" if movie.year else ""
            msg = (f"✅ *{lang_label} subtitle ready!*\n\n"
                   f"🎬 *{movie.title}*{year_part}\n\n"
                   f"Your requested subtitle is now available!")
            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                json={"chat_id": sub.chat_id, "text": msg, "parse_mode": "Markdown",
                      "reply_markup": {"inline_keyboard": [[{"text": "📥 Download Now", "url": link}]]}},
                timeout=8)
            sub.notified = True
        except Exception as e:
            print(f"Sub notify error: {e}")
    db.session.commit()


# ══════════════════════════════════════════════════════════════
# EXISTING TELEGRAM WEBHOOK (channel posts — kept as-is)
# ══════════════════════════════════════════════════════════════

@app.route('/api/telegram_webhook', methods=['POST'])
def telegram_webhook():
    """
    Legacy webhook endpoint — handles channel post commands (/start, /search, etc.)
    The new bot webhook is at /bot/webhook and handles all bot interactions.
    This endpoint is kept for backward compatibility if you have it registered
    somewhere, but /bot/webhook is the primary one to register with Telegram.
    """
    TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not TOKEN: return jsonify({"ok": True})
    data    = request.json or {}
    message = data.get('message', {})
    chat_id = str(message.get('chat', {}).get('id', ''))
    text    = (message.get('text') or '').strip()
    if not chat_id or not text: return jsonify({"ok": True})

    def send(msg, buttons=None):
        payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
        if buttons: payload["reply_markup"] = {"inline_keyboard": buttons}
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      json=payload, timeout=8)

    if text == '/start':
        send("👋 *Welcome to MalSubs Bot!*\n\n"
             "📌 *Commands:*\n"
             "🔍 `/search Interstellar` — find a subtitle\n"
             "🙋 `/request Interstellar` — request a missing subtitle\n"
             "📊 `/status` — site statistics\n\n"
             "Or just *type a movie name* to search!")
        return jsonify({"ok": True})

    if text == '/status':
        mv  = Movie.query.filter_by(media_type='movie').count()
        srv = Movie.query.filter(Movie.media_type == 'series').distinct(Movie.title).count()
        trl = TranslationCache.query.count()
        pnd = TranslationJob.query.filter_by(status='Pending').count()
        send(f"📊 *MalSubs*\n\n🎬 Movies: {mv}\n📺 Series: {srv}\n🌐 Translations: {trl}\n⏳ Pending jobs: {pnd}")
        return jsonify({"ok": True})

    query, command = text, None
    if text.startswith('/search '): query, command = text[8:].strip(), 'search'
    elif text.startswith('/request '): query, command = text[9:].strip(), 'request'
    elif text.startswith('/'): send("❓ Unknown command. Try /start"); return jsonify({"ok": True})

    if not query: send("Please add a title. Example: `/search Oppenheimer`"); return jsonify({"ok": True})

    if command == 'request':
        db.session.add(SubtitleRequest(title=query, details="Requested via Telegram bot", votes=1))
        db.session.commit()
        send(f"✅ *Request submitted!*\n🎬 *{query}*\n\nWe'll notify you when ready!\n{WEBSITE_BASE_URL}/requests")
        return jsonify({"ok": True})

    results = Movie.query.filter(Movie.title.ilike(f'%{query}%')).order_by(Movie.views.desc()).limit(6).all()
    seen, deduped = set(), []
    for m in results:
        k = m.title if m.media_type == 'series' else m.id
        if k not in seen: seen.add(k); deduped.append(m)

    if not deduped:
        send(f"😔 No subtitle found for *{query}*",
             buttons=[[{"text": "🙋 Request it", "url": f"{WEBSITE_BASE_URL}/requests"}]])
        return jsonify({"ok": True})

    lines = [f"🔍 Results for *{query}*:\n"]
    btns  = []
    for m in deduped[:4]:
        langs = ('EN ' if m.english_srt else '') + ' '.join(t.language.upper() for t in m.translations)
        lines.append(f"• *{m.title}*{' (' + m.year + ')' if m.year else ''} — {langs.strip() or 'No subs'}")
        url = (f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(m.title)}"
               if m.media_type == 'series' else f"{WEBSITE_BASE_URL}/movie/{m.slug or m.id}")
        btns.append([{"text": f"📥 {m.title[:30]}", "url": url}])
    send('\n'.join(lines), buttons=btns)
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════
# NEW THIS WEEK + TRENDING + HF HEALTH + SEO + PLANNER + ANALYTICS
# ══════════════════════════════════════════════════════════════

@app.route('/new-this-week')
def new_this_week():
    week_ago   = datetime.utcnow() - timedelta(days=7)
    new_movies = Movie.query.filter(Movie.created_at >= week_ago,
                 Movie.media_type == 'movie').order_by(Movie.created_at.desc()).all()
    raw_series = Movie.query.filter(Movie.created_at >= week_ago,
                 Movie.media_type == 'series').order_by(Movie.created_at.desc()).all()
    seen, new_series = set(), []
    for ep in raw_series:
        if ep.title not in seen: seen.add(ep.title); new_series.append(ep)
    total = len(new_movies) + len(new_series)
    return render_template('new_this_week.html',
        new_movies=new_movies, new_series=new_series, total=total,
        week_ago=week_ago, categories=get_categories_list(),
        seo_title=f"New Subtitles This Week ({total} titles) | MalSubs",
        seo_desc=f"{total} new subtitles added this week.")

@app.route('/api/trending_week')
def trending_week():
    week_ago = datetime.utcnow() - timedelta(days=7)
    rows = (db.session.query(DownloadLog.movie_id,
                             func.count(DownloadLog.id).label('cnt'))
            .filter(DownloadLog.downloaded_at >= week_ago)
            .group_by(DownloadLog.movie_id)
            .order_by(func.count(DownloadLog.id).desc()).limit(12).all())
    result, seen = [], set()
    for row in rows:
        m = Movie.query.get(row.movie_id)
        if not m: continue
        k = m.title if m.media_type == 'series' else m.id
        if k in seen: continue
        seen.add(k)
        result.append({'id': m.id, 'title': m.title, 'year': m.year, 'rating': m.rating,
            'poster': m.poster_url, 'slug': m.slug, 'type': m.media_type, 'downloads': row.cnt,
            'url': (f"/series/{urllib.parse.quote(m.title)}" if m.media_type == 'series'
                    else f"/movie/{m.slug or m.id}")})
    return jsonify({'movies': result})

@app.route('/api/hf_health')
@login_required
def hf_health_api():
    return jsonify(check_hf_health(force=request.args.get('force', '0') == '1'))

@app.route('/api/generate_seo_desc/<int:movie_id>', methods=['POST'])
@login_required
def api_generate_seo_desc(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    desc  = generate_seo_description(movie)
    if not movie.plot or len(movie.plot) < 30:
        movie.plot = desc; db.session.commit()
    return jsonify({'ok': True, 'description': desc})

@app.route('/admin/search_analytics')
@login_required
def search_analytics():
    top_queries = (db.session.query(SearchLog.search_query,
        func.count(SearchLog.id).label('cnt'),
        func.avg(SearchLog.results_count).label('avg_results'))
        .group_by(SearchLog.search_query).order_by(func.count(SearchLog.id).desc()).limit(50).all())
    zero_results = (db.session.query(SearchLog.search_query,
        func.count(SearchLog.id).label('cnt'))
        .filter(SearchLog.results_count == 0).group_by(SearchLog.search_query)
        .order_by(func.count(SearchLog.id).desc()).limit(30).all())
    try:
        daily_volume = db.session.execute(db.text(
            "SELECT DATE(searched_at) as day, COUNT(*) as cnt FROM search_log "
            "WHERE searched_at >= :cutoff GROUP BY day ORDER BY day ASC"),
            {"cutoff": datetime.utcnow() - timedelta(days=14)}).fetchall()
    except Exception:
        daily_volume = []
    return render_template('admin_search_analytics.html',
        top_queries=top_queries, zero_results=zero_results,
        daily_volume=daily_volume,
        total_searches=SearchLog.query.count(),
        total_zero=SearchLog.query.filter_by(results_count=0).count(),
        categories=get_categories_list())

@app.route('/admin/upload_planner')
@login_required
def upload_planner():
    from datetime import date
    plans = UploadPlan.query.order_by(
        UploadPlan.scheduled_date.asc().nullslast(), UploadPlan.priority.asc()).all()
    today = date.today()
    return render_template('admin_upload_planner.html',
        plans=plans, today=today,
        overdue=[p for p in plans if p.scheduled_date and p.scheduled_date < today and p.status == 'Planned'],
        upcoming=[p for p in plans if p.status in ('Planned', 'InProgress')],
        done=[p for p in plans if p.status == 'Done'],
        categories=get_categories_list())

@app.route('/admin/upload_plan/add', methods=['POST'])
@login_required
def upload_plan_add():
    from datetime import date as dt
    raw = request.form.get('scheduled_date')
    db.session.add(UploadPlan(
        title=request.form.get('title', '').strip()[:200],
        imdb_id=request.form.get('imdb_id', '').strip(),
        media_type=request.form.get('media_type', 'movie'),
        scheduled_date=dt.fromisoformat(raw) if raw else None,
        notes=request.form.get('notes', '').strip()[:500],
        poster_url=request.form.get('poster_url', '').strip(),
        priority=int(request.form.get('priority', 2)),
        status='Planned'))
    db.session.commit()
    return redirect(url_for('upload_planner'))

@app.route('/admin/upload_plan/update/<int:pid>', methods=['POST'])
@login_required
def upload_plan_update(pid):
    p = UploadPlan.query.get_or_404(pid)
    p.status = request.form.get('status', p.status)
    p.notes  = request.form.get('notes', p.notes)
    db.session.commit()
    return jsonify({'ok': True})

@app.route('/admin/upload_plan/delete/<int:pid>', methods=['POST'])
@login_required
def upload_plan_delete(pid):
    db.session.delete(UploadPlan.query.get_or_404(pid))
    db.session.commit()
    return redirect(url_for('upload_planner'))

@app.route('/api/trigger_telegram/<int:movie_id>', methods=['GET', 'POST'])
def trigger_telegram(movie_id):
    if request.args.get('secret') != TELEGRAM_SECRET:
        return "Unauthorized", 401

    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    CHANNEL_ID     = os.environ.get('TELEGRAM_CHANNEL_ID')
    movie          = Movie.query.get_or_404(movie_id)

    if "SilentMode" in (movie.category or ''):
        return "Silent Mode Active - No Post", 200
    if not TELEGRAM_TOKEN or not CHANNEL_ID:
        return "Missing Telegram Secrets", 400

    clean_category = (movie.category or '').replace(", SilentMode", "").replace("SilentMode", "")
    tags           = " ".join([f"#{t.strip().replace(' ', '_')}"
                               for t in clean_category.split(',') if t.strip()]) or "#General"
    footer         = ("\n\n━━━━━━━━━━━━━━━━━━━━\n"
                      "📢 *Join Channel:* @malayalam\\_sub1\n"
                      "💬 *Request Subtitles:* @SubmanagerRobot")

    safe_title   = movie.title or "Unknown Title"
    safe_rating  = movie.rating or "N/A"
    runtime_text = f"⏱ *Runtime:* {movie.runtime}\n" if movie.runtime else ""
    safe_plot    = ""
    if movie.plot:
        cp = movie.plot.replace("*", "").replace("_", "").replace("`", "")
        safe_plot = f"📖 *Plot:* {cp[:250] + '...' if len(cp) > 250 else cp}\n\n"

    if movie.media_type == 'series':
        caption    = (f"📺 *{safe_title}* - New Episode!\n\n"
                      f"🔢 *Season {movie.season or 1} - Episode {movie.episode or 1}*\n"
                      f"⭐️ *Rating:* {safe_rating} / 10\n"
                      f"{runtime_text}🎭 *Category:* {tags}\n\n"
                      f"{safe_plot}"
                      f"✅ *Subtitles Ready:* Malayalam, Tamil, Hindi\n"
                      f"⚡️ *High-Speed Download*\n\n"
                      f"👇 *Get the episode here:*{footer}")
        button_url = f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(safe_title)}/{movie.season or 1}"
    else:
        caption    = (f"🎬 *{safe_title}*\n\n"
                      f"⭐️ *Rating:* {safe_rating} / 10\n"
                      f"{runtime_text}🎭 *Category:* {tags}\n\n"
                      f"{safe_plot}"
                      f"✅ *Subtitles Ready:* Malayalam, Tamil, Hindi\n"
                      f"⚡️ *High-Speed Download*\n\n"
                      f"👇 *Get the movie here:*{footer}")
        slug       = movie.slug or str(movie.id)
        button_url = f"{WEBSITE_BASE_URL}/movie/{slug}"

    try:
        payload  = {
            "chat_id":      CHANNEL_ID,
            "photo":        movie.poster_url or "https://via.placeholder.com/500x750?text=No+Poster",
            "caption":      caption,
            "parse_mode":   "Markdown",
            "reply_markup": {"inline_keyboard": [[{"text": "📥 Download Subtitles", "url": button_url}]]}
        }
        response = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", json=payload)
        if response.status_code == 200:
            return "Posted to Telegram Successfully!", 200
        return f"Telegram API Error: {response.text}", 500
    except Exception as e:
        return str(e), 500

@app.route('/sitemap.xml')
def sitemap():
    xml = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
           f'<url><loc>{WEBSITE_BASE_URL}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>',
           f'<url><loc>{WEBSITE_BASE_URL}/requests</loc><changefreq>daily</changefreq><priority>0.7</priority></url>',
           f'<url><loc>{WEBSITE_BASE_URL}/favorites</loc><changefreq>weekly</changefreq><priority>0.5</priority></url>']

    seen_series = set()
    for media in Movie.query.order_by(Movie.id.desc()).all():
        if media.media_type == 'series':
            key = (media.title, media.season)
            if key in seen_series: continue
            seen_series.add(key)
            url = f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(media.title)}/{media.season or 1}"
        else:
            slug = media.slug or str(media.id)
            url  = f"{WEBSITE_BASE_URL}/movie/{slug}"
        xml.append(f'<url><loc>{url}</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>')

    xml.append('</urlset>')
    return app.response_class('\n'.join(xml), mimetype='application/xml')

@app.route('/api/request_sub', methods=['POST'])
def request_sub():
    return submit_request()

@app.route('/api/scheduled_fetch', methods=['POST', 'GET'])
def scheduled_fetch():
    secret = request.args.get('secret') or request.headers.get('X-Auth-Secret')
    if secret != os.environ.get('SCHEDULER_SECRET', 'scheduler-secret'):
        return jsonify({"error": "unauthorized"}), 401

    TMDB_API_KEY  = os.environ.get('TMDB_API_KEY')
    SUBDL_API_KEY = os.environ.get('SUBDL_API_KEY')
    if not TMDB_API_KEY or not SUBDL_API_KEY:
        return jsonify({"error": "API keys missing"}), 500

    TMDB_GENRE_MAP = {
        28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
        99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
        27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance",
        878: "Science Fiction", 10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western"
    }

    def process_movie(tmdb_id, require_digital=False):
        try:
            ext_data = requests.get(
                f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}",
                timeout=5).json()
            imdb_id = ext_data.get('imdb_id')
            if not imdb_id: return False
        except Exception:
            return False

        if Movie.query.filter_by(imdb_id=imdb_id, media_type='movie').first():
            return False

        if require_digital:
            try:
                release_data = requests.get(
                    f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates?api_key={TMDB_API_KEY}",
                    timeout=5).json()
                if not any(rd.get('type') == 4
                           for country in release_data.get('results', [])
                           for rd in country.get('release_dates', [])):
                    return False
            except Exception:
                return False

        srt_text = None
        try:
            imdb_fmt   = imdb_id if imdb_id.startswith('tt') else f"tt{imdb_id}"
            subdl_url  = (f"https://api.subdl.com/api/v1/subtitles?api_key={SUBDL_API_KEY}"
                          f"&imdb_id={imdb_fmt}&type=movie&languages=EN")
            subdl_resp = requests.get(subdl_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if subdl_resp.status_code == 200:
                subdl_data = subdl_resp.json()
                if subdl_data.get('status') and subdl_data.get('subtitles'):
                    def score_sub(sub):
                        if sub.get('hearing_impaired'): return -1
                        return (2 if sub.get('format', '').lower() == 'srt' else 0,
                                int(sub.get('downloads', 0)), float(sub.get('rating', 0)))
                    scored = sorted(
                        ((score_sub(s), s) for s in subdl_data['subtitles'] if score_sub(s) != -1),
                        key=lambda x: x[0], reverse=True)
                    if scored:
                        dl_url  = "https://dl.subdl.com" + scored[0][1]['url']
                        dl_resp = requests.get(dl_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                        if dl_resp.status_code == 200:
                            raw = dl_resp.content
                            if raw[:4] == b'PK\x03\x04':
                                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                                    for name in zf.namelist():
                                        if name.lower().endswith('.srt'):
                                            srt_text = zf.read(name).decode('utf-8', errors='ignore'); break
                            else:
                                srt_text = raw.decode('utf-8', errors='ignore')
        except Exception:
            pass

        if not srt_text or not srt_text.strip():
            return False

        r2_url = None
        try:
            safe_id     = imdb_id.replace('tt', '')
            r2_filename = f"english_movie_{safe_id}_{os.urandom(4).hex()}.srt"
            if s3_client and r2_bucket:
                s3_client.put_object(Bucket=r2_bucket, Key=r2_filename,
                                     Body=srt_text.encode('utf-8'),
                                     ContentType='application/x-subrip')
                r2_url = f"{r2_public_url.rstrip('/')}/{r2_filename}"
        except Exception:
            return False

        try:
            details  = requests.get(
                f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US").json()
            title    = details.get('title', 'Unknown')
            year     = details.get('release_date', '')[:4] if details.get('release_date') else ''
            rating   = str(details.get('vote_average', 'N/A'))
            poster   = (f"https://image.tmdb.org/t/p/w300{details['poster_path']}"
                        if details.get('poster_path') else 'https://via.placeholder.com/500x750?text=No+Poster')
            plot     = details.get('overview', '')
            runtime  = f"{details.get('runtime', '')} min" if details.get('runtime') else ''
            category = ", ".join([TMDB_GENRE_MAP.get(g['id'], '') for g in details.get('genres', [])
                                  if TMDB_GENRE_MAP.get(g['id'])]) or 'General'
        except Exception:
            title = 'Unknown'; year = ''; rating = 'N/A'
            poster = 'https://via.placeholder.com/500x750?text=No+Poster'
            plot = ''; runtime = ''; category = 'General'

        slug      = _unique_slug(title, year)
        new_movie = Movie(
            media_type='movie', title=title, year=year, rating=rating,
            poster_url=poster, english_srt=r2_url if r2_url else srt_text,
            category=category, plot=plot, runtime=runtime, imdb_id=imdb_id, slug=slug
        )
        db.session.add(new_movie)
        db.session.commit()

        if r2_url:
            threading.Thread(target=trigger_hf_translation, args=(new_movie.id, r2_url)).start()

        # Broadcast to bot users
        threading.Thread(target=broadcast_new_subtitle, args=(new_movie,), daemon=True).start()

        return True

    candidate_ids = set()
    for endpoint in [
        (f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}"
         f"&language=en-US&sort_by=release_date.desc&with_release_type=4&page=1"),
        f"https://api.themoviedb.org/3/movie/now_playing?api_key={TMDB_API_KEY}&language=en-US&page=1"
    ]:
        try:
            for m in requests.get(endpoint, timeout=10).json().get('results', [])[:30]:
                candidate_ids.add(m['id'])
        except Exception:
            pass

    candidate_list = list(candidate_ids)
    random.shuffle(candidate_list)
    new_movies = 0
    for tmdb_id in candidate_list:
        if new_movies >= 5: break
        if process_movie(tmdb_id, require_digital=True):
            new_movies += 1

    if new_movies < 5:
        fallback_ids = set()
        for endpoint, pages in [('popular', 3), ('top_rated', 3)]:
            for pg in range(1, pages + 1):
                try:
                    url = f"https://api.themoviedb.org/3/movie/{endpoint}?api_key={TMDB_API_KEY}&language=en-US&page={pg}"
                    for m in requests.get(url, timeout=10).json().get('results', [])[:30]:
                        fallback_ids.add(m['id'])
                except Exception:
                    pass
        fallback_list = list(fallback_ids)
        random.shuffle(fallback_list)
        for tmdb_id in fallback_list:
            if new_movies >= 5: break
            if process_movie(tmdb_id, require_digital=False):
                new_movies += 1

    log = SchedulerLog(result='success' if new_movies > 0 else 'empty',
                       message=f"Added {new_movies} movies")
    db.session.add(log)
    db.session.commit()
    return jsonify({"message": f"Added {new_movies} new movies (5 max)"}), 200

# ------------------ AUTO RETRY FAILED JOBS ------------------

def auto_retry_failed():
    while True:
        time.sleep(1800)
        with app.app_context():
            failed = TranslationJob.query.filter_by(status='Failed').all()
            if failed:
                for job in failed:
                    movie = Movie.query.get(job.movie_id)
                    if movie and movie.english_srt:
                        job.status   = 'Pending'
                        job.progress = 0
                        job.priority = 1 if movie.media_type == 'movie' else 2
                db.session.commit()
                print(f"🔁 Re-queued {len(failed)} failed jobs — dispatching…")
                dispatch_pending_jobs()

threading.Thread(target=auto_retry_failed, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
