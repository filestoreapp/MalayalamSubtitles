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
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session, Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_, cast, Float
from sqlalchemy.orm import joinedload

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
    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language = db.Column(db.String(10))
    translated_srt = db.Column(db.Text)
    downloads = db.Column(db.Integer, default=0)
    movie = db.relationship('Movie', backref=db.backref('translations', cascade='all, delete-orphan'))

class TranslationJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language = db.Column(db.String(10))
    status = db.Column(db.String(20), default='Pending')
    progress = db.Column(db.Integer, default=0)
    movie = db.relationship('Movie', backref=db.backref('jobs', cascade='all, delete-orphan'))

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

# ------------------ NEW MODELS ------------------

class SubtitleRating(db.Model):
    """User ratings and comments on subtitle quality per language."""
    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language = db.Column(db.String(10))
    rating = db.Column(db.Integer)           # 1–5 stars
    comment = db.Column(db.Text, nullable=True)
    session_id = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, server_default=func.now())
    movie = db.relationship('Movie', backref=db.backref('sub_ratings', cascade='all, delete-orphan'))

class Favorite(db.Model):
    """Session-based watchlist / favorites."""
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(100), index=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    created_at = db.Column(db.DateTime, server_default=func.now())
    movie = db.relationship('Movie', backref=db.backref('favorites', cascade='all, delete-orphan'))

class SubtitleRequest(db.Model):
    """Community subtitle requests with vote counts."""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200))
    details = db.Column(db.Text)
    votes = db.Column(db.Integer, default=1)
    status = db.Column(db.String(20), default='Pending')  # Pending | InProgress | Done
    created_at = db.Column(db.DateTime, server_default=func.now())

class RequestVote(db.Model):
    """One vote per session per request (prevents duplicates)."""
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.Integer, db.ForeignKey('subtitle_request.id', ondelete='CASCADE'))
    session_id = db.Column(db.String(100))

with app.app_context():
    db.create_all()
    if not SiteStat.query.first():
        db.session.add(SiteStat(total_visitors=0))
        db.session.commit()

# ------------------ SESSION ID HELPER ------------------
def get_session_id():
    """Return a persistent UUID for the current visitor (stored in Flask session)."""
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
    return session['user_id']

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
    """
    Returns a dict of Open Graph / meta-tag data for a movie or series episode.
    Inject into template context as `seo` and render in <head>:

        <title>{{ seo.title }}</title>
        <meta name="description" content="{{ seo.description }}">
        <meta property="og:title" content="{{ seo.og_title }}">
        <meta property="og:description" content="{{ seo.og_description }}">
        <meta property="og:image" content="{{ seo.og_image }}">
        <meta property="og:url" content="{{ seo.og_url }}">
        <meta property="og:type" content="{{ seo.og_type }}">
        <meta name="twitter:card" content="summary_large_image">
        <meta name="twitter:title" content="{{ seo.og_title }}">
        <meta name="twitter:image" content="{{ seo.og_image }}">
        <link rel="canonical" href="{{ seo.canonical }}">
    """
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
HF_WORKER_URL = os.environ.get('HF_WORKER_URL', '')
HF_SECRET = os.environ.get('HF_SECRET', 'shared-secret')
TELEGRAM_SECRET = os.environ.get('TELEGRAM_SECRET', '')

def trigger_hf_translation(movie_id: int, english_srt_url: str):
    if not HF_WORKER_URL:
        print("⚠️ HF_WORKER_URL not set")
        return

    languages = ['ml', 'ta', 'hi']
    with app.app_context():
        for lang in languages:
            job = TranslationJob.query.filter_by(movie_id=movie_id, language=lang).first()
            if not job:
                job = TranslationJob(movie_id=movie_id, language=lang, status='Pending', progress=0)
                db.session.add(job)
                db.session.commit()

            try:
                resp = requests.post(HF_WORKER_URL, json={
                    "movie_id": movie_id,
                    "english_srt_url": english_srt_url,
                    "language": lang
                }, timeout=10)
                if resp.status_code == 200:
                    job.status = 'Processing'
                    job.progress = 0
                    db.session.commit()
                    print(f"✅ {lang.upper()} job accepted for movie {movie_id}")
                else:
                    job.status = 'Failed'
                    db.session.commit()
                    print(f"❌ {lang.upper()} trigger failed: {resp.status_code}")
            except Exception as e:
                print(f"❌ {lang.upper()} trigger error: {e}")
                job.status = 'Failed'
                db.session.commit()

# ------------------ CALLBACK ENDPOINT ------------------
@app.route('/api/translation_callback', methods=['POST'])
def translation_callback():
    data = request.json
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

    if status == 'Completed' and movie_id:
        all_jobs = TranslationJob.query.filter_by(movie_id=movie_id).all()
        all_done = all(j.status == 'Completed' for j in all_jobs if j.language in ['ml', 'ta', 'hi'])
        if all_done:
            try:
                requests.get(
                    url_for('trigger_telegram', movie_id=movie_id, _external=True),
                    params={'secret': TELEGRAM_SECRET},
                    timeout=5
                )
            except Exception as e:
                print(f"⚠️ Auto Telegram post failed: {e}")

    return jsonify({"ok": True})

# ------------------ AUTH ------------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def log_admin_action(action, details):
    log = AdminLog(action=action, details=details, ip_address=request.remote_addr)
    db.session.add(log)
    db.session.commit()

# ===========================================================================
#  USER ROUTES
# ===========================================================================

@app.route('/')
def index():
    stat = SiteStat.query.first()
    stat.total_visitors += 1
    db.session.commit()

    search_query   = request.args.get('q', '')
    category_query = request.args.get('cat', '')
    page           = request.args.get('page', 1, type=int)
    categories_list = get_categories_list()

    if search_query:
        pagination = Movie.query.filter(
            (Movie.title.ilike(f'%{search_query}%')) |
            (Movie.category.ilike(f'%{search_query}%'))
        ).order_by(Movie.id.desc()).paginate(page=page, per_page=12, error_out=False)
        return render_template('index.html',
                               pagination=pagination,
                               search_query=search_query,
                               categories=categories_list,
                               mode="search")

    if category_query:
        pagination = Movie.query.filter(
            Movie.category.ilike(f'%{category_query}%')
        ).order_by(Movie.id.desc()).paginate(page=page, per_page=12, error_out=False)
        return render_template('index.html',
                               pagination=pagination,
                               search_query=category_query,
                               categories=categories_list,
                               mode="search")

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

    # Top subtitle requests for homepage widget
    top_requests = SubtitleRequest.query\
        .filter_by(status='Pending')\
        .order_by(SubtitleRequest.votes.desc())\
        .limit(5).all()

    return render_template('index.html',
                           mode="home",
                           categories=categories_list,
                           trending_movies=trending_movies,
                           trending_series=trending_series,
                           popular_movies=popular_movies,
                           top_rated_movies=top_rated_movies,
                           top_rated_series=top_rated_series,
                           recent_uploads=recent_uploads,
                           top_requests=top_requests,
                           search_query='',
                           pagination=None)

@app.route('/robots.txt')
def robots_txt():
    rules = "User-agent: *\nDisallow: /admin\nDisallow: /login\nDisallow: /delete/\nAllow: /\n"
    return rules, 200, {'Content-Type': 'text/plain'}

@app.route('/keep-alive')
def keep_alive():
    return "Server is awake!", 200

# ---------------------------------------------------------------------------
#  MOVIE ROUTES (slug-based)
# ---------------------------------------------------------------------------

@app.route('/movie/<int:movie_id>')
def old_movie_redirect(movie_id):
    """Backward-compat redirect: /movie/<id>  →  /movie/<slug>"""
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

    # Track recently viewed (session-based, last 20 unique IDs)
    sid = get_session_id()
    rv  = session.get('recently_viewed', [])
    if movie.id in rv:
        rv.remove(movie.id)
    rv.insert(0, movie.id)
    session['recently_viewed'] = rv[:20]

    ready_languages = [c.language for c in movie.translations]
    primary_genre   = movie.category.split(',')[0].strip() if movie.category else 'General'
    related_movies  = Movie.query.filter(
        Movie.category.ilike(f'%{primary_genre}%'),
        Movie.id != movie.id
    ).limit(4).all()

    # Subtitle ratings per available language
    lang_ratings = {}
    for lang in ['en'] + ready_languages:
        rows = SubtitleRating.query.filter_by(movie_id=movie.id, language=lang).all()
        if rows:
            avg = round(sum(r.rating for r in rows) / len(rows), 1)
            lang_ratings[lang] = {'avg': avg, 'count': len(rows), 'comments': rows[-5:]}

    # Has user already favorited this?
    is_favorite = Favorite.query.filter_by(session_id=sid, movie_id=movie.id).first() is not None

    seo = build_seo_meta(movie)

    return render_template('movie.html',
                           movie=movie,
                           ready_languages=ready_languages,
                           related_movies=related_movies,
                           lang_ratings=lang_ratings,
                           is_favorite=is_favorite,
                           seo=seo)

# ---------------------------------------------------------------------------
#  SERIES ROUTES
# ---------------------------------------------------------------------------

@app.route('/series/<string:title>')
def series_overview(title):
    # joinedload prevents N+1 lazy-load queries for translations (was causing timeout/500)
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
    # Build sorted list of (season_num, episodes) so Jinja2 doesn't need to sort
    sorted_seasons = sorted(seasons.items())
    try:
        seo = build_seo_meta(show_data)
    except Exception:
        seo = None
    return render_template('series_overview.html',
                           title=title,
                           seasons=seasons,
                           sorted_seasons=sorted_seasons,
                           show_data=show_data,
                           seo=seo,
                           categories=get_categories_list())

@app.route('/series/<string:title>/<int:season>')
def series_page(title, season):
    # joinedload prevents N+1 lazy-load queries for each episode's translations
    eps = Movie.query\
        .filter_by(media_type='series', title=title, season=season)\
        .options(joinedload(Movie.translations))\
        .order_by(Movie.episode.asc())\
        .all()
    if not eps:
        return "Season not found", 404
    show_data    = eps[0]
    # All seasons list for navigation
    all_seasons  = db.session.query(Movie.season)\
        .filter_by(media_type='series', title=title)\
        .distinct().order_by(Movie.season.asc()).all()
    all_seasons  = [r[0] for r in all_seasons if r[0]]
    try:
        seo = build_seo_meta(show_data)
    except Exception:
        seo = None
    return render_template('series.html',
                           title=title,
                           season=season,
                           episodes=eps,
                           all_seasons=all_seasons,
                           show_data=show_data,
                           seo=seo,
                           categories=get_categories_list())

# ---------------------------------------------------------------------------
#  DOWNLOAD
# ---------------------------------------------------------------------------

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

    lang_map   = {'en': 'English', 'ml': 'Malayalam', 'ta': 'Tamil', 'hi': 'Hindi'}
    full_lang  = lang_map.get(language, language.upper())
    safe_title = re.sub(r'[^\w\s-]', '', movie.title)
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

# ---------------------------------------------------------------------------
#  ADVANCED SEARCH
# ---------------------------------------------------------------------------

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
    if imdb:
        if hasattr(Movie, 'imdb_id'):
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

    return render_template('search.html',
                           pagination=pagination,
                           categories=get_categories_list(),
                           current_filters={
                               'q': q, 'type': media_type,
                               'year_from': year_from, 'year_to': year_to,
                               'rating_min': rating_min, 'rating_max': rating_max,
                               'category': category, 'lang': lang,
                               'imdb': imdb, 'sort': sort
                           })

# ===========================================================================
#  FAVORITES / WATCHLIST
# ===========================================================================

@app.route('/favorites')
def favorites_page():
    sid      = get_session_id()
    favs     = Favorite.query.filter_by(session_id=sid)\
                             .order_by(Favorite.created_at.desc()).all()
    movies   = [f.movie for f in favs if f.movie]
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
    """Return list of movie IDs favorited by current session (for UI highlighting)."""
    sid  = get_session_id()
    ids  = [f.movie_id for f in Favorite.query.filter_by(session_id=sid).all()]
    return jsonify({"favorites": ids})

# ===========================================================================
#  RECENTLY VIEWED
# ===========================================================================

@app.route('/api/recently_viewed')
def recently_viewed():
    """Return enriched data for up-to-10 recently viewed movies."""
    ids = session.get('recently_viewed', [])[:10]
    if not ids:
        return jsonify({"movies": []})
    movies = {m.id: m for m in Movie.query.filter(Movie.id.in_(ids)).all()}
    result = []
    for mid in ids:
        m = movies.get(mid)
        if m:
            result.append({
                "id":        m.id,
                "title":     m.title,
                "year":      m.year,
                "rating":    m.rating,
                "poster":    m.poster_url,
                "slug":      m.slug,
                "type":      m.media_type
            })
    return jsonify({"movies": result})

# ===========================================================================
#  SUBTITLE RATINGS & COMMENTS
# ===========================================================================

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

    # One rating per session per movie+language
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

# ===========================================================================
#  SUBTITLE REQUESTS WITH VOTING
# ===========================================================================

@app.route('/requests')
def subtitle_requests_page():
    sort = request.args.get('sort', 'votes')
    if sort == 'newest':
        order = SubtitleRequest.created_at.desc()
    else:
        order = SubtitleRequest.votes.desc()
    page  = request.args.get('page', 1, type=int)
    pagination = SubtitleRequest.query.filter(
        SubtitleRequest.status != 'Done'
    ).order_by(order).paginate(page=page, per_page=20, error_out=False)

    sid   = get_session_id()
    voted = {v.request_id for v in RequestVote.query.filter_by(session_id=sid).all()}

    return render_template('requests.html',
                           pagination=pagination,
                           voted=voted,
                           sort=sort)

@app.route('/api/submit_request', methods=['POST'])
def submit_request():
    data    = request.json
    title   = (data.get('title') or '').strip()[:200]
    details = (data.get('details') or '').strip()[:1000]

    if not title:
        return jsonify({"error": "Title is required"}), 400

    sid = get_session_id()

    # Check duplicate (same title, within 7 days)
    week_ago   = datetime.utcnow() - timedelta(days=7)
    duplicate  = SubtitleRequest.query.filter(
        SubtitleRequest.title.ilike(title),
        SubtitleRequest.created_at >= week_ago
    ).first()
    if duplicate:
        # Auto-upvote if same session hasn't voted
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

    # Also notify Telegram
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

# ===========================================================================
#  MOVIE INFO API (for recently viewed JS widget)
# ===========================================================================

@app.route('/api/movie_info/<int:movie_id>')
def movie_info(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    return jsonify({
        'id':        movie.id,
        'title':     movie.title,
        'year':      movie.year,
        'rating':    movie.rating,
        'poster_url': movie.poster_url,
        'slug':      movie.slug,
        'type':      movie.media_type
    })

# ===========================================================================
#  ADMIN & DASHBOARD
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

    scheduler_logs    = SchedulerLog.query.order_by(SchedulerLog.id.desc()).limit(5).all()
    pending_requests  = SubtitleRequest.query.filter_by(status='Pending').count()
    total_ratings     = SubtitleRating.query.count()

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
                           total_ratings=total_ratings)

# ------------------ ANALYTICS DATA (for Chart.js) ------------------

@app.route('/api/analytics_data')
@login_required
def analytics_data():
    """
    Returns JSON used by the admin dashboard Chart.js charts.

    Charts:
      1. uploads_by_month   – bar chart, last 12 months
      2. downloads_by_lang  – pie chart
      3. views_top10        – horizontal bar, top 10 movies by views
      4. jobs_summary       – pie chart, job status breakdown
      5. requests_top10     – bar chart, top 10 subtitle requests by votes
    """

    # 1. Uploads per month (last 12 months)
    twelve_months_ago = datetime.utcnow() - timedelta(days=365)
    monthly_raw = db.session.execute(db.text("""
        SELECT TO_CHAR(created_at, 'YYYY-MM') AS month, COUNT(*) AS cnt
        FROM movie
        WHERE created_at >= :cutoff
        GROUP BY month ORDER BY month ASC
    """), {"cutoff": twelve_months_ago}).fetchall()
    uploads_by_month = {"labels": [r[0] for r in monthly_raw],
                        "data":   [int(r[1]) for r in monthly_raw]}

    # 2. Downloads by language
    lang_raw = db.session.query(
        TranslationCache.language,
        func.sum(TranslationCache.downloads)
    ).group_by(TranslationCache.language).all()
    lang_map = {'ml': 'Malayalam', 'ta': 'Tamil', 'hi': 'Hindi', 'en': 'English'}
    downloads_by_lang = {
        "labels": [lang_map.get(r[0], r[0]) for r in lang_raw],
        "data":   [int(r[1] or 0) for r in lang_raw]
    }

    # 3. Top 10 movies by views
    top_views = Movie.query.order_by(Movie.views.desc()).limit(10).all()
    views_top10 = {
        "labels": [m.title[:30] for m in top_views],
        "data":   [m.views for m in top_views]
    }

    # 4. Translation job status summary
    job_statuses = db.session.query(
        TranslationJob.status, func.count(TranslationJob.id)
    ).group_by(TranslationJob.status).all()
    jobs_summary = {
        "labels": [r[0] for r in job_statuses],
        "data":   [int(r[1]) for r in job_statuses]
    }

    # 5. Top 10 subtitle requests by votes
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

# ------------------ ADMIN: SUBTITLE REQUESTS MANAGEMENT ------------------

@app.route('/admin/requests')
@login_required
def admin_requests():
    page       = request.args.get('page', 1, type=int)
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

# ------------------ OTHER ADMIN ROUTES ------------------

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
        langs = ', '.join([c.language for c in m.translations])
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

        # Regenerate slug if title/year changed
        media.slug = _unique_slug(media.title, media.year, exclude_id=media.id)

        new_srt = request.files.get('new_srt')
        if new_srt and new_srt.filename:
            content      = new_srt.read().decode('utf-8', errors='ignore')
            storage_data = content
            if s3_client and r2_bucket:
                safe_title = media.title.replace(" ", "_").replace("/", "").lower()
                ep_tag     = f"_s{media.season}e{media.episode}" if media.media_type == 'series' else ""
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

# ------------------ TMDB PROXY ------------------

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

# ------------------ TMDB EPISODE VERIFICATION ------------------

def tmdb_episode_exists(imdb_id: str, season: int, episode: int) -> bool:
    api_key = os.environ.get('TMDB_API_KEY')
    if not api_key:
        return True
    try:
        find_data  = requests.get(
            f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={api_key}&external_source=imdb_id",
            timeout=10).json()
        tv_results = find_data.get('tv_results', [])
        if not tv_results:
            return True
        series_id = tv_results[0]['id']
        ep_resp   = requests.get(
            f"https://api.themoviedb.org/3/tv/{series_id}/season/{season}/episode/{episode}?api_key={api_key}",
            timeout=10)
        return ep_resp.status_code == 200
    except Exception:
        return True

# ------------------ SLUG HELPERS ------------------

def generate_slug(title, year):
    base = re.sub(r'[^\w\s-]', '', (title or '').lower().strip())
    base = re.sub(r'[-\s]+', '-', base)
    return f"{base}-{year}" if year else base

def _unique_slug(title, year, exclude_id=None):
    """Generate a slug guaranteed unique in the DB."""
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

# ------------------ AUTO FETCH SRT ------------------

@app.route('/api/auto_fetch_srt', methods=['POST'])
@login_required
def auto_fetch_srt():
    data       = request.json
    imdb_id    = data.get('imdb_id')
    media_type = data.get('media_type', 'movie')
    season     = data.get('season')
    episode    = data.get('episode')

    SUBDL_API_KEY = os.environ.get('SUBDL_API_KEY')
    OS_API_KEY    = os.environ.get('OS_API_KEY')

    if not imdb_id or imdb_id == 'undefined':
        return jsonify({"error": "Missing IMDb ID"}), 400
    if not str(imdb_id).startswith('tt'):
        imdb_id = f"tt{imdb_id}"
    clean_imdb = str(imdb_id).replace('tt', '')

    if media_type == 'series' and season and episode:
        if not tmdb_episode_exists(imdb_id, int(season), int(episode)):
            return jsonify({"error": f"Episode S{season}E{episode} does not exist (TMDB)"}), 400

    custom_headers = {"User-Agent": "Mozilla/5.0 Chrome/114.0.0.0 Safari/537.36"}
    error_log      = []

    # ----- SUBDL -----
    if SUBDL_API_KEY:
        try:
            url = (f"https://api.subdl.com/api/v1/subtitles?api_key={SUBDL_API_KEY}"
                   f"&imdb_id={imdb_id}&type={'tv' if media_type == 'series' else 'movie'}&languages=EN"
                   + (f"&season_number={season}&episode_number={episode}" if media_type == 'series' else ''))
            res_raw = requests.get(url, headers=custom_headers)
            if res_raw.status_code == 200:
                res  = res_raw.json()
                subs = res.get('subtitles', []) if res.get('status') else []
                if media_type == 'series' and season and episode and subs:
                    filtered = []
                    for sub in subs:
                        ss, se = sub.get('season'), sub.get('episode')
                        if ss is not None and se is not None:
                            if ss == season and se == episode:
                                filtered.append(sub)
                        else:
                            m = re.search(r'S(\d+)\s*E(\d+)', sub.get('release_name', ''), re.I)
                            if m and int(m.group(1)) == season and int(m.group(2)) == episode:
                                filtered.append(sub)
                    subs = filtered or []

                def score_sub(sub):
                    if sub.get('hearing_impaired'): return -1
                    return (2 if sub.get('format', '').lower() == 'srt' else 0,
                            int(sub.get('downloads', 0)),
                            float(sub.get('rating', 0)))

                scored = sorted(((score_sub(s), s) for s in subs if score_sub(s) != -1),
                                key=lambda x: x[0], reverse=True)
                if scored:
                    dl_url  = "https://dl.subdl.com" + scored[0][1]['url']
                    dl_res  = requests.get(dl_url, headers=custom_headers)
                    srt_text = ""
                    if b'PK\x03\x04' in dl_res.content[:4]:
                        with zipfile.ZipFile(io.BytesIO(dl_res.content)) as z:
                            for fn in z.namelist():
                                if fn.endswith('.srt'):
                                    srt_text = z.read(fn).decode('utf-8', errors='ignore'); break
                    else:
                        srt_text = dl_res.text
                    if srt_text:
                        return jsonify({"success": True, "srt_text": srt_text, "source": "Subdl"})
                else:
                    error_log.append("Subdl: No suitable subtitles")
            else:
                error_log.append(f"Subdl HTTP {res_raw.status_code}")
        except Exception as e:
            error_log.append(f"Subdl Crash: {e}")

    # ----- OPENSUBTITLES -----
    if OS_API_KEY:
        try:
            os_headers = {"Api-Key": OS_API_KEY, "Content-Type": "application/json",
                          "User-Agent": "malayalamsubtitles_app v1.0"}
            if media_type == 'series':
                url = (f"https://api.opensubtitles.com/api/v1/subtitles"
                       f"?parent_imdb_id={clean_imdb}&season_number={season}&episode_number={episode}&languages=en")
            else:
                url = f"https://api.opensubtitles.com/api/v1/subtitles?imdb_id={clean_imdb}&languages=en"

            search_res = requests.get(url, headers=os_headers)
            if search_res.status_code == 200:
                items = search_res.json().get('data', [])
                if media_type == 'series':
                    items = [i for i in items if
                             i.get('attributes', {}).get('season') == season and
                             i.get('attributes', {}).get('episode') == episode]
                if items:
                    file_id   = items[0]['attributes']['files'][0]['file_id']
                    dl_resp   = requests.post("https://api.opensubtitles.com/api/v1/download",
                                              headers=os_headers, json={"file_id": file_id})
                    if dl_resp.status_code == 200:
                        link = dl_resp.json().get('link')
                        if link:
                            os_dl    = requests.get(link, headers=custom_headers)
                            srt_text = ""
                            if b'PK\x03\x04' in os_dl.content[:4]:
                                with zipfile.ZipFile(io.BytesIO(os_dl.content)) as z:
                                    for fn in z.namelist():
                                        if fn.endswith('.srt'):
                                            srt_text = z.read(fn).decode('utf-8', errors='ignore'); break
                            else:
                                srt_text = os_dl.text
                            if srt_text:
                                return jsonify({"success": True, "srt_text": srt_text, "source": "OpenSubtitles"})
                        else:
                            error_log.append("OS: Download link blocked")
                    else:
                        error_log.append(f"OS Download HTTP {dl_resp.status_code}")
                else:
                    error_log.append("OS: No matching subtitles")
            else:
                error_log.append(f"OS Search HTTP {search_res.status_code}")
        except Exception as e:
            error_log.append(f"OS Crash: {e}")

    return jsonify({"error": " | ".join(error_log)}), 404

# ------------------ MASTER UPLOAD ROUTE ------------------

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

        items_to_process = []
        for i, text in enumerate(fetched_srts):
            if text.strip():
                ep = fetched_episodes[i] if i < len(fetched_episodes) else str(i + 1)
                items_to_process.append((text, ep))
        for i, srt_file in enumerate(valid_files):
            content = srt_file.read().decode('utf-8', errors='ignore')
            ep = valid_manual_eps[i] if i < len(valid_manual_eps) else str(i + 1)
            items_to_process.append((content, ep))

        try:
            safe_season = int(season_raw) if season_raw and str(season_raw).strip() else None
        except ValueError:
            safe_season = 1

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
                episode=current_ep, year=year,
                rating=rating, poster_url=poster_url,
                english_srt=storage_data,
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

        return redirect(url_for('dashboard'))

    return render_template('admin.html')

# ------------------ TELEGRAM TRIGGER ------------------

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
                      "💬 *Request Subtitles:* @Subrequest\\_bot")

    safe_title  = movie.title or "Unknown Title"
    safe_rating = movie.rating or "N/A"
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
        # ✅ FIX: use slug-based URL, fall back to ID if slug missing
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

# ------------------ SITEMAP ------------------

@app.route('/sitemap.xml')
def sitemap():
    xml  = ['<?xml version="1.0" encoding="UTF-8"?>',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
            f'<url><loc>{WEBSITE_BASE_URL}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>',
            f'<url><loc>{WEBSITE_BASE_URL}/requests</loc><changefreq>daily</changefreq><priority>0.7</priority></url>',
            f'<url><loc>{WEBSITE_BASE_URL}/favorites</loc><changefreq>weekly</changefreq><priority>0.5</priority></url>']

    seen_series = set()
    for media in Movie.query.order_by(Movie.id.desc()).all():
        if media.media_type == 'series':
            key = (media.title, media.season)
            if key in seen_series:
                continue
            seen_series.add(key)
            url = f"{WEBSITE_BASE_URL}/series/{urllib.parse.quote(media.title)}/{media.season or 1}"
        else:
            # ✅ FIX: slug-based URL
            slug = media.slug or str(media.id)
            url  = f"{WEBSITE_BASE_URL}/movie/{slug}"
        xml.append(f'<url><loc>{url}</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>')

    xml.append('</urlset>')
    return app.response_class('\n'.join(xml), mimetype='application/xml')

# ------------------ SUBTITLE REQUEST (old Telegram-only endpoint kept) ------------------

@app.route('/api/request_sub', methods=['POST'])
def request_sub():
    """Legacy endpoint kept for backward compat – now delegates to submit_request logic."""
    return submit_request()

# ------------------ SCHEDULER ------------------

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
            imdb_fmt  = imdb_id if imdb_id.startswith('tt') else f"tt{imdb_id}"
            subdl_url = (f"https://api.subdl.com/api/v1/subtitles?api_key={SUBDL_API_KEY}"
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

        slug = _unique_slug(title, year)
        new_movie = Movie(
            media_type='movie', title=title, year=year, rating=rating,
            poster_url=poster, english_srt=r2_url if r2_url else srt_text,
            category=category, plot=plot, runtime=runtime, imdb_id=imdb_id, slug=slug
        )
        db.session.add(new_movie)
        db.session.commit()

        if r2_url:
            threading.Thread(target=trigger_hf_translation, args=(new_movie.id, r2_url)).start()
        return True

    # Phase 1 – OTT
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

    # Phase 2 – Fallback
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
            for job in TranslationJob.query.filter_by(status='Failed').all():
                movie = Movie.query.get(job.movie_id)
                if movie and movie.english_srt:
                    job.status   = 'Pending'
                    job.progress = 0
                    db.session.commit()
                    threading.Thread(target=trigger_hf_translation,
                                     args=(movie.id, movie.english_srt)).start()

threading.Thread(target=auto_retry_failed, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
