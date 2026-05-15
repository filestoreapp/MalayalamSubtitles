import os
import io
import requests
import boto3
import urllib.parse
import re
import zipfile
import time
import threading
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_, cast, Float

app = Flask(__name__)

# ------------------ CONFIG ------------------
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True, 'pool_recycle': 300}
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')
db = SQLAlchemy(app)

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

with app.app_context():
    db.create_all()
    if not SiteStat.query.first():
        db.session.add(SiteStat(total_visitors=0))
        db.session.commit()

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

# ------------------ TRIGGER TRANSLATION ------------------
HF_WORKER_URL = os.environ.get('HF_WORKER_URL', '')
HF_SECRET = os.environ.get('HF_SECRET', 'shared-secret')
TELEGRAM_SECRET = os.environ.get('TELEGRAM_SECRET', '')

def trigger_hf_translation(movie_id: int, english_srt_url: str):
    """Fire translation requests for Malayalam, Tamil, Hindi."""
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
    status = data.get('status')
    progress = data.get('progress', 0)

    job = TranslationJob.query.filter_by(movie_id=movie_id, language=language).first()
    if job:
        job.status = status
        job.progress = progress
        db.session.commit()

    if status == 'Completed' and movie_id:
        all_jobs = TranslationJob.query.filter_by(movie_id=movie_id).all()
        all_done = all(j.status == 'Completed' for j in all_jobs if j.language in ['ml','ta','hi'])
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

# ------------------ USER ROUTES ------------------
@app.route('/')
def index():
    stat = SiteStat.query.first()
    stat.total_visitors += 1
    db.session.commit()

    search_query = request.args.get('q', '')
    category_query = request.args.get('cat', '')
    page = request.args.get('page', 1, type=int)
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
        pagination = Movie.query.filter(Movie.category.ilike(f'%{category_query}%')).order_by(Movie.id.desc()).paginate(page=page, per_page=12, error_out=False)
        return render_template('index.html',
                               pagination=pagination,
                               search_query=category_query,
                               categories=categories_list,
                               mode="search")

    # ---- Home page sections ----
    # Trending Movies
    trending_movies = Movie.query.filter_by(media_type='movie').order_by(Movie.views.desc()).limit(12).all()

    # Trending TV Shows (one card per unique title)
    trending_series_subq = db.session.query(
        Movie.title,
        func.max(Movie.views).label('max_views')
    ).filter_by(media_type='series').group_by(Movie.title).subquery()
    trending_series = Movie.query.join(
        trending_series_subq,
        db.and_(Movie.title == trending_series_subq.c.title,
                Movie.views == trending_series_subq.c.max_views)
    ).order_by(Movie.views.desc()).limit(12).all()

    # Popular Movies
    popular_movies = Movie.query.filter_by(media_type='movie').order_by(Movie.views.desc()).limit(12).all()

    # Top Rated Movies
    top_rated_movies = Movie.query.filter_by(media_type='movie')\
                           .order_by(cast(Movie.rating, Float).desc()).limit(12).all()

    # Top Rated TV Shows (one card per unique title)
    top_rated_series_subq = db.session.query(
        Movie.title,
        func.max(cast(Movie.rating, Float)).label('max_rating')
    ).filter_by(media_type='series').group_by(Movie.title).subquery()
    top_rated_series = Movie.query.join(
        top_rated_series_subq,
        db.and_(Movie.title == top_rated_series_subq.c.title,
                cast(Movie.rating, Float) == top_rated_series_subq.c.max_rating)
    ).order_by(cast(Movie.rating, Float).desc()).limit(12).all()

    # Recent Uploads (one card per unique title)
    recent_uploads_subq = db.session.query(
        Movie.title,
        func.max(Movie.id).label('max_id')
    ).group_by(Movie.title).subquery()
    recent_uploads = Movie.query.join(
        recent_uploads_subq,
        Movie.id == recent_uploads_subq.c.max_id
    ).order_by(Movie.id.desc()).limit(12).all()

    return render_template('index.html',
                           mode="home",
                           categories=categories_list,
                           trending_movies=trending_movies,
                           trending_series=trending_series,
                           popular_movies=popular_movies,
                           top_rated_movies=top_rated_movies,
                           top_rated_series=top_rated_series,
                           recent_uploads=recent_uploads,
                           search_query='',
                           pagination=None)

@app.route('/robots.txt')
def robots_txt():
    rules = "User-agent: *\nDisallow: /admin\nDisallow: /login\nDisallow: /delete/\nAllow: /\n"
    return rules, 200, {'Content-Type': 'text/plain'}

@app.route('/keep-alive')
def keep_alive():
    return "Server is awake!", 200

@app.route('/movie/<int:movie_id>')
def movie_hub(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    movie.views += 1
    db.session.commit()
    ready_languages = [c.language for c in movie.translations]
    primary_genre = movie.category.split(',')[0].strip() if movie.category else 'General'
    related_movies = Movie.query.filter(Movie.category.ilike(f'%{primary_genre}%'), Movie.id != movie.id).limit(4).all()
    return render_template('movie.html', movie=movie, ready_languages=ready_languages, related_movies=related_movies)

@app.route('/series/<string:title>')
def series_overview(title):
    episodes = Movie.query.filter_by(media_type='series', title=title)\
                         .order_by(Movie.season.asc(), Movie.episode.asc()).all()
    if not episodes:
        return "Series not found", 404
    show_data = episodes[0]
    seasons = {}
    for ep in episodes:
        s = ep.season or 1
        if s not in seasons:
            seasons[s] = []
        seasons[s].append(ep)
    return render_template('series_overview.html', title=title, show_data=show_data, seasons=seasons)

@app.route('/series/<string:title>/<int:season>')
def series_page(title, season):
    episodes = Movie.query.filter_by(media_type='series', title=title, season=season)\
                         .order_by(Movie.episode.asc()).all()
    if not episodes:
        return "Season not found", 404
    show_data = episodes[0]
    return render_template('series.html', title=title, season=season, episodes=episodes, show_data=show_data)

@app.route('/download/<int:movie_id>/<language>')
def download(movie_id, language):
    movie = Movie.query.get_or_404(movie_id)
    if language == 'en':
        srt_data = movie.english_srt
    else:
        cache = TranslationCache.query.filter_by(movie_id=movie_id, language=language).first_or_404()
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

    lang_map = {'en': 'English', 'ml': 'Malayalam', 'ta': 'Tamil', 'hi': 'Hindi'}
    full_lang = lang_map.get(language, language.upper())

    safe_title = re.sub(r'[^\w\s-]', '', movie.title)
    clean_title = re.sub(r'[-\s]+', '.', safe_title).strip('.')
    if movie.media_type == 'series':
        s = movie.season or 1
        e = movie.episode or 1
        final_name = f"{clean_title}.S{s:02d}E{e:02d}.{full_lang}.srt"
    else:
        year_str = f".{movie.year}" if movie.year else ""
        final_name = f"{clean_title}{year_str}.{full_lang}.srt"

    return send_file(mem_file, as_attachment=True, download_name=final_name, mimetype='application/x-subrip')

# ------------------ ADVANCED SEARCH ------------------
@app.route('/search')
def advanced_search():
    q = request.args.get('q', '').strip()
    media_type = request.args.get('type', '')
    year_from = request.args.get('year_from', type=int)
    year_to = request.args.get('year_to', type=int)
    rating_min = request.args.get('rating_min', type=float)
    rating_max = request.args.get('rating_max', type=float)
    category = request.args.get('category', '')
    lang = request.args.get('lang', '')
    imdb = request.args.get('imdb', '').strip()
    sort = request.args.get('sort', 'newest')
    page = request.args.get('page', 1, type=int)

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
        base_q = base_q.filter(cast(Movie.rating, Float) >= rating_min)
    if rating_max is not None:
        base_q = base_q.filter(cast(Movie.rating, Float) <= rating_max)
    if imdb:
        if hasattr(Movie, 'imdb_id'):
            base_q = base_q.filter(Movie.imdb_id.ilike(f'%{imdb}%'))
    if lang:
        if lang == 'en':
            base_q = base_q.filter(Movie.english_srt.isnot(None), Movie.english_srt != '')
        else:
            base_q = base_q.filter(Movie.translations.any(TranslationCache.language == lang))

    sort_mapping = {
        'newest': Movie.id.desc(),
        'oldest': Movie.id.asc(),
        'downloads': Movie.views.desc(),
        'rating_desc': cast(Movie.rating, Float).desc(),
        'rating_asc': cast(Movie.rating, Float).asc(),
        'title_asc': Movie.title.asc(),
        'title_desc': Movie.title.desc()
    }
    base_q = base_q.order_by(sort_mapping.get(sort, Movie.id.desc()))
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

# ------------------ ADMIN & DASHBOARD ------------------
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form['username'] == os.environ.get('ADMIN_USER', 'admin') and \
           request.form['password'] == os.environ.get('ADMIN_PASS', 'change-me'):
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Invalid credentials.')
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    TranslationJob.query.filter(TranslationJob.status.in_(['Completed', 'Success'])).delete(synchronize_session=False)
    db.session.commit()

    stat = SiteStat.query.first()
    total_dl = db.session.query(func.sum(TranslationCache.downloads)).scalar() or 0
    total_subs = TranslationCache.query.count()

    page = request.args.get('page', 1, type=int)
    all_media_paginated = Movie.query.order_by(Movie.id.desc()).paginate(page=page, per_page=15, error_out=False)
    pop_lang = db.session.query(TranslationCache.language, func.count(TranslationCache.id))\
                        .group_by(TranslationCache.language).order_by(func.count(TranslationCache.id).desc()).first()
    recent_jobs = TranslationJob.query.order_by(TranslationJob.id.desc()).limit(15).all()

    try:
        db_size_query = db.session.execute(db.text("SELECT pg_size_pretty(pg_database_size(current_database()))")).scalar()
        db_size = db_size_query if db_size_query else "Unknown"
    except:
        db_size = "Error reading DB"

    r2_size_str = "Not Connected"
    r2_file_count = 0
    if s3_client and r2_bucket:
        try:
            total_bytes = 0
            paginator = s3_client.get_paginator('list_objects_v2')
            for page_obj in paginator.paginate(Bucket=r2_bucket):
                if 'Contents' in page_obj:
                    for obj in page_obj['Contents']:
                        total_bytes += obj['Size']
                        r2_file_count += 1
            if total_bytes < 1024 * 1024:
                r2_size_str = f"{total_bytes / 1024:.2f} KB"
            elif total_bytes < 1024 * 1024 * 1024:
                r2_size_str = f"{total_bytes / (1024 * 1024):.2f} MB"
            else:
                r2_size_str = f"{total_bytes / (1024 * 1024 * 1024):.2f} GB"
        except:
            r2_size_str = "Read Error"

    return render_template('dashboard.html',
                           visitors=stat.total_visitors,
                           downloads=total_dl,
                           total_subtitles=total_subs,
                           popular_lang=pop_lang,
                           all_media=all_media_paginated,
                           jobs=recent_jobs,
                           db_size=db_size,
                           r2_size_str=r2_size_str,
                           r2_file_count=r2_file_count)

@app.route('/admin/reset_jobs')
@login_required
def reset_jobs():
    stuck_jobs = TranslationJob.query.filter_by(status='Processing').all()
    for job in stuck_jobs:
        job.status = 'Pending'
    db.session.commit()
    pending = TranslationJob.query.filter_by(status='Pending').all()
    for job in pending:
        movie = Movie.query.get(job.movie_id)
        if movie and movie.english_srt:
            threading.Thread(target=trigger_hf_translation, args=(movie.id, movie.english_srt)).start()
    return redirect(url_for('dashboard'))

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
    return redirect(url_for('dashboard'))

@app.route('/admin/edit/<int:movie_id>', methods=['GET', 'POST'])
@login_required
def edit_media(movie_id):
    media = Movie.query.get_or_404(movie_id)
    if request.method == 'POST':
        media.title = request.form.get('title')
        media.year = request.form.get('year')
        media.rating = request.form.get('rating')
        media.poster_url = request.form.get('poster_url')
        media.category = request.form.get('category')
        media.plot = request.form.get('plot')
        media.runtime = request.form.get('runtime')
        if media.media_type == 'series':
            media.season = request.form.get('season')
            media.episode = request.form.get('episode')

        new_srt = request.files.get('new_srt')
        if new_srt and new_srt.filename:
            content = new_srt.read().decode('utf-8', errors='ignore')
            storage_data = content
            if s3_client and r2_bucket:
                safe_title = media.title.replace(" ", "_").replace("/", "").lower()
                ep_tag = f"_s{media.season}e{media.episode}" if media.media_type == 'series' else ""
                r2_filename = f"english_{safe_title}{ep_tag}_edit_{os.urandom(4).hex()}.srt"
                try:
                    s3_client.put_object(
                        Bucket=r2_bucket, Key=r2_filename,
                        Body=content.encode('utf-8'), ContentType='application/x-subrip'
                    )
                    storage_data = f"{r2_public_url}/{r2_filename}"
                except Exception as e:
                    print(f"R2 Upload Failed on Edit: {e}")
            media.english_srt = storage_data
            TranslationCache.query.filter_by(movie_id=media.id).delete()
            TranslationJob.query.filter_by(movie_id=media.id).delete()
            db.session.commit()
            threading.Thread(target=trigger_hf_translation, args=(media.id, storage_data)).start()
        db.session.commit()
        return redirect(url_for('dashboard'))
    return render_template('edit.html', media=media)

# --- TMDB PROXY ---
@app.route('/api/tmdb_search')
@login_required
def tmdb_search():
    query = request.args.get('query')
    tmdb_type = request.args.get('type')
    api_key = os.environ.get('TMDB_API_KEY')
    url = f"https://api.themoviedb.org/3/search/{tmdb_type}?api_key={api_key}&query={urllib.parse.quote(query)}"
    try:
        return jsonify(requests.get(url).json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/tmdb_details')
@login_required
def tmdb_details():
    tmdb_id = request.args.get('id')
    tmdb_type = request.args.get('type')
    api_key = os.environ.get('TMDB_API_KEY')
    url = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}?api_key={api_key}&append_to_response=external_ids"
    try:
        return jsonify(requests.get(url).json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- DUAL-ENGINE AUTO FETCHER (with intelligent Subdl selection) ---
@app.route('/api/auto_fetch_srt', methods=['POST'])
@login_required
def auto_fetch_srt():
    data = request.json
    imdb_id = data.get('imdb_id')
    media_type = data.get('media_type', 'movie')
    season = data.get('season')
    episode = data.get('episode')

    SUBDL_API_KEY = os.environ.get('SUBDL_API_KEY')
    OS_API_KEY = os.environ.get('OS_API_KEY')

    if not imdb_id or imdb_id == 'undefined':
        return jsonify({"error": "Missing IMDb ID"}), 400

    if not str(imdb_id).startswith('tt'):
        imdb_id = f"tt{imdb_id}"
    clean_imdb = str(imdb_id).replace('tt', '')

    custom_headers = {"User-Agent": "Mozilla/5.0 ... Chrome/114.0.0.0 Safari/537.36"}
    error_log = []

    if SUBDL_API_KEY:
        try:
            if media_type == 'series':
                url = f"https://api.subdl.com/api/v1/subtitles?api_key={SUBDL_API_KEY}&imdb_id={imdb_id}&type=tv&season_number={season}&episode_number={episode}&languages=EN"
            else:
                url = f"https://api.subdl.com/api/v1/subtitles?api_key={SUBDL_API_KEY}&imdb_id={imdb_id}&type=movie&languages=EN"

            res_raw = requests.get(url, headers=custom_headers)
            if res_raw.status_code == 200:
                res = res_raw.json()
                if res.get('status') and res.get('subtitles'):
                    subs = res['subtitles']
                    def score_sub(sub):
                        if sub.get('hearing_impaired', False):
                            return -1
                        fmt_score = 2 if sub.get('format', '').lower() == 'srt' else 0
                        downloads = int(sub.get('downloads', 0))
                        rating = float(sub.get('rating', 0))
                        return (fmt_score, downloads, rating)

                    scored = []
                    for sub in subs:
                        s = score_sub(sub)
                        if s == -1:
                            continue
                        scored.append((s, sub))

                    if scored:
                        scored.sort(key=lambda x: x[0], reverse=True)
                        best_sub = scored[0][1]
                        dl_url = "https://dl.subdl.com" + best_sub['url']
                        dl_res = requests.get(dl_url, headers=custom_headers)
                        srt_text = ""
                        if dl_url.endswith('.zip') or b'PK\x03\x04' in dl_res.content[:4]:
                            with zipfile.ZipFile(io.BytesIO(dl_res.content)) as z:
                                for filename in z.namelist():
                                    if filename.endswith('.srt'):
                                        srt_text = z.read(filename).decode('utf-8', errors='ignore')
                                        break
                        else:
                            srt_text = dl_res.text
                        if srt_text:
                            return jsonify({"success": True, "srt_text": srt_text, "source": "Subdl"})
                    else:
                        error_log.append("Subdl: No suitable subtitles after filtering")
                else:
                    error_log.append("Subdl: No subtitles found")
            else:
                error_log.append(f"Subdl HTTP {res_raw.status_code}")
        except Exception as e:
            error_log.append(f"Subdl Crash: {str(e)}")

    if OS_API_KEY:
        try:
            os_headers = {"Api-Key": OS_API_KEY, "Content-Type": "application/json", "User-Agent": "malayalamsubtitles_app v1.0"}
            if media_type == 'series':
                url = f"https://api.opensubtitles.com/api/v1/subtitles?parent_imdb_id={clean_imdb}&season_number={season}&episode_number={episode}&languages=en"
            else:
                url = f"https://api.opensubtitles.com/api/v1/subtitles?imdb_id={clean_imdb}&languages=en"

            search_res_raw = requests.get(url, headers=os_headers)
            if search_res_raw.status_code == 200:
                search_res = search_res_raw.json()
                if search_res.get('data'):
                    file_id = search_res['data'][0]['attributes']['files'][0]['file_id']
                    dl_response_raw = requests.post("https://api.opensubtitles.com/api/v1/download", headers=os_headers, json={"file_id": file_id})
                    if dl_response_raw.status_code == 200:
                        dl_response = dl_response_raw.json()
                        link = dl_response.get('link')
                        if link:
                            os_dl = requests.get(link, headers=custom_headers)
                            srt_text = ""
                            if link.endswith('.zip') or b'PK\x03\x04' in os_dl.content[:4]:
                                with zipfile.ZipFile(io.BytesIO(os_dl.content)) as z:
                                    for filename in z.namelist():
                                        if filename.endswith('.srt'):
                                            srt_text = z.read(filename).decode('utf-8', errors='ignore')
                                            break
                            else:
                                srt_text = os_dl.text
                            if srt_text:
                                return jsonify({"success": True, "srt_text": srt_text, "source": "OpenSubtitles"})
                        else:
                            error_log.append("OS: Download blocked")
                    else:
                        error_log.append(f"OS Download HTTP {dl_response_raw.status_code}")
                else:
                    error_log.append("OS: No file found")
            else:
                error_log.append(f"OS Search HTTP {search_res_raw.status_code}")
        except Exception as e:
            error_log.append(f"OS Crash: {str(e)}")

    return jsonify({"error": f"{' | '.join(error_log)}"}), 404

# --- MASTER UPLOAD ROUTE ---
@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    if request.method == 'POST':
        media_type = request.form.get('media_type', 'movie')
        title = request.form.get('title', 'Unknown Title')
        season_raw = request.form.get('season', '')
        year = request.form.get('year', '')
        rating = request.form.get('rating', '0')
        poster_url = request.form.get('poster_url', '')
        silent_upload = request.form.get('silent_upload')
        plot = request.form.get('plot', '')
        runtime = request.form.get('runtime', '')

        categories = request.form.getlist('category')
        category_string = ", ".join(categories)
        if silent_upload == 'yes':
            category_string += ", SilentMode"

        fetched_srts = request.form.getlist('fetched_srts[]')
        fetched_episodes = request.form.getlist('fetched_episodes[]')

        files = request.files.getlist('files[]')
        manual_episodes = request.form.getlist('episodes[]')

        valid_files = [f for f in files if f and f.filename]
        valid_manual_eps = [ep for ep in manual_episodes if ep.strip()]

        items_to_process = []

        for i, text in enumerate(fetched_srts):
            if text.strip():
                ep = fetched_episodes[i] if i < len(fetched_episodes) else str(i+1)
                items_to_process.append((text, ep))

        for i, srt_file in enumerate(valid_files):
            content = srt_file.read().decode('utf-8', errors='ignore')
            ep = valid_manual_eps[i] if i < len(valid_manual_eps) else str(i+1)
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
                ep_tag = f"_s{safe_season}e{current_ep}" if media_type == 'series' else ""
                r2_filename = f"english_{safe_title_str}{ep_tag}_{os.urandom(4).hex()}.srt"
                try:
                    s3_client.put_object(
                        Bucket=r2_bucket, Key=r2_filename,
                        Body=safe_content.encode('utf-8'), ContentType='application/x-subrip'
                    )
                    storage_data = f"{r2_public_url}/{r2_filename}"
                except Exception as e:
                    print(f"R2 Upload Failed: {e}")

            new_media = Movie(
                media_type=media_type, title=title,
                season=safe_season if media_type == 'series' else None,
                episode=current_ep, year=year,
                rating=rating, poster_url=poster_url, english_srt=storage_data,
                category=category_string, plot=plot, runtime=runtime
            )
            db.session.add(new_media)

            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                return f"Database Error during Movie Insert: {str(e)}", 500

            threading.Thread(target=trigger_hf_translation, args=(new_media.id, storage_data)).start()

        return redirect(url_for('dashboard'))

    return render_template('admin.html')

# --- TELEGRAM TRIGGER ---
@app.route('/api/trigger_telegram/<int:movie_id>', methods=['GET', 'POST'])
def trigger_telegram(movie_id):
    if request.args.get('secret') != TELEGRAM_SECRET:
        return "Unauthorized", 401

    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID')
    movie = Movie.query.get_or_404(movie_id)

    if "SilentMode" in (movie.category or ''):
        return "Silent Mode Active - No Post", 200

    if not TELEGRAM_TOKEN or not CHANNEL_ID:
        return "Missing Telegram Secrets on Render", 400

    clean_category = (movie.category or '').replace(", SilentMode", "").replace("SilentMode", "")
    tags = " ".join([f"#{t.strip().replace(' ', '_')}" for t in clean_category.split(',') if t.strip()]) if clean_category.strip() else "#General"

    website_base_url = "https://malayalamsubtitles.onrender.com"
    footer = f"\n\n━━━━━━━━━━━━━━━━━━━━\n📢 *Join Channel:* @malayalam\\_sub1\n💬 *Request Subtitles:* @Subrequest\\_bot"

    safe_title = movie.title or "Unknown Title"
    safe_rating = movie.rating or "N/A"
    runtime_text = f"⏱ *Runtime:* {movie.runtime}\n" if movie.runtime else ""

    safe_plot = ""
    if movie.plot:
        clean_plot = movie.plot.replace("*", "").replace("_", "").replace("`", "")
        safe_plot = clean_plot[:250] + "..." if len(clean_plot) > 250 else clean_plot
        safe_plot = f"📖 *Plot:* {safe_plot}\n\n"

    if movie.media_type == 'series':
        caption = (f"📺 *{safe_title}* - New Episode!\n\n"
                   f"🔢 *Season {movie.season or 1} - Episode {movie.episode or 1}*\n"
                   f"⭐️ *Rating:* {safe_rating} / 10\n"
                   f"{runtime_text}"
                   f"🎭 *Category:* {tags}\n\n"
                   f"{safe_plot}"
                   f"✅ *Subtitles Ready:* Malayalam, Tamil, Hindi\n"
                   f"⚡️ *High-Speed Download*\n\n"
                   f"👇 *Get the episode here:*{footer}")
        encoded_title = urllib.parse.quote(safe_title)
        button_url = f"{website_base_url}/series/{encoded_title}/{movie.season or 1}"
    else:
        caption = (f"🎬 *{safe_title}*\n\n"
                   f"⭐️ *Rating:* {safe_rating} / 10\n"
                   f"{runtime_text}"
                   f"🎭 *Category:* {tags}\n\n"
                   f"{safe_plot}"
                   f"✅ *Subtitles Ready:* Malayalam, Tamil, Hindi\n"
                   f"⚡️ *High-Speed Download*\n\n"
                   f"👇 *Get the movie here:*{footer}")
        button_url = f"{website_base_url}/movie/{movie.id}"

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        payload = {
            "chat_id": CHANNEL_ID,
            "photo": movie.poster_url or "https://via.placeholder.com/500x750?text=No+Poster",
            "caption": caption,
            "parse_mode": "Markdown",
            "reply_markup": {"inline_keyboard": [[{"text": "📥 Download Subtitles", "url": button_url}]]}
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            return "Posted to Telegram Successfully!", 200
        else:
            return f"Telegram API Error: {response.text}", 500
    except Exception as e:
        return str(e), 500

# --- SITEMAP & REQUESTS ---
@app.route('/sitemap.xml')
def sitemap():
    base_url = "https://malayalamsubtitles.onrender.com"
    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    xml.append(f'<url><loc>{base_url}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>')

    all_media = Movie.query.order_by(Movie.id.desc()).all()
    for media in all_media:
        if media.media_type == 'series':
            encoded_title = urllib.parse.quote(media.title)
            url = f"{base_url}/series/{encoded_title}/{media.season or 1}"
        else:
            url = f"{base_url}/movie/{media.id}"
        xml.append(f'<url><loc>{url}</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>')

    xml.append('</urlset>')
    return app.response_class('\n'.join(xml), mimetype='application/xml')

@app.route('/api/request_sub', methods=['POST'])
def request_sub():
    data = request.json
    title = data.get('title')
    details = data.get('details', 'No extra details provided.')

    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID')
    if not TELEGRAM_TOKEN or not CHANNEL_ID:
        return jsonify({"error": "Telegram not configured"}), 500

    msg = f"🔔 *New Subtitle Request from Website*\n\n🎬 *Title:* {title}\n📝 *Details:* {details}\n\n_Admin, add this to your upload list!_"

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": CHANNEL_ID, "text": msg, "parse_mode": "Markdown"}
        requests.post(url, json=payload)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------ AUTO FETCH SCHEDULER ENDPOINT ------------------
@app.route('/api/scheduled_fetch', methods=['POST', 'GET'])
def scheduled_fetch():
    """Called by external cron job every 2 hours. Fetches new OTT (digital release) movies only."""
    secret = request.args.get('secret') or request.headers.get('X-Auth-Secret')
    if secret != os.environ.get('SCHEDULER_SECRET', 'scheduler-secret'):
        return jsonify({"error": "unauthorized"}), 401

    TMDB_API_KEY = os.environ.get('TMDB_API_KEY')
    SUBDL_API_KEY = os.environ.get('SUBDL_API_KEY')
    if not TMDB_API_KEY or not SUBDL_API_KEY:
        return jsonify({"error": "API keys missing"}), 500

    # 1. Collect movie IDs from TMDB, focusing on digital releases
    movie_ids = set()

    # a) Movies with digital release (OTT), sorted by newest
    try:
        # with_release_type=4 filters for digital release
        url = (
            f"https://api.themoviedb.org/3/discover/movie?"
            f"api_key={TMDB_API_KEY}&language=en-US&sort_by=release_date.desc"
            f"&with_release_type=4&page=1"
        )
        resp = requests.get(url).json()
        for m in resp.get('results', [])[:20]:
            movie_ids.add(m['id'])
    except Exception as e:
        print(f"TMDB discover error: {e}")

    # b) Now playing movies (cinema) – but only those that also have a digital release
    #    We'll fetch a few and later filter by checking release types
    try:
        url = f"https://api.themoviedb.org/3/movie/now_playing?api_key={TMDB_API_KEY}&language=en-US&page=1"
        resp = requests.get(url).json()
        for m in resp.get('results', [])[:10]:
            # quick check if the movie has a digital release before adding
            movie_id = m['id']
            # To avoid extra API calls, we can add them now and check later during IMDb ID lookup,
            # or we can skip now_playing entirely to keep it purely OTT. 
            # I'll keep a small selection but will verify digital release later.
            movie_ids.add(movie_id)
    except:
        pass

    new_movies = 0
    for tmdb_id in movie_ids:
        # 2. Get IMDb ID from TMDB
        try:
            ext_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
            ext_data = requests.get(ext_url).json()
            imdb_id = ext_data.get('imdb_id')
            if not imdb_id:
                continue
        except:
            continue

        # Skip if already in database
        if Movie.query.filter_by(imdb_id=imdb_id, media_type='movie').first():
            continue

        # 3. Verify the movie has a digital release (skip if not)
        #    We can do this by checking the movie's release dates from TMDB
        try:
            release_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates?api_key={TMDB_API_KEY}"
            release_data = requests.get(release_url).json()
            has_digital = False
            for country in release_data.get('results', []):
                for rd in country.get('release_dates', []):
                    if rd.get('type') == 4:  # 4 = Digital
                        has_digital = True
                        break
                if has_digital:
                    break
            if not has_digital:
                continue   # Skip if no digital release date found
        except:
            # If we can't verify, skip to be safe
            continue

        # 4. Download English subtitle from Subdl
        srt_text = None
        try:
            imdb_id_fmt = imdb_id if imdb_id.startswith('tt') else f"tt{imdb_id}"
            subdl_url = f"https://api.subdl.com/api/v1/subtitles?api_key={SUBDL_API_KEY}&imdb_id={imdb_id_fmt}&type=movie&languages=EN"
            subdl_resp = requests.get(subdl_url, headers={"User-Agent": "Mozilla/5.0..."})
            if subdl_resp.status_code == 200:
                subdl_data = subdl_resp.json()
                if subdl_data.get('status') and subdl_data.get('subtitles'):
                    subs = subdl_data['subtitles']
                    def score_sub(sub):
                        if sub.get('hearing_impaired', False):
                            return -1
                        fmt_score = 2 if sub.get('format', '').lower() == 'srt' else 0
                        downloads = int(sub.get('downloads', 0))
                        rating = float(sub.get('rating', 0))
                        return (fmt_score, downloads, rating)
                    scored = []
                    for s in subs:
                        sc = score_sub(s)
                        if sc != -1:
                            scored.append((sc, s))
                    if scored:
                        scored.sort(key=lambda x: x[0], reverse=True)
                        best = scored[0][1]
                        dl_url = "https://dl.subdl.com" + best['url']
                        dl_resp = requests.get(dl_url, headers={"User-Agent": "Mozilla/5.0..."})
                        if dl_resp.status_code == 200:
                            srt_text = dl_resp.text
        except Exception as e:
            print(f"Subdl error for {imdb_id}: {e}")
            continue

        if not srt_text:
            continue

        # 5. Upload to R2
        try:
            safe_id = imdb_id.replace('tt', '')
            r2_filename = f"english_movie_{safe_id}_{os.urandom(4).hex()}.srt"
            r2_url = None
            if s3_client and r2_bucket:
                s3_client.put_object(
                    Bucket=r2_bucket, Key=r2_filename,
                    Body=srt_text.encode('utf-8'), ContentType='application/x-subrip'
                )
                r2_url = f"{r2_public_url.rstrip('/')}/{r2_filename}"
        except:
            continue

        # 6. Gather full metadata from TMDB
        try:
            details_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            details = requests.get(details_url).json()
            title = details.get('title', 'Unknown')
            year = details.get('release_date', '')[:4] if details.get('release_date') else ''
            rating = str(details.get('vote_average', 'N/A'))
            poster = f"https://image.tmdb.org/t/p/w500{details['poster_path']}" if details.get('poster_path') else 'https://via.placeholder.com/500x750?text=No+Poster'
            plot = details.get('overview', '')
            runtime = f"{details.get('runtime', '')} min" if details.get('runtime') else ''
        except:
            title = 'Unknown'
            year = ''
            rating = 'N/A'
            poster = 'https://via.placeholder.com/500x750?text=No+Poster'
            plot = ''
            runtime = ''

        new_movie = Movie(
            media_type='movie',
            title=title,
            year=year,
            rating=rating,
            poster_url=poster,
            english_srt=r2_url if r2_url else srt_text,
            category='General',
            plot=plot,
            runtime=runtime,
            imdb_id=imdb_id
        )
        db.session.add(new_movie)
        db.session.commit()

        if r2_url:
            threading.Thread(target=trigger_hf_translation, args=(new_movie.id, r2_url)).start()
        new_movies += 1

    return jsonify({"message": f"Added {new_movies} new OTT movies"}), 200
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
