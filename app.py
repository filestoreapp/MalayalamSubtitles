import os
import io
import requests
import boto3
import urllib.parse
import re
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func

app = Flask(__name__)

# --- DATABASE CONNECTION & ANTI-CRASH FIX ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://neondb_owner:npg_fuIRzQj83YZo@ep-fragrant-forest-a1pm9zrx-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True, 'pool_recycle': 300}
app.secret_key = 'malayalam_subtitle_hub_secret_key' 

db = SQLAlchemy(app)

# --- CLOUDFLARE R2 SETUP ---
r2_endpoint = os.environ.get('R2_ENDPOINT_URL')
r2_access_key = os.environ.get('R2_ACCESS_KEY_ID')
r2_secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
r2_bucket = os.environ.get('R2_BUCKET_NAME')
r2_public_url = os.environ.get('R2_PUBLIC_URL')

if r2_endpoint and r2_access_key and r2_secret_key:
    s3_client = boto3.client('s3',
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key
    )
else:
    s3_client = None

# --- DATABASE MODELS ---
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
    movie = db.relationship('Movie', backref=db.backref('jobs', cascade='all, delete-orphan'))

class SiteStat(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    total_visitors = db.Column(db.Integer, default=0)

with app.app_context():
    db.create_all()
    if not SiteStat.query.first():
        db.session.add(SiteStat(total_visitors=0))
        db.session.commit()

# --- ADMIN PROTECTION ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# --- USER ROUTES ---
@app.route('/')
def index():
    stat = SiteStat.query.first()
    stat.total_visitors += 1
    db.session.commit()
    
    search_query = request.args.get('q', '')
    category_query = request.args.get('cat', '')
    view_all = request.args.get('view', '')
    
    all_media = Movie.query.order_by(Movie.id.desc()).all()

    categories_set = set()
    for movie in all_media:
        if movie.category:
            for cat in movie.category.split(','):
                if cat.strip() and cat.strip() != "SilentMode":
                    categories_set.add(cat.strip())
    categories_list = sorted(list(categories_set))

    if search_query:
        search_results = Movie.query.filter(
            (Movie.title.ilike(f'%{search_query}%')) | 
            (Movie.category.ilike(f'%{search_query}%'))
        ).order_by(Movie.id.desc()).all()
        return render_template('index.html', search_results=search_results, search_query=search_query, categories=categories_list)
    
    if category_query:
        search_results = Movie.query.filter(Movie.category.ilike(f'%{category_query}%')).order_by(Movie.id.desc()).all()
        return render_template('index.html', search_results=search_results, search_query=category_query, categories=categories_list)

    if view_all == 'trending':
        search_results = Movie.query.order_by(Movie.views.desc()).all()
        return render_template('index.html', search_results=search_results, search_query="All Trending Subtitles", categories=categories_list)
    elif view_all == 'latest':
        search_results = Movie.query.order_by(Movie.id.desc()).all()
        return render_template('index.html', search_results=search_results, search_query="All Latest Additions", categories=categories_list)

    latest_movies = all_media[:10] 
    top_movies = Movie.query.order_by(Movie.views.desc()).limit(10).all()
    hero_movies = top_movies[:5] 
    
    return render_template('index.html', latest_movies=latest_movies, top_movies=top_movies, hero_movies=hero_movies, categories=categories_list)

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

@app.route('/series/<string:title>/<int:season>')
def series_page(title, season):
    episodes = Movie.query.filter_by(media_type='series', title=title, season=season).order_by(Movie.episode.asc()).all()
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
        cache.downloads += 1
        db.session.commit()
        
    if srt_data.startswith('http'):
        try:
            response = requests.get(srt_data)
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

# --- ADMIN & DASHBOARD ROUTES ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form['username'] == 'admin' and request.form['password'] == 'malayalam123':
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Invalid credentials.')
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    stat = SiteStat.query.first()
    total_dl = db.session.query(func.sum(TranslationCache.downloads)).scalar() or 0
    total_subs = TranslationCache.query.count()
    all_media = Movie.query.order_by(Movie.id.desc()).all()
    pop_lang = db.session.query(TranslationCache.language, func.count(TranslationCache.id)).group_by(TranslationCache.language).order_by(func.count(TranslationCache.id).desc()).first()
    recent_jobs = TranslationJob.query.order_by(TranslationJob.id.desc()).limit(15).all()
    return render_template('dashboard.html', visitors=stat.total_visitors, downloads=total_dl, total_subtitles=total_subs, popular_lang=pop_lang, all_media=all_media, jobs=recent_jobs)

@app.route('/admin/reset_jobs')
@login_required
def reset_jobs():
    stuck_jobs = TranslationJob.query.filter_by(status='Processing').all()
    for job in stuck_jobs:
        job.status = 'Pending'
    db.session.commit()
    try:
        requests.get("https://malayalamsub-malayalamsubs.hf.space/start-worker", timeout=5)
    except Exception: pass
    return redirect(url_for('dashboard'))

@app.route('/admin/queue_translations/<int:movie_id>')
@login_required
def queue_translations(movie_id):
    for lang in ['ml', 'ta', 'hi']:
        if not TranslationCache.query.filter_by(movie_id=movie_id, language=lang).first() and \
           not TranslationJob.query.filter_by(movie_id=movie_id, language=lang).first():
            new_job = TranslationJob(movie_id=movie_id, language=lang)
            db.session.add(new_job)
    db.session.commit()
    try:
        requests.get("https://malayalamsub-malayalamsubs.hf.space/start-worker", timeout=5)
    except Exception: pass
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
            
            for lang in ['ml', 'ta', 'hi']:
                db.session.add(TranslationJob(movie_id=media.id, language=lang, status='Pending'))
            
            try:
                requests.get("https://malayalamsub-malayalamsubs.hf.space/start-worker", timeout=5)
            except Exception: pass

        db.session.commit()
        return redirect(url_for('dashboard'))

    return render_template('edit.html', media=media)

# --- DUAL-ENGINE AUTO FETCHER (Subdl + OpenSubtitles) ---
@app.route('/api/auto_fetch_srt', methods=['POST'])
@login_required
def auto_fetch_srt():
    data = request.json
    imdb_id = data.get('imdb_id')
    media_type = data.get('media_type', 'movie')
    season = data.get('season')
    episode = data.get('episode')

    SUBDL_API_KEY = "Fj3xMg24eTEfVxBSWOfx04kP55CHGtvB"
    OS_API_KEY = "9AnWofHGkYabMMjKUhXpeDdwqrLvss2n"

    if not str(imdb_id).startswith('tt'):
        imdb_id = f"tt{imdb_id}"

    # --- ENGINE 1: SUBDL ---
    if SUBDL_API_KEY:
        try:
            url = f"https://api.subdl.com/api/v1/subtitles?imdb_id={imdb_id}&languages=EN&api_key={SUBDL_API_KEY}"
            if media_type == 'series':
                url += f"&season_number={season}&episode_number={episode}"
                
            res = requests.get(url).json()
            if res.get('status') and res.get('subtitles'):
                dl_url = "https://dl.subdl.com" + res['subtitles'][0]['url']
                srt_text = requests.get(dl_url).text
                return jsonify({"success": True, "srt_text": srt_text, "source": "Subdl"})
        except Exception as e:
            print("Subdl Failed:", e)

    # --- ENGINE 2: OPENSUBTITLES ---
    if OS_API_KEY:
        try:
            headers = {"Api-Key": OS_API_KEY, "Content-Type": "application/json"}
            url = f"https://api.opensubtitles.com/api/v1/subtitles?imdb_id={imdb_id}&languages=en"
            if media_type == 'series':
                url += f"&season_number={season}&episode_number={episode}"

            search_res = requests.get(url, headers=headers).json()
            if search_res.get('data'):
                file_id = search_res['data'][0]['attributes']['files'][0]['file_id']
                dl_res = requests.post("https://api.opensubtitles.com/api/v1/download", headers=headers, json={"file_id": file_id}).json()
                link = dl_res.get('link')
                if link:
                    srt_text = requests.get(link).text
                    return jsonify({"success": True, "srt_text": srt_text, "source": "OpenSubtitles"})
        except Exception as e:
            print("OpenSubtitles Failed:", e)

    return jsonify({"error": "Failed to find English subtitles on both databases."}), 404

# --- MASTER UPLOAD ROUTE ---
@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    if request.method == 'POST':
        media_type = request.form.get('media_type')
        title = request.form.get('title')
        season = request.form.get('season')
        year = request.form.get('year')           
        rating = request.form.get('rating')
        poster_url = request.form.get('poster_url') 
        silent_upload = request.form.get('silent_upload') 
        plot = request.form.get('plot')
        runtime = request.form.get('runtime')
        
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

        for content, ep_str in items_to_process:
            storage_data = content 
            
            if s3_client and r2_bucket:
                safe_title = title.replace(" ", "_").replace("/", "").lower()
                ep_tag = f"_s{season}e{ep_str}" if media_type == 'series' else ""
                r2_filename = f"english_{safe_title}{ep_tag}_{os.urandom(4).hex()}.srt"
                try:
                    s3_client.put_object(
                        Bucket=r2_bucket, Key=r2_filename,
                        Body=content.encode('utf-8'), ContentType='application/x-subrip'
                    )
                    storage_data = f"{r2_public_url}/{r2_filename}"
                except Exception as e:
                    print(f"R2 Upload Failed: {e}")

            current_ep = int(ep_str) if media_type == 'series' else None
            
            new_media = Movie(
                media_type=media_type, title=title, 
                season=int(season) if season and media_type == 'series' else None,
                episode=current_ep, year=year, 
                rating=rating, poster_url=poster_url, english_srt=storage_data, 
                category=category_string, plot=plot, runtime=runtime
            )
            db.session.add(new_media)
            db.session.commit()

            for lang in ['ml', 'ta', 'hi']:
                db.session.add(TranslationJob(movie_id=new_media.id, language=lang, status='Pending'))
            db.session.commit()

        try: 
            requests.get("https://malayalamsub-malayalamsubs.hf.space/start-worker", timeout=5)
        except Exception: pass

        return redirect(url_for('dashboard'))

    return render_template('admin.html')

# --- SECURE RENDER RELAY FOR TELEGRAM ---
@app.route('/api/trigger_telegram/<int:movie_id>', methods=['GET', 'POST'])
def trigger_telegram(movie_id):
    if request.args.get('secret') != 'malayalam_super_secret_999':
        return "Unauthorized", 401

    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID')
    movie = Movie.query.get_or_404(movie_id)
    
    raw_category = movie.category if movie.category else "General"
    
    if "SilentMode" in raw_category:
        return "Silent Mode Active - No Post", 200

    if not TELEGRAM_TOKEN or not CHANNEL_ID:
        return "Missing Telegram Secrets on Render", 400

    clean_category = raw_category.replace(", SilentMode", "").replace("SilentMode", "")
    tags = " ".join([f"#{t.strip().replace(' ', '_')}" for t in clean_category.split(',') if t.strip()]) if clean_category.strip() else "#General"
    
    website_base_url = "https://malayalamsubtitles.onrender.com"
    footer = f"\n\n━━━━━━━━━━━━━━━━━━━━\n📢 *Join Channel:* @malayalam\\_sub1\n💬 *Request Subtitles:* @Subrequest\\_bot"
    
    safe_title = movie.title if movie.title else "Unknown Title"
    safe_rating = movie.rating if movie.rating else "N/A"
    
    runtime_text = f"⏱ *Runtime:* {movie.runtime}\n" if movie.runtime else ""
    
    safe_plot = 
