import os
import io
import requests
import boto3
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import pysrt
from deep_translator import GoogleTranslator

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
    all_media = Movie.query.order_by(Movie.id.desc()).all()

    if search_query:
        search_results = Movie.query.filter(Movie.title.ilike(f'%{search_query}%')).order_by(Movie.id.desc()).all()
        return render_template('index.html', search_results=search_results, search_query=search_query)
    
    latest_movies = all_media[:5]
    top_movies = Movie.query.order_by(Movie.views.desc()).limit(5).all()
    
    categories_set = set()
    for movie in all_media:
        if movie.category:
            for cat in movie.category.split(','):
                if cat.strip():
                    categories_set.add(cat.strip())
    
    categories_list = sorted(list(categories_set))
    return render_template('index.html', latest_movies=latest_movies, top_movies=top_movies, categories=categories_list, all_media=all_media)

@app.route('/robots.txt')
def robots_txt():
    rules = """User-agent: *
Disallow: /admin
Disallow: /login
Disallow: /delete/
Allow: /
"""
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
        srt_text = movie.english_srt
    else:
        cache = TranslationCache.query.filter_by(movie_id=movie_id, language=language).first_or_404()
        srt_text = cache.translated_srt
        cache.downloads += 1
        db.session.commit()
        
    # --- SMART ROUTING: CLOUDFLARE URL VS LEGACY TEXT ---
    if srt_text.startswith('http'):
        # It's an R2 URL! Redirect the user directly to Cloudflare's high-speed servers.
        return redirect(srt_text)
        
    # --- LEGACY DATABASE TEXT FALLBACK ---
    mem_file = io.BytesIO()
    mem_file.write(srt_text.encode('utf-8'))
    mem_file.seek(0)
    
    s = movie.season or 1
    e = movie.episode or 1
    
    if movie.media_type == 'series':
        name = f"{movie.title}_S{s:02d}E{e:02d}_{language}.srt"
    else:
        name = f"{movie.title}_{language}.srt"
        
    return send_file(mem_file, as_attachment=True, download_name=name.replace(" ", "_"), mimetype='application/x-subrip')

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

@app.route('/admin/queue_translations/<int:movie_id>')
@login_required
def queue_translations(movie_id):
    target_languages = ['ml', 'ta', 'hi'] 
    for lang in target_languages:
        if not TranslationCache.query.filter_by(movie_id=movie_id, language=lang).first() and \
           not TranslationJob.query.filter_by(movie_id=movie_id, language=lang).first():
            new_job = TranslationJob(movie_id=movie_id, language=lang)
            db.session.add(new_job)
    db.session.commit()

    try:
        hf_url = "https://malayalamsub-malayalamsubs.hf.space/start-worker"
        requests.get(hf_url, timeout=10)
    except Exception as e:
        print(f"Webhook signal failed, but job queued: {e}")

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

@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    if request.method == 'POST':
        media_type = request.form.get('media_type')
        title = request.form.get('title')
        season = request.form.get('season')
        episode = request.form.get('episode')
        year = request.form.get('year')
        rating = request.form.get('rating')
        poster_url = request.form.get('poster_url') 
        
        categories = request.form.getlist('category')
        category_string = ", ".join(categories)
        
        srt_file = request.files.get('file')           
        
        if srt_file and title and poster_url:
            content = srt_file.read().decode('utf-8', errors='ignore')
            
            # --- UPLOAD TO CLOUDFLARE R2 ---
            storage_data = content # Default to database text if Cloudflare fails
            if s3_client and r2_bucket:
                # Create a safe, unique filename (e.g. english_movie_a1b2.srt)
                safe_title = title.replace(" ", "_").replace("/", "").lower()
                r2_filename = f"english_{safe_title}_{os.urandom(4).hex()}.srt"
                try:
                    s3_client.put_object(
                        Bucket=r2_bucket,
                        Key=r2_filename,
                        Body=content.encode('utf-8'),
                        ContentType='application/x-subrip'
                    )
                    # Success! Save the URL instead of the 100KB text.
                    storage_data = f"{r2_public_url}/{r2_filename}"
                except Exception as e:
                    print(f"R2 Upload Failed: {e}")

            new_media = Movie(
                media_type=media_type, title=title, season=int(season) if season else None,
                episode=int(episode) if episode else None, year=year, rating=rating, 
                poster_url=poster_url, english_srt=storage_data, category=category_string
            )
            db.session.add(new_media)
            db.session.commit()
            return redirect(url_for('dashboard'))
    return render_template('admin.html')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
