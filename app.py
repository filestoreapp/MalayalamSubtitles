import os
import io
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import pysrt
from deep_translator import GoogleTranslator

app = Flask(__name__)

# --- THE NEW CLOUD DATABASE CONNECTION ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://neondb_owner:npg_fuIRzQj83YZo@ep-fragrant-forest-a1pm9zrx-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = 'malayalam_subtitle_hub_secret_key' # Change this later for security

db = SQLAlchemy(app)

# --- DATABASE MODELS ---
class Movie(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    media_type = db.Column(db.String(10))    
    title = db.Column(db.String(200)) 
    season = db.Column(db.Integer, nullable=True)  
    episode = db.Column(db.Integer, nullable=True) 
    year = db.Column(db.String(4))           
    rating = db.Column(db.String(10))        
    poster_url = db.Column(db.String(500)) # Saves Image URL
    english_srt = db.Column(db.Text)
    views = db.Column(db.Integer, default=0)

class TranslationCache(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movie.id', ondelete='CASCADE'))
    language = db.Column(db.String(10)) 
    translated_srt = db.Column(db.Text)
    downloads = db.Column(db.Integer, default=0)
    movie = db.relationship('Movie', backref=db.backref('translations', cascade='all, delete-orphan'))

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
    if search_query:
        all_media = Movie.query.filter(Movie.title.ilike(f'%{search_query}%')).order_by(Movie.id.desc()).all()
    else:
        all_media = Movie.query.order_by(Movie.id.desc()).limit(100).all()
        
    display_items = {}
    for m in all_media:
        if m.media_type == 'series':
            if m.title not in display_items: display_items[m.title] = m 
        else: display_items[f"movie_{m.id}"] = m 
    return render_template('index.html', media_list=display_items.values(), search_query=search_query)
# --- KEEP ALIVE ROUTE FOR UPTIMEROBOT ---
@app.route('/keep-alive')
def keep_alive():
    return "Server is awake!", 200

@app.route('/series/<string:title>')
def series_hub(title):
    episodes = Movie.query.filter_by(media_type='series', title=title).order_by(Movie.season, Movie.episode).all()
    if not episodes: return redirect(url_for('index'))
    master_info = episodes[0]
    master_info.views += 1
    db.session.commit()
    
    seasons = {}
    for ep in episodes:
        if ep.season not in seasons: seasons[ep.season] = []
        ready_langs = [c.language for c in ep.translations]
        seasons[ep.season].append({'id': ep.id, 'episode': ep.episode, 'ready_languages': ready_langs})
    return render_template('series.html', title=title, seasons=seasons, master_info=master_info)

@app.route('/movie/<int:movie_id>')
def movie_hub(movie_id):
    movie = Movie.query.get_or_404(movie_id)
    movie.views += 1
    db.session.commit()
    ready_languages = [c.language for c in movie.translations]
    return render_template('movie.html', movie=movie, ready_languages=ready_languages)

@app.route('/request_translation/<int:movie_id>/<language>')
def request_translation(movie_id, language):
    if language == 'en': return redirect(url_for('preview', movie_id=movie_id, language='en'))
    cache = TranslationCache.query.filter_by(movie_id=movie_id, language=language).first()
    if cache: return redirect(url_for('preview', movie_id=movie_id, language=language))
    movie = Movie.query.get_or_404(movie_id)
    subs = pysrt.from_string(movie.english_srt)
    return render_template('loading.html', movie=movie, language=language, total_lines=len(subs))

@app.route('/preview/<int:movie_id>/<language>')
def preview(movie_id, language):
    movie = Movie.query.get_or_404(movie_id)
    if language == 'en': srt_text = movie.english_srt
    else:
        cache = TranslationCache.query.filter_by(movie_id=movie_id, language=language).first_or_404()
        srt_text = cache.translated_srt
    subs = pysrt.from_string(srt_text)
    return render_template('preview.html', movie=movie, language=language, subs=subs[:100])

@app.route('/api/translate_chunk', methods=['POST'])
def translate_chunk():
    data = request.json
    movie = Movie.query.get(data['movie_id'])
    subs = pysrt.from_string(movie.english_srt)
    chunk = [sub.text for sub in subs[data['start']:data['end']]]
    try:
        translated_chunk = GoogleTranslator(source='auto', target=data['language']).translate_batch(chunk)
    except: translated_chunk = ["" for _ in chunk]
    return jsonify({"translated": translated_chunk})

@app.route('/api/save_translation', methods=['POST'])
def save_translation():
    data = request.json
    movie = Movie.query.get(data['movie_id'])
    subs = pysrt.from_string(movie.english_srt)
    full_srt_text = ""
    for i, sub in enumerate(subs):
        if i < len(data['translated_texts']):
            txt = data['translated_texts'][i].replace("", "").strip()
            full_srt_text += f"{sub.index}\n{sub.start} --> {sub.end}\n{txt}\n\n"
    db.session.add(TranslationCache(movie_id=data['movie_id'], language=data['language'], translated_srt=full_srt_text))
    db.session.commit()
    return jsonify({"status": "success"})

@app.route('/download/<int:movie_id>/<language>')
def download(movie_id, language):
    movie = Movie.query.get_or_404(movie_id)
    if language == 'en': srt_text = movie.english_srt
    else:
        cache = TranslationCache.query.filter_by(movie_id=movie_id, language=language).first_or_404()
        srt_text = cache.translated_srt
        cache.downloads += 1
        db.session.commit()
    mem_file = io.BytesIO()
    mem_file.write(srt_text.encode('utf-8'))
    mem_file.seek(0)
    name = f"{movie.title}_S{movie.season:02d}E{movie.episode:02d}_{language}.srt" if movie.media_type == 'series' else f"{movie.title}_{language}.srt"
    return send_file(mem_file, as_attachment=True, download_name=name.replace(" ", "_"))

# --- ADMIN & DASHBOARD ROUTES ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form['username'] == 'admin' and request.form['password'] == 'malayalam123':
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Invalid credentials.')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('index'))

@app.route('/dashboard')
@login_required
def dashboard():
    stat = SiteStat.query.first()
    total_dl = db.session.query(func.sum(TranslationCache.downloads)).scalar() or 0
    total_subs = TranslationCache.query.count()
    all_media = Movie.query.order_by(Movie.id.desc()).all()
    pop_lang = db.session.query(TranslationCache.language, func.count(TranslationCache.id)).group_by(TranslationCache.language).order_by(func.count(TranslationCache.id).desc()).first()
    return render_template('dashboard.html', visitors=stat.total_visitors, downloads=total_dl, total_subtitles=total_subs, popular_lang=pop_lang, all_media=all_media)

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
        srt_file = request.files.get('file')           
        
        if srt_file and title and poster_url:
            content = srt_file.read().decode('utf-8', errors='ignore')
            new_media = Movie(
                media_type=media_type, title=title, 
                season=int(season) if season else None,
                episode=int(episode) if episode else None,
                year=year, rating=rating, 
                poster_url=poster_url, 
                english_srt=content
            )
            db.session.add(new_media)
            db.session.commit()
            return redirect(url_for('dashboard'))
    return render_template('admin.html')

if __name__ == '__main__':
    app.run(debug=True, port=8080)

