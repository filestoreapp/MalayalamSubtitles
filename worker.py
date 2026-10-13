import os
import hmac
import threading
import time
import pysrt
from deep_translator import GoogleTranslator
from flask import request, jsonify
from app import app as main_app, db, Movie, TranslationJob, TranslationCache

worker_app = Flask(__name__)
worker_running = False

def process_queue():
    global worker_running
    if worker_running:
        return
    worker_running = True
    with main_app.app_context():
        while True:
            job = TranslationJob.query.filter_by(status='Pending').first()
            if not job:
                break
            job.status = 'Processing'
            db.session.commit()
            try:
                movie = Movie.query.get(job.movie_id)
                subs = pysrt.from_string(movie.english_srt)
                full_srt_text = ""
                batch_size = 20
                for i in range(0, len(subs), batch_size):
                    chunk_subs = subs[i:i+batch_size]
                    chunk = [sub.text for sub in chunk_subs]
                    try:
                        translated_chunk = GoogleTranslator(source='auto', target=job.language).translate_batch(chunk)
                    except Exception:
                        translated_chunk = ["" for _ in chunk]
                    for j, sub in enumerate(chunk_subs):
                        txt = translated_chunk[j] if j < len(translated_chunk) else ""
                        if txt:
                            txt = txt.replace("", "").strip()
                        full_srt_text += f"{sub.index}\n{sub.start} --> {sub.end}\n{txt}\n\n"
                    time.sleep(2)
                new_cache = TranslationCache(movie_id=movie.id, language=job.language, translated_srt=full_srt_text)
                db.session.add(new_cache)
                job.status = 'Completed'
                db.session.commit()
            except Exception:
                job.status = 'Failed'
                db.session.commit()
    worker_running = False

@worker_app.route('/start-worker', methods=['POST'])
def start_worker():
    # NOTE: Deprecated local Google-Translate path. HF worker is canonical.
    # Require WORKER_SECRET when set; refuse to run as public entrypoint otherwise.
    expected = os.environ.get('WORKER_SECRET', '')
    if expected:
        auth = request.headers.get('Authorization', '')
        token = auth.replace('Bearer ', '', 1) if auth.startswith('Bearer ') else ''
        if not hmac.compare_digest(token, expected):
            return jsonify({"error": "unauthorized"}), 401
    if worker_running:
        return jsonify({"error": "already running"}), 409
    thread = threading.Thread(target=process_queue, daemon=True)
    thread.start()
    return "Worker Started", 200

if __name__ == "__main__":
    worker_app.run(host='0.0.0.0', port=7860)
