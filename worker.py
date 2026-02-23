from flask import Flask
import threading
import time
import pysrt
from deep_translator import GoogleTranslator
from app import app as main_app, db, Movie, TranslationJob, TranslationCache

worker_app = Flask(__name__)
worker_running = False 

def process_queue():
    global worker_running
    if worker_running: return
    worker_running = True

    with main_app.app_context():
        while True:
            job = TranslationJob.query.filter_by(status='Pending').first()
            if not job: break 

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
                    except:
                        translated_chunk = ["" for _ in chunk]
                        
                    for j, sub in enumerate(chunk_subs):
                        txt = translated_chunk[j] if j < len(translated_chunk) else ""
                        if txt: txt = txt.replace("", "").strip()
                        full_srt_text += f"{sub.index}\n{sub.start} --> {sub.end}\n{txt}\n\n"
                    time.sleep(2) 
                    
                new_cache = TranslationCache(movie_id=movie.id, language=job.language, translated_srt=full_srt_text)
                db.session.add(new_cache)
                job.status = 'Completed'
                db.session.commit()

            except Exception as e:
                job.status = 'Failed'
                db.session.commit()

    worker_running = False 

@worker_app.route('/start-worker', methods=['POST'])
def start_worker():
    thread = threading.Thread(target=process_queue)
    thread.start()
    return "Worker Started", 200

if __name__ == "__main__":
    worker_app.run(host='0.0.0.0', port=8000)
