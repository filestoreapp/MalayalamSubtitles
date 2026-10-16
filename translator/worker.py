"""
Subtitle Dog – Translation Worker (Render service)
===================================================
Runs the subtitle translation queue as a free Render web service, replacing
the HuggingFace Space worker.

  • Priority queue — movies (priority=1) before series (priority=2)
  • Batched Google Translate — one request per ~40 subtitle lines
    (the old worker sent one request PER LINE, ~20 req/s, and Google
    throttled it into a permanent 429 spiral)
  • Back-off with real cool-down (5s/10s/20s/40s) on failures
  • HF Helsinki fallback per failed batch, English kept as last resort
  • Same API contract as the old worker:
      POST /api/translate  {movie_id, english_srt_url, language, priority}
      GET  /api/status
      GET  /api/queue_info
  • Reports progress/completion back to the main app via callback

Env vars:
  DATABASE_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL,
  R2_BUCKET_NAME, R2_PUBLIC_URL,
  CALLBACK_URL      – main app, e.g. https://…onrender.com/api/translation_callback
  CALLBACK_SECRET   – Bearer token the main app expects (its HF_SECRET)
  API_SECRET        – Bearer token this worker expects on /api/translate
                      (set to the same value as the app's HF_SECRET)
  HF_API_TOKEN      – for the Helsinki fallback (optional)
  NUM_WORKERS (default 2), TRANSLATE_DELAY (default 0.4), BATCH_LINES (default 40)
"""

import hmac
import os
import queue
import re
import socket
import threading
import time

# Hard ceiling on any network operation. deep_translator does not set its own
# timeout, and Google sometimes black-holes datacenter connections instead of
# rejecting them — without this, a hung socket stalls the worker thread forever
# (no exception, no retry, no failure; the job just sits in-flight).
socket.setdefaulttimeout(30)

import boto3
import psycopg2
import requests
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

# ── Config ───────────────────────────────────────────────────────────────────
DATABASE_URL   = os.environ.get("DATABASE_URL", "")
R2_ACCESS_KEY  = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY  = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT    = os.environ.get("R2_ENDPOINT_URL", "")
R2_BUCKET      = os.environ.get("R2_BUCKET_NAME", "")
R2_PUBLIC_URL  = os.environ.get("R2_PUBLIC_URL", "")
CALLBACK_URL   = os.environ.get("CALLBACK_URL", "")
CALLBACK_SECRET = os.environ.get("CALLBACK_SECRET", "")
API_SECRET     = os.environ.get("API_SECRET", "")
HF_API_TOKEN   = os.environ.get("HF_API_TOKEN", "")

NUM_WORKERS     = int(os.environ.get("NUM_WORKERS", "2"))
TRANSLATE_DELAY = float(os.environ.get("TRANSLATE_DELAY", "0.4"))
BATCH_LINES     = int(os.environ.get("BATCH_LINES", "40"))

# ── State ────────────────────────────────────────────────────────────────────
_pqueue      = queue.PriorityQueue()   # (priority, seq, job_dict)
_seq_lock    = threading.Lock()
_seq_counter = 0
_in_flight      = set()               # {(movie_id, language)}
_in_flight_lock = threading.Lock()
_stats      = {"completed": 0, "failed": 0, "queued": 0, "workers": NUM_WORKERS}
_stats_lock = threading.Lock()


def _next_seq():
    global _seq_counter
    with _seq_lock:
        _seq_counter += 1
        return _seq_counter


# ── Translation ──────────────────────────────────────────────────────────────
def _chunk_jobs(jobs, max_lines=40, max_chars=4500):
    """Split (block_i, line_i, text) jobs into chunks that fit one HTTP
    request: at most max_lines lines and max_chars characters."""
    chunk, chars = [], 0
    for job in jobs:
        t = job[2]
        if chunk and (len(chunk) >= max_lines or chars + len(t) + 1 > max_chars):
            yield chunk
            chunk, chars = [], 0
        chunk.append(job)
        chars += len(t) + 1
    if chunk:
        yield chunk


def _google_batch(lines, target_lang):
    """Translate a list of lines with ONE HTTP request to Google's gtx
    endpoint (newline-joined, alignment verified). Returns the list of
    translated lines, or None if all attempts failed."""
    if not lines:
        return []
    url = "https://translate.googleapis.com/translate_a/single"
    params = {"client": "gtx", "sl": "en", "tl": target_lang, "dt": "t"}
    for attempt in range(3):
        try:
            r = requests.post(url, params=params,
                              data={"q": "\n".join(lines)}, timeout=20)
            if r.status_code == 429 or r.status_code >= 500:
                raise Exception(f"HTTP {r.status_code}")
            r.raise_for_status()
            data = r.json()
            segs = data[0] if isinstance(data, list) and data else []
            full = "".join(seg[0] for seg in segs if seg and seg[0])
            out = full.split("\n")
            if len(out) == len(lines):
                return out
            raise ValueError(f"line mismatch ({len(out)} != {len(lines)})")
        except Exception as e:
            wait = [3, 8, 15][attempt]
            print(f"  \u26a0\ufe0f  Google [{target_lang}] batch of {len(lines)} "
                  f"attempt {attempt+1} failed: {e} (retry in {wait}s)", flush=True)
            time.sleep(wait)
    return None


def translate_srt(english_srt: str, target_lang: str,
                  progress_callback=None) -> str:
    """
    Translate an SRT file to target_lang.
    • Primary: Google Translate gtx endpoint, TRUE batching — all dialogue
      lines are sent newline-joined, one HTTP request per ~40 lines
      (~50x fewer requests than one-per-line).
    • Fallback: HuggingFace Helsinki-NLP opus-mt model (per failed batch).
    • Last resort: keep the original English line.
    Reports progress as batches complete.
    """
    hf_url = (f"https://api-inference.huggingface.co/models/"
              f"Helsinki-NLP/opus-mt-en-{target_lang}")
    hf_hdrs = {"Authorization": f"Bearer {HF_API_TOKEN}"} if HF_API_TOKEN else None

    def hf_fallback(lines):
        # Single request for the whole batch (newline-joined). If the model
        # doesn't preserve line alignment, fall back to English.
        try:
            r = requests.post(hf_url, headers=hf_hdrs,
                              json={"inputs": "\n".join(lines)}, timeout=25)
            r.raise_for_status()
            data = r.json()
            txt = data[0].get("translation_text", "") if isinstance(data, list) and data else ""
            out = txt.split("\n")
            if len(out) == len(lines):
                return out
            print(f"  \u26a0\ufe0f  HF line mismatch ({len(out)} != {len(lines)})",
                  flush=True)
        except Exception as e:
            print(f"  \u274c HF fallback failed: {e}", flush=True)
        return list(lines)

    blocks = re.split(r"\n\s*\n", english_srt.strip())

    # Parse: keep (index, timestamp, dialogue) per block; collect every
    # non-blank dialogue line for batched translation.
    parsed = []   # [(index_line, timestamp_line, [dialogue lines])]
    jobs = []     # [(block_i, line_i, text)]
    for bi, block in enumerate(blocks):
        lines = block.strip().splitlines()
        if len(lines) < 2:
            parsed.append((None, None, lines))
            continue
        dialogue = lines[2:]
        parsed.append((lines[0], lines[1], dialogue))
        for li, l in enumerate(dialogue):
            if l.strip():
                jobs.append((bi, li, l.strip()))

    # Translate in true HTTP batches.
    results = {}
    chunks = list(_chunk_jobs(jobs, BATCH_LINES, 4500))
    for ci, chunk in enumerate(chunks):
        chunk_txt = [c[2] for c in chunk]
        out = _google_batch(chunk_txt, target_lang)
        if out is None and hf_hdrs:
            print(f"  \u21aa\ufe0f  HF fallback for batch of {len(chunk)} lines",
                  flush=True)
            out = hf_fallback(chunk_txt)
        if out is None:
            out = chunk_txt  # last resort: keep English
        for (bi, li, _), tr in zip(chunk, out):
            txt = tr.replace("", "").strip() if tr else ""
            results[(bi, li)] = txt
        if progress_callback and chunks:
            progress_callback(min(int((ci + 1) / len(chunks) * 100), 100))
        time.sleep(TRANSLATE_DELAY)

    # Reassemble: timestamps untouched, only dialogue swapped.
    translated = []
    for bi, (index_line, timestamp_line, dialogue) in enumerate(parsed):
        if index_line is None:
            translated.append("\n".join(dialogue))
            continue
        out_lines = []
        for li, l in enumerate(dialogue):
            tr = results.get((bi, li))
            out_lines.append(tr if (l.strip() and tr) else l)
        translated.append(f"{index_line}\n{timestamp_line}\n" + "\n".join(out_lines))

    if progress_callback:
        progress_callback(100)
    print(f"  \U0001f4dd [{target_lang}] done: {len(blocks)} blocks, "
          f"{len(jobs)} lines in {len(chunks)} HTTP requests", flush=True)
    return "\n\n".join(translated) + "\n"


# ── R2 & DB ───────────────────────────────────────────────────────────────────
def upload_to_r2(filename: str, content: bytes) -> str:
    s3 = boto3.client("s3", endpoint_url=R2_ENDPOINT,
                      aws_access_key_id=R2_ACCESS_KEY,
                      aws_secret_access_key=R2_SECRET_KEY)
    s3.put_object(Bucket=R2_BUCKET, Key=filename, Body=content,
                  ContentType="text/plain; charset=utf-8")
    return f"{R2_PUBLIC_URL.rstrip('/')}/{filename}"


def update_db(movie_id: int, language: str, srt_url: str):
    # SELECT-then-upsert: no unique constraint required on the table.
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM translation_cache WHERE movie_id=%s "
                    "AND language=%s LIMIT 1", (movie_id, language))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE translation_cache SET translated_srt=%s WHERE id=%s",
                        (srt_url, row[0]))
        else:
            cur.execute("INSERT INTO translation_cache "
                        "(movie_id, language, translated_srt, downloads) "
                        "VALUES (%s, %s, %s, 0)", (movie_id, language, srt_url))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def notify_app(movie_id: int, language: str, status: str, progress: int = 0):
    if not CALLBACK_URL:
        return
    payload = {"movie_id": movie_id, "language": language,
               "status": status, "progress": progress}
    for attempt in range(3):
        try:
            r = requests.post(CALLBACK_URL, json=payload,
                              headers={"Authorization": f"Bearer {CALLBACK_SECRET}"},
                              timeout=10)
            r.raise_for_status()
            return
        except Exception as e:
            print(f"  ⚠️  Callback attempt {attempt+1} failed: {e}", flush=True)
            time.sleep(2)
    print(f"  ❌ All callback attempts failed for movie {movie_id} ({language})",
          flush=True)


# ── Job processor ────────────────────────────────────────────────────────────
def process_job(movie_id: int, english_srt_src: str, target_lang: str, priority: int):
    key = (movie_id, target_lang)
    start = time.time()
    print(f"\n🚀 START movie={movie_id} lang={target_lang} priority={priority}", flush=True)

    def on_progress(pct):
        notify_app(movie_id, target_lang, "Processing", progress=pct)

    try:
        # English source may be a URL or inline SRT text.
        src = (english_srt_src or "").strip()
        if src.startswith("http"):
            r = requests.get(src, timeout=30)
            r.raise_for_status()
            english_srt = r.text
        else:
            english_srt = english_srt_src
        print(f"  📥 Source: {len(english_srt.splitlines())} lines", flush=True)
        notify_app(movie_id, target_lang, "Processing", progress=0)

        translated = translate_srt(english_srt, target_lang, progress_callback=on_progress)

        filename = f"{target_lang}_{movie_id}.srt"
        public_url = upload_to_r2(filename, translated.encode("utf-8"))
        print(f"  ☁️  Uploaded → {public_url}", flush=True)

        update_db(movie_id, target_lang, public_url)
        notify_app(movie_id, target_lang, "Completed", progress=100)

        print(f"  ✅ DONE in {time.time()-start:.1f}s\n", flush=True)
        with _stats_lock:
            _stats["completed"] += 1
    except Exception as e:
        print(f"  ❌ FAILED after {time.time()-start:.1f}s: {e}\n", flush=True)
        notify_app(movie_id, target_lang, "Failed", progress=0)
        with _stats_lock:
            _stats["failed"] += 1
    finally:
        with _in_flight_lock:
            _in_flight.discard(key)


def worker_loop():
    while True:
        try:
            priority, seq, job = _pqueue.get(timeout=5)
            process_job(job["movie_id"], job["english_srt_url"],
                        job["language"], priority)
            _pqueue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"❌ Worker loop error: {e}", flush=True)
            _pqueue.task_done()


for _ in range(NUM_WORKERS):
    threading.Thread(target=worker_loop, daemon=True).start()

print(f"✅ {NUM_WORKERS} translation workers started "
      f"(batch={BATCH_LINES} lines, delay={TRANSLATE_DELAY}s)", flush=True)


# ── API ──────────────────────────────────────────────────────────────────────
api = FastAPI(title="Subtitle Dog Translation Worker")


class TranslateRequest(BaseModel):
    movie_id: int
    english_srt_url: str
    language: str = "ml"
    priority: int = 2


def _check_auth(authorization: str):
    if not API_SECRET:
        return
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not hmac.compare_digest(token, API_SECRET):
        raise HTTPException(status_code=401, detail="unauthorized")


@api.post("/api/translate")
async def translate_endpoint(req: TranslateRequest,
                             authorization: str = Header(default="")):
    _check_auth(authorization)
    key = (req.movie_id, req.language)
    with _in_flight_lock:
        if key in _in_flight:
            return {"status": "already_queued", "movie_id": req.movie_id,
                    "language": req.language}
        _in_flight.add(key)
    seq = _next_seq()
    _pqueue.put((req.priority, seq, {"movie_id": req.movie_id,
                                    "english_srt_url": req.english_srt_url,
                                    "language": req.language}))
    with _stats_lock:
        _stats["queued"] += 1
    depth = _pqueue.qsize()
    print(f"📥 Queued movie={req.movie_id} lang={req.language} "
          f"priority={req.priority} (depth: {depth})", flush=True)
    return {"status": "queued", "movie_id": req.movie_id,
            "language": req.language, "priority": req.priority,
            "queue_depth": depth}


@api.get("/api/status")
async def status():
    with _in_flight_lock:
        in_flight_list = list(_in_flight)
    with _stats_lock:
        stats_copy = dict(_stats)
    return {"status": "ok", "workers": NUM_WORKERS,
            "queue_depth": _pqueue.qsize(),
            "in_flight": len(in_flight_list),
            "in_flight_jobs": in_flight_list,
            "translate_delay": TRANSLATE_DELAY,
            "batch_lines": BATCH_LINES,
            **stats_copy}


@api.get("/api/queue_info")
async def queue_info():
    with _pqueue.mutex:
        items = list(_pqueue.queue)
    return {"total_pending": len(items),
            "movie_pending": sum(1 for i in items if i[0] == 1),
            "series_pending": sum(1 for i in items if i[0] == 2),
            "next_job": {"priority": items[0][0],
                         "movie_id": items[0][2]["movie_id"],
                         "language": items[0][2]["language"]} if items else None}
