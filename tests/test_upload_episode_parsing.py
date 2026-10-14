"""Regression tests for the ONE PIECE S02E05 upload episode-parsing bug.

Root cause: the upload form's movie section always submitted a leading
fetched_srts[]="" + fetched_episodes[]="1" (display:none does NOT exclude
inputs from submission), while a series row contributed only fetched_srts[].
The old server code paired by raw index with fallback str(i+1), so a single
auto-fetch of Ep5 arrived as fetched_srts=["", SRT5], fetched_episodes=["1"]
and was parsed as episode 2 -> "Duplicate found: ... S02E02".

These tests send the exact browser-style form fields (old cached form AND
new fixed form) and assert the episode that actually lands in the DB.
SQLite, CSRF disabled, no network (HF worker URL unset).
Run: python3 tests/test_upload_episode_parsing.py
"""
import io
import os
import sys

os.environ['SECRET_KEY'] = 'test-secret-key-0123456789abcdef0123456789'
os.environ['DATABASE_URL'] = 'sqlite:///' + os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'ep_parse.db')
os.environ['ADMIN_USER'] = 'admin'
os.environ['ADMIN_PASS'] = 'testpass'
os.environ['HF_SECRET'] = 'dummy-hf-secret-for-local-test-only'
os.environ['TELEGRAM_SECRET'] = 'dummy-telegram-secret-for-local-test-only'
os.environ['SCHEDULER_SECRET'] = 'dummy-scheduler-secret-for-local-test-only'

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app as appmod  # noqa: E402
from app import app, db, Movie  # noqa: E402

app.config['WTF_CSRF_ENABLED'] = False
app.config['TESTING'] = True
if appmod.csrf:
    appmod.csrf.exempt(appmod.admin)
try:
    appmod.limiter.enabled = False
except Exception:
    pass

SRT = "1\n00:00:01,000 --> 00:00:02,000\nHello\n"
BASE = {'media_type': 'series', 'title': 'ONE PIECE', 'year': '2023',
        'season': '2', 'imdb_id': 'tt11737520', 'rating': '8.3',
        'poster_url': '', 'plot': '', 'runtime': '60 min',
        'silent_upload': 'yes'}


def _reset():
    """Fresh DB + the keeper S02E02 row, then a logged-in client."""
    with app.app_context():
        db.drop_all()
        db.create_all()
        db.session.add(Movie(media_type='series', title='ONE PIECE', season=2, episode=2,
                             year='2023', rating='8.3', poster_url='', english_srt='FAKE',
                             category='Action', plot='', runtime='60 min',
                             imdb_id='tt11737520', is_silent=True, slug='one-piece-2023-dd6e'))
        db.session.commit()
    c = app.test_client()
    with c.session_transaction() as s:
        s['logged_in'] = True
    return c


def _count(ep):
    with app.app_context():
        return Movie.query.filter_by(imdb_id='tt11737520', season=2, episode=ep).count()


def test_old_form_autofetch_ep5_creates_ep5():
    c = _reset()
    r = c.post('/admin', data={**BASE, 'episodes[]': ['1', '5'],
                               'fetched_srts[]': ['', SRT], 'fetched_episodes[]': ['1']})
    assert r.status_code in (301, 302), r.data[:300]
    assert _count(5) == 1
    assert _count(2) == 1  # keeper untouched, no second S02E02


def test_old_form_duplicate_ep2_blocked():
    c = _reset()
    r = c.post('/admin', data={**BASE, 'episodes[]': ['1', '2'],
                               'fetched_srts[]': ['', SRT], 'fetched_episodes[]': ['1']})
    assert r.status_code == 409
    assert b'S02E02' in r.data
    assert _count(2) == 1


def test_new_form_autofetch_ep3_works():
    c = _reset()
    r = c.post('/admin', data={**BASE, 'episodes[]': ['3'],
                               'fetched_srts[]': [SRT], 'fetched_episodes[]': ['3']})
    assert r.status_code in (301, 302), r.data[:300]
    assert _count(3) == 1


def test_old_form_full_season_fetch():
    c = _reset()
    eps = ['1', '3', '4', '5', '6', '7', '8']
    r = c.post('/admin', data={**BASE, 'episodes[]': ['1'] + eps,
                               'fetched_srts[]': [''] + [SRT] * len(eps),
                               'fetched_episodes[]': ['1']})
    assert r.status_code in (301, 302), r.data[:300]
    for e in eps:
        assert _count(int(e)) == 1, f"episode {e} missing/wrong"


def test_old_form_manual_file_ep5():
    c = _reset()
    r = c.post('/admin', data={**BASE, 'episodes[]': ['1', '5'],
                               'files[]': [(io.BytesIO(b''), ''),
                                           (io.BytesIO(SRT.encode()), 'S02E05.srt')]})
    assert r.status_code in (301, 302), r.data[:300]
    assert _count(5) == 1


def test_new_form_mixed_file_ep5_plus_fetched_ep6():
    c = _reset()
    r = c.post('/admin', data={**BASE, 'episodes[]': ['5'],
                               'fetched_srts[]': [SRT], 'fetched_episodes[]': ['6'],
                               'files[]': [(io.BytesIO(SRT.encode()), 'S02E05.srt')]})
    assert r.status_code in (301, 302), r.data[:300]
    assert _count(5) == 1
    assert _count(6) == 1


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn()
            print(f"PASS {name}")
    print("ALL PASS")
