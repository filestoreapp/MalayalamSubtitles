# MalayalamSubtitles

Download Malayalam, Tamil and Hindi subtitles for movies and TV shows.

## Stack
- Python Flask + Flask-SQLAlchemy, Gunicorn
- PostgreSQL (`DATABASE_URL`)
- Cloudflare R2 (S3-compatible) for English SRTs
- Hugging Face worker for translation (`HF_WORKER_URL` + `HF_SECRET`)
- Telegram bot (`TELEGRAM_BOT_TOKEN`)

## Run locally
```bash
pip install -r requirements.txt
export SECRET_KEY=... DATABASE_URL=... ADMIN_USERNAME=... ADMIN_PASSWORD_HASH=...
flask --app app init-db
gunicorn app:app --bind 0.0.0.0:5000
```

Generate a password hash with:
```bash
python3 -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('YOUR-PASSWORD'))"
```

## Deploy (Render)
- Service is defined in `render.yaml` (branch `Claude-3`, autoDeploy off).
- Secrets live only in Render dashboard env vars, never in the repo.
- After merging a PR, trigger a manual deploy in Render dashboard or via API.
- Run `migrate.sql` in Render Postgres shell after deploying schema changes.

## Security notes
- Startup fails fast if `SECRET_KEY`, `HF_SECRET`, `TELEGRAM_SECRET`, `SCHEDULER_SECRET` are missing.
- Admin auth uses `ADMIN_USERNAME` + `ADMIN_PASSWORD_HASH` (Werkzeug). Legacy `ADMIN_USER`/`ADMIN_PASS` still work with a deprecation warning; migrate to hash.
- All state-changing admin actions are POST + CSRF. Bearer endpoints (`/bot/webhook`, `/api/translation_callback`, `/api/trigger_telegram`, `/api/scheduled_fetch`, `POST /start-worker`) use `Authorization: Bearer` / `X-Auth-Secret` headers, never query-string secrets.
- Telegram webhooks are verified via `X-Telegram-Bot-Api-Secret-Token` when `TELEGRAM_WEBHOOK_SECRET` is set. Re-register webhook with `secret_token` after setting it.
- See `SETUP_NOTES.md` for webhook, HF worker, and migration details.
