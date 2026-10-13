# Setup Notes

## 1. Run Database Migration
Go to your Render PostgreSQL dashboard → Shell, paste contents of `migrate.sql`.
Do NOT run the unique index until duplicates are cleaned (Animals ID 1275, Odyssey loser).

## 2. Register Telegram Bot Webhook (with secret)
Set `TELEGRAM_WEBHOOK_SECRET` in Render first, then register:
```
https://api.telegram.org/bot<TOKEN>/setWebhook?url=<YOUR_RENDER_URL>/bot/webhook&secret_token=<TELEGRAM_WEBHOOK_SECRET>
```
Legacy `/api/telegram_webhook` still works (verified) but is deprecated; migrate to `/bot/webhook`.
Verify with `getWebhookInfo`.

## 3. Environment Variables (Render, values never in repo)
| Variable | Purpose |
|---|---|
| SECRET_KEY | Flask secret, min 32 chars, required |
| ADMIN_USERNAME / ADMIN_PASSWORD_HASH | Hashed admin auth (Werkzeug). Legacy ADMIN_USER/ADMIN_PASS still work with warning |
| HF_SECRET / HF_WORKER_URL | HF worker auth + URL |
| TELEGRAM_SECRET / TELEGRAM_WEBHOOK_SECRET | Internal trigger secret / webhook verification |
| TELEGRAM_BOT_TOKEN / TELEGRAM_ADMIN_IDS / TELEGRAM_CHANNEL_ID | Bot |
| SCHEDULER_SECRET / WORKER_SECRET | Scheduler header secret / worker Bearer secret |
| TMDB_API_KEY / SUBDL_API_KEY / OS_API_KEY | Subtitle sources |
| ANTHROPIC_API_KEY | AI SEO descriptions (optional) |
| DATABASE_URL / R2_* | DB + storage |
| MAX_CONCURRENT_JOBS / SERIES_BATCH_SIZE / AVG_TRANSLATION_MINS | 5 / 3 / 4 |

## 4. HF Worker (separate service, not in this repo)
HF worker must send `Authorization: Bearer <HF_SECRET>` on callbacks and verify it.
Set on HF Space: `RENDER_CALLBACK_URL`, `RENDER_SECRET` (= HF_SECRET), `NUM_WORKERS`, `TRANSLATE_DELAY`, `PROGRESS_INTERVAL`.

## 5. New URLs
- /new-this-week → Weekly new subtitles page
- /subtitles/Action → Genre landing pages
- /admin/upload_planner → Content calendar
- /admin/search_analytics → Search analytics
- /admin/series → Series management
- /admin/duplicates → Duplicate detection
- POST /admin/cleanup_jobs → Delete Completed jobs older than 7 days
- POST /admin/refresh_r2_stats → Refresh cached R2 size/count
- POST /admin/duplicates/keep_best → Keep best duplicate, delete rest
