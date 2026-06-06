# Setup Notes for New Features

## 1. Run Database Migration
Go to your Render PostgreSQL dashboard → Shell, paste contents of migrate.sql

## 2. Register Telegram Bot Webhook
Replace {TOKEN} and {YOUR_RENDER_URL}:
```
https://api.telegram.org/bot{TOKEN}/setWebhook?url={YOUR_RENDER_URL}/api/telegram_webhook
```

## 3. Optional Environment Variables (Render)
| Variable | Value | Purpose |
|---|---|---|
| MAX_CONCURRENT_JOBS | 5 | HF worker concurrency |
| SERIES_BATCH_SIZE | 3 | Series jobs per dispatch cycle |
| AVG_TRANSLATION_MINS | 4 | ETA calculation |
| ANTHROPIC_API_KEY | sk-ant-... | Enable AI SEO descriptions |

## 4. HF Worker Update (hf_worker.py)
Set these on HuggingFace Space:
| Variable | Value |
|---|---|
| NUM_WORKERS | 3 |
| TRANSLATE_DELAY | 0.15 |
| PROGRESS_INTERVAL | 5 |
| RENDER_CALLBACK_URL | https://yourapp.onrender.com/api/translation_callback |
| RENDER_SECRET | (same as HF_SECRET on Render) |

## 5. New URLs
- /new-this-week → Weekly new subtitles page
- /subtitles/Action → Genre landing pages (all 18 genres)
- /admin/upload_planner → Content calendar
- /admin/search_analytics → Search analytics
- /admin/series → Series management
- /admin/duplicates → Duplicate detection
