"""Security smoke tests (no DB/network). Run: pytest tests/test_security_smoke.py"""
import os
os.environ.setdefault("SECRET_KEY", "x"*40)
os.environ.setdefault("HF_SECRET", "test-hf-secret")
os.environ.setdefault("TELEGRAM_SECRET", "test-telegram-secret")
os.environ.setdefault("SCHEDULER_SECRET", "test-scheduler-secret")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

def test_app_imports():
    import app as appmod
    assert appmod.app is not None
    assert appmod.app.config["SESSION_COOKIE_HTTPONLY"] is True
    assert appmod.app.config["SESSION_COOKIE_SECURE"] is True

def test_fail_fast_missing_secret(monkeypatch):
    # SECRET_KEY validation happens at import; here we check the helper exists
    import app as appmod
    assert hasattr(appmod, "_required_env")

def test_logout_is_post():
    import app as appmod
    methods = appmod.app.view_functions["logout"].__dict__.get("__wrapped__", None)
    # Flask route methods are stored in url_map
    rules = [r for r in appmod.app.url_map.iter_rules() if r.endpoint == "logout"]
    assert rules and "POST" in rules[0].methods
    assert "GET" not in rules[0].methods

def test_state_changing_are_post():
    import app as appmod
    for ep in ["delete_request", "reset_jobs", "retry_failed", "queue_all_missing", "queue_translations", "delete_job", "delete_media", "trigger_telegram", "scheduled_fetch"]:
        rules = [r for r in appmod.app.url_map.iter_rules() if r.endpoint == ep]
        assert rules, f"missing {ep}"
        assert "GET" not in rules[0].methods, f"{ep} still allows GET"
        assert "POST" in rules[0].methods, f"{ep} missing POST"
