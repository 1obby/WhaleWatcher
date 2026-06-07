"""
start.py — запускает Flask + бот вместе в одном процессе.
Flask работает в фоновом потоке, бот в главном.
"""
import threading
import asyncio
import os
import sys

def run_flask():
    """Запуск Flask в отдельном потоке."""
    from app import app, ensure_db, migrate_from_json
    ensure_db()
    migrate_from_json("alerts.json")
    port = int(os.environ.get("PORT", 5000))
    print(f"[FLASK] Запуск на порту {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Запускаем Flask в фоновом потоке
flask_thread = threading.Thread(target=run_flask, daemon=True)
flask_thread.start()
print("[START] Flask запущен в фоне")

# Запускаем бот в главном потоке
print("[START] Запуск бота...")
import main  # твой main.py