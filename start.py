"""
start.py — Flask (Mini App) + бот (мониторинг Mantle) в одном процессе.
"""
import threading
import os
import runpy

def _run_flask():
    from app import app, ensure_db
    ensure_db()
    port = int(os.environ.get("PORT", 5000))
    print(f"[FLASK] Сервер на порту {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Flask в фоне
t = threading.Thread(target=_run_flask, daemon=True)
t.start()
print("[START] Flask запущен ✅")

# Бот в главном потоке
print("[START] Запуск бота...")
runpy.run_path("main.py", run_name="__main__")
