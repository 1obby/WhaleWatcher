"""
start.py — запускает Flask (Mini App) + бот (мониторинг Mantle) вместе.
Flask работает в фоновом потоке — отдаёт API для сайта.
Бот работает в главном потоке — мониторит блоки Mantle.
Оба процесса читают/пишут ОДНУ базу данных alerts.db.
"""
import threading
import os
import runpy

# ─── 1. Flask в фоновом потоке ────────────────────────────────
def _run_flask():
    from app import app, ensure_db
    ensure_db()
    port = int(os.environ.get("PORT", 5000))
    print(f"[FLASK] Сервер запущен на порту {port}")
    # use_reloader=False — обязательно в потоке!
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

flask_thread = threading.Thread(target=_run_flask, daemon=True)
flask_thread.start()
print("[START] Flask запущен в фоне ✅")

# ─── 2. Бот в главном потоке ─────────────────────────────────
# runpy запускает main.py как __main__ — срабатывает if __name__ == "__main__"
# и запускается asyncio.run(main()) с мониторингом блоков
print("[START] Запускаем бот и мониторинг Mantle...")
runpy.run_path("main (2).py", run_name="__main__")
