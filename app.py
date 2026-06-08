"""
WhaleWatcher Mini App — Flask Backend  v2
Читает alerts.db (SQLite WAL), отдаёт REST API + Telegram WebApp.

Зависимости: pip install flask gunicorn
"""

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, g, jsonify, render_template, request

# ──────────────────────────────────────────────────────────────────────────────
# КОНФИГ
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).parent
DB_FILE    = os.getenv("DB_FILE",    str(BASE_DIR / "alerts.db"))
PORT       = int(os.getenv("PORT",   5000))
MANTLESCAN = "https://mantlescan.xyz/tx/"

app = Flask(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# CORS — необходим для Telegram WebView
# ──────────────────────────────────────────────────────────────────────────────

@app.after_request
def cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["X-Content-Type-Options"]        = "nosniff"
    return resp


# ──────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_=None):
    db = g.pop("db", None)
    if db:
        db.close()


def ensure_db() -> None:
    """Создаёт схему БД если её нет (idempotent)."""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    with conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                tx_hash   TEXT    UNIQUE NOT NULL,
                value_mnt REAL    NOT NULL,
                from_addr TEXT    NOT NULL,
                to_addr   TEXT    NOT NULL,
                type      TEXT    NOT NULL,
                ai_signal TEXT    DEFAULT '',
                tags      TEXT    DEFAULT '[]',
                extra     TEXT    DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ts   ON alerts(timestamp);
            CREATE INDEX IF NOT EXISTS idx_type ON alerts(type);
            CREATE INDEX IF NOT EXISTS idx_from ON alerts(from_addr);
        """)
    conn.close()
    print(f"[DB] OK → {DB_FILE}")


def migrate_from_json(path: str = "alerts.json") -> int:
    """Импортирует alerts.json → SQLite при первом запуске."""
    src = BASE_DIR / path
    if not src.exists():
        return 0
    try:
        data = json.loads(src.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[MIGRATE] Ошибка чтения {path}: {e}")
        return 0

    conn  = sqlite3.connect(DB_FILE)
    count = 0
    with conn:
        for entry in data:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO alerts
                        (timestamp, tx_hash, value_mnt, from_addr,
                         to_addr, type, ai_signal, tags)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (
                    entry.get("timestamp", ""),
                    entry.get("tx_hash",   ""),
                    float(entry.get("value_mnt", 0)),
                    entry.get("from_addr", ""),
                    entry.get("to_addr",   ""),
                    entry.get("type",      "transfer"),
                    entry.get("ai_signal", ""),
                    json.dumps(entry.get("tags") or []),
                ))
                count += 1
            except Exception:
                pass
    conn.close()
    return count


# ──────────────────────────────────────────────────────────────────────────────
# WALLET REGISTRY  (из TAGGED_WALLETS в main.py)
# ──────────────────────────────────────────────────────────────────────────────

WALLET_LABELS: dict[str, tuple[str, str]] = {
    # addr_lower: (label, category)
    "0x0000004eba872864a71b957180eb17dff71bb8f1": ("🐋 Mega Whale",       "Smart Money"),
    "0x88a8984f2b8507bbc1c699594e3a4ecdefed4784": ("❄️ Cold Storage",      "Smart Money"),
    "0x7647b72b4c89446f7d86bb7a30fd51b6d91577aa": ("🔀 Personal Relay",   "Routing"),
    "0xf22943d05ab93f63b0a229b12f4425e72a4c1f1c": ("🐳 Whale #1",          "Smart Money"),
    "0x59800fc68c7039566ed7a04b0f735255093cac1d": ("🐳 Whale #2",          "Smart Money"),
    "0x6117a8af9d748780051415433a5702ee5f669d2d": ("🐳 Whale #3",          "Smart Money"),
    "0x0f0c716b007c289c0011e470cc7f14de4fe9fc80": ("🎯 Strategic #1",      "Smart Money"),
    "0xa19ab9905dc9e4bcb8f982b063710a508b612434": ("🎯 Strategic #2",      "Smart Money"),
    "0xa713fc94db054aa435af4d9c66c3433dca98559f": ("🎯 Strategic #3",      "Smart Money"),
    "0x15bb5d31048381c84a157526cef9513531b8be1e": ("🏛 Inst. Fund",         "Smart Money"),
    "0xeaf4311ee279734facf77d167eec277d8343603e": ("🧠 Smart Holder #1",   "Smart Money"),
    "0x4edb32cfc71e6c404bea8bbbdc8d9b8e03b08235": ("🧠 Smart Holder #2",   "Smart Money"),
    "0xd4d2e6ebca6c94dd28a0935ae468012fdda5d35a": ("🧠 Smart Holder #3",   "Smart Money"),
    "0x682a1ab616f3ff8378392fbe6c8d17826081456f": ("⚡ DeFi Trader",       "Smart Money"),
    "0xd8169f099ce16c87a99d2a8494023574b5eea9c5": ("⚡ High-freq Trader",  "Smart Money"),
    "0x0d4dc3b8becc98782309e443a6da4b9455b5ca48": ("🏦 Bybit",             "CEX"),
    "0x88a1493366d48225fc3cefbdae9ebb23e323ade3": ("🏦 Bybit",             "CEX"),
    "0x588846213a30fd36244e0ae0ebb2374516da836c": ("🏦 Bybit Hot Relay",   "CEX"),
    "0xc868d0ea71243f1580f934cdc59620603bf9f1f1": ("🏦 Bybit Hot Relay",   "CEX"),
    "0x4a67e97e770de93952b8596f04c13ada0ab9a69c": ("🏦 Bybit Relay",       "CEX"),
    "0xb38e8c17e38363af6ebdcb3dae12e0243582891d": ("🔶 Binance",           "CEX"),
    "0x28c6c06298d514db089934071355e5743bf21d60": ("🔶 Binance",           "CEX"),
    "0x2933782b5a8d72f2754103d1489614f29bfa4625": ("🟢 KuCoin",            "CEX"),
    "0x013e138ef6008ae5fdfde29700e3f2bc61d21e3a": ("🦁 Merchant Moe",      "DEX"),
    "0xb9d507990c009ed1ee853a07b6a20c0925dd8a08": ("⛓ Budget L2",          "Protocol"),
    "0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8": ("⛓ WMNT Token",         "Protocol"),
    "0xed884f0460a634c69dbb7def54858465808aacef": ("⛓ Rewards Stn",        "Protocol"),
    "0xcd9dab9fa5b55ee4569edc402d3206123b1285f4": ("⛓ Treasury FF",        "Protocol"),
    "0x94fec56bbeceacc71c9e61623ace9f8e1b1cf473": ("⛓ Treasury L2",        "Protocol"),
    "0x6906d4ac9236849a755d16b38945cdc44dc01d07": ("🔀 Routing Wallet",    "Routing"),
    "0xe6aec6f5b4a21722d2663e0e2bf8cbe4d16c0747": ("📦 Large Sender",      "Potential"),
    "0x193f3520fbc1948d46a4cf37f2d1b13ad6c5ea17": ("📦 Large Accum.",      "Potential"),
    "0x4589ac7bc932b8c8e4ea001d44d40d5e4858b808": ("❓ Unknown",           "Unknown"),
    "0x6d9982a5902227e7d6838f3e5da421de587e94b3": ("📜 DeFi Contract",     "Protocol"),
}


def enrich(row) -> dict:
    d = dict(row)
    try:
        d["tags"] = json.loads(d.get("tags") or "[]")
    except Exception:
        d["tags"] = []
    d.pop("extra", None)

    fa = d.get("from_addr", "").lower()
    ta = d.get("to_addr",   "").lower()
    fl = WALLET_LABELS.get(fa)
    tl = WALLET_LABELS.get(ta)
    d["from_label"] = fl[0] if fl else None
    d["from_cat"]   = fl[1] if fl else None
    d["to_label"]   = tl[0] if tl else None
    d["to_cat"]     = tl[1] if tl else None
    d["scan_url"]   = MANTLESCAN + d.get("tx_hash", "")
    return d


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — Frontend
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES — API
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/alerts")
def api_alerts():
    """
    GET /api/alerts
    Params: limit, type, min_mnt, search, order (time|value)
    """
    limit  = min(int(request.args.get("limit",  50)), 500)
    a_type = request.args.get("type",   "all")
    min_v  = float(request.args.get("min_mnt", 0))
    search = request.args.get("search", "").strip()
    order  = request.args.get("order",  "time")

    q, p, conds = "SELECT * FROM alerts", [], []

    if a_type not in ("", "all"):
        conds.append("type = ?")
        p.append(a_type)
    if min_v > 0:
        conds.append("value_mnt >= ?")
        p.append(min_v)
    if search:
        s = f"%{search.lower()}%"
        conds.append("(LOWER(from_addr) LIKE ? OR LOWER(to_addr) LIKE ? "
                     "OR LOWER(tx_hash) LIKE ?)")
        p += [s, s, s]

    if conds:
        q += " WHERE " + " AND ".join(conds)

    q += " ORDER BY " + ("value_mnt DESC" if order == "value" else "id DESC")
    q += " LIMIT ?";  p.append(limit)

    rows = [enrich(r) for r in get_db().execute(q, p).fetchall()]
    return jsonify({"data": rows, "count": len(rows)})


@app.route("/api/stats")
def api_stats():
    db = get_db()

    tot_cnt, tot_vol, max_vol = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(value_mnt),0), "
        "COALESCE(MAX(value_mnt),0) FROM alerts"
    ).fetchone()

    since24 = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cnt24, vol24 = db.execute(
        "SELECT COUNT(*), COALESCE(SUM(value_mnt),0) "
        "FROM alerts WHERE timestamp >= ?", (since24,)
    ).fetchone()

    by_type = {}
    for r in db.execute(
        "SELECT type, COUNT(*), COALESCE(SUM(value_mnt),0) "
        "FROM alerts GROUP BY type"
    ).fetchall():
        by_type[r[0]] = {"count": r[1], "volume": round(r[2], 2)}

    uniq = db.execute(
        "SELECT COUNT(DISTINCT from_addr) FROM alerts"
    ).fetchone()[0]

    last_row = db.execute(
        "SELECT timestamp FROM alerts ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # Alpha Score из meta (пишется ботом)
    alpha_row   = db.execute(
        "SELECT value FROM meta WHERE key='last_alpha_score'"
    ).fetchone()
    signal_row  = db.execute(
        "SELECT value FROM meta WHERE key='last_alpha_signal'"
    ).fetchone()
    thresh_row  = db.execute(
        "SELECT value FROM meta WHERE key='threshold_mnt'"
    ).fetchone()

    return jsonify({
        "total":          tot_cnt,
        "total_volume":   round(tot_vol, 2),
        "max_volume":     round(max_vol,  2),
        "last_24h":       {"count": cnt24, "volume": round(vol24, 2)},
        "by_type":        by_type,
        "unique_wallets": uniq,
        "last_ts":        last_row[0] if last_row else None,
        "alpha_score":    int(alpha_row[0])  if alpha_row  else None,
        "alpha_signal":   signal_row[0]       if signal_row else None,
        "threshold_mnt":  float(thresh_row[0]) if thresh_row else 50.0,
    })


@app.route("/api/chart/hourly")
def api_chart_hourly():
    """Почасовые данные за 24 ч, разбитые по типу события."""
    since  = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    rows   = get_db().execute("""
        SELECT strftime('%H', timestamp) AS hr,
               type,
               COALESCE(SUM(value_mnt), 0) AS vol,
               COUNT(*)                     AS cnt
        FROM alerts WHERE timestamp >= ?
        GROUP BY hr, type ORDER BY hr
    """, (since,)).fetchall()

    TYPES   = ("transfer", "swap", "cex_inflow", "cex_outflow")
    buckets = {str(h).zfill(2): {t: 0.0 for t in TYPES} | {"cnt": 0}
               for h in range(24)}

    for r in rows:
        hr = str(r["hr"]).zfill(2)
        t  = r["type"] if r["type"] in TYPES else "transfer"
        buckets[hr][t]    += r["vol"]
        buckets[hr]["cnt"] += r["cnt"]

    now_h   = datetime.now(timezone.utc).hour
    ordered = []
    for i in range(24):
        hr = str((now_h - 23 + i) % 24).zfill(2)
        ordered.append({"label": f"{hr}:00", **buckets[hr]})

    return jsonify(ordered)


@app.route("/api/top_wallets")
def api_top_wallets():
    rows = get_db().execute("""
        SELECT from_addr, COUNT(*) cnt, COALESCE(SUM(value_mnt),0) vol
        FROM alerts GROUP BY from_addr ORDER BY vol DESC LIMIT 15
    """).fetchall()

    result = []
    for r in rows:
        info  = WALLET_LABELS.get(r["from_addr"].lower())
        label = info[0] if info else r["from_addr"][:10] + "…"
        cat   = info[1] if info else "Unknown"
        result.append({
            "addr":  r["from_addr"],
            "count": r["cnt"],
            "vol":   round(r["vol"], 2),
            "label": label,
            "cat":   cat,
        })
    return jsonify(result)


@app.route("/api/wallet_spark/<addr>")
def api_wallet_spark(addr: str):
    """
    GET /api/wallet_spark/<addr>?period=24h|3d|7d  (default: 3d)

    Возвращает кумулятивный нетто-поток кошелька по бакетам.
    net_i = inflow_i - outflow_i
    cumulative[i] = sum(net_0 .. net_i)

    Prefix = первые 10 символов addr (без 0x).

    Ответ:
      spark          — массив кумулятивных значений (фикс. длины: 24/36/42)
      first_nonzero  — значение первой ненулевой точки кумулятива (или null)
      last_nonzero   — значение последней ненулевой точки (или null)
      has_data       — true если хотя бы 1 бакет ненулевой по нетто
      first_seen     — unix-timestamp первой транзакции кошелька (или null)
    """
    _EMPTY = {
        "spark": [], "first_nonzero": None, "last_nonzero": None,
        "has_data": False, "first_seen": None,
    }
    try:
        period = request.args.get("period", "3d").strip().lower()

        PERIODS = {
            "24h": {"points": 24, "step_hours": 1},
            "3d":  {"points": 36, "step_hours": 2},
            "7d":  {"points": 42, "step_hours": 4},
        }
        cfg        = PERIODS.get(period, PERIODS["3d"])
        n_points   = cfg["points"]
        step_hours = cfg["step_hours"]

        # Убираем «0x» / «0X» для prefix-матчинга
        clean  = addr[2:] if addr.lower().startswith("0x") else addr
        prefix = clean[:10]
        like   = f"%{prefix}%"

        now = datetime.now(timezone.utc)
        db  = get_db()

        # ── Строим бакеты: inflow - outflow ──────────────────────────────────
        net_vals: list[float] = []
        for i in range(n_points):
            bucket_end   = now - timedelta(hours=step_hours * (n_points - 1 - i))
            bucket_start = bucket_end - timedelta(hours=step_hours)
            ts0 = bucket_start.isoformat()
            ts1 = bucket_end.isoformat()

            inflow_row = db.execute("""
                SELECT COALESCE(SUM(value_mnt), 0) AS vol
                FROM alerts
                WHERE to_addr LIKE ?
                  AND timestamp >= ? AND timestamp < ?
            """, (like, ts0, ts1)).fetchone()

            outflow_row = db.execute("""
                SELECT COALESCE(SUM(value_mnt), 0) AS vol
                FROM alerts
                WHERE from_addr LIKE ?
                  AND timestamp >= ? AND timestamp < ?
            """, (like, ts0, ts1)).fetchone()

            inflow  = float(inflow_row["vol"])  if inflow_row  else 0.0
            outflow = float(outflow_row["vol"]) if outflow_row else 0.0
            net_vals.append(inflow - outflow)

        # ── Кумулятив ────────────────────────────────────────────────────────
        cumulative: list[float] = []
        running = 0.0
        for net in net_vals:
            running += net
            cumulative.append(round(running, 4))

        # ── Метаданные ───────────────────────────────────────────────────────
        has_data = any(v != 0.0 for v in cumulative)

        nonzero_vals = [v for v in cumulative if v != 0.0]
        first_nonzero = nonzero_vals[0]  if nonzero_vals else None
        last_nonzero  = nonzero_vals[-1] if nonzero_vals else None

        # first_seen: самая ранняя транзакция кошелька
        first_row = db.execute("""
            SELECT MIN(timestamp) AS ts FROM alerts
            WHERE from_addr LIKE ? OR to_addr LIKE ?
        """, (like, like)).fetchone()

        first_seen = None
        if first_row and first_row["ts"]:
            try:
                dt = datetime.fromisoformat(
                    first_row["ts"].replace("Z", "+00:00")
                )
                first_seen = int(dt.timestamp())
            except Exception:
                pass

        return jsonify({
            "spark":         cumulative,
            "first_nonzero": first_nonzero,
            "last_nonzero":  last_nonzero,
            "has_data":      has_data,
            "first_seen":    first_seen,
        })

    except Exception as exc:
        app.logger.error("wallet_spark error for %s: %s", addr, exc)
        return jsonify(_EMPTY)


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ensure_db()
    n = migrate_from_json("alerts.json")
    if n:
        print(f"[MIGRATE] alerts.json → SQLite: {n} записей")
    print(f"[WW] http://0.0.0.0:{PORT}  DB={DB_FILE}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
