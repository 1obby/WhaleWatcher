# 🐋 WhaleWatcher — Mantle Network On-Chain Intelligence

> Real-time whale tracker with AI signals, Alpha Score, and verifiable prediction accuracy — delivered via Telegram.

**Live demo:** [@whalewatcherhtBot](https://t.me/whalewatcherhtBot)  
**Web dashboard:** [whalewatcher-production-6e1c.up.railway.app](https://whalewatcher-production-6e1c.up.railway.app)

---

## What it does

WhaleWatcher monitors Mantle Network in real time and delivers actionable intelligence directly to Telegram:

- **Block-by-block scanning** — native MNT transfers, DEX swaps, CEX flows processed with sub-second latency
- **AI pattern recognition** — Qwen LLM classifies each transaction batch: CEX Deposit Flow, Whale Distribution, Smart Money Accumulation, DEX Demand Spike, and more
- **Alpha Score** — composite 0–100 signal per batch: wallet reputation × volume × market alignment
- **Prediction verification** — every BUY/SELL signal is stored and automatically checked against real price at 15m / 30m / 1h / 2h / 4h / 8h / 24h

## Sample alert

```
📊 Alpha Score: 72/100 (📉 sell)

─────────────────
**Signal: SELL** — Mega Whale distributing via CEX relay
**Pattern:** CEX Deposit Flow
**Volume:** 33,940 MNT (~$18,454)
**Key actor:** 0x0000004e [Mega Whale, OTC Distributor]
**Flag:** Chunk Splitting Detected — automated distribution
─────────────────

👤 0x0000004e [Mega Whale][OTC Distributor][Bybit-funded]
  🔔 Transfer 11,278 MNT → Personal Relay [tx]
```

## Data sources

| Source | What it provides |
|---|---|
| Mantle RPC (direct) | Native transfers, block-by-block |
| Merchant Moe V2 | DEX swap logs, buy/sell direction |
| Agni Finance V3 | DEX swap logs, dynamic pool discovery via factory |
| CoinGecko API | MNT price + 24h change for market context |
| 500+ labelled wallets | CEX hot wallets, Smart Money, OTC clusters, Mega Whale |

## Architecture

```
Mantle RPC ──► monitor_blocks()
                    │
                    ▼
              transaction buffer (1-min window)
                    │
                    ▼
           aggregate_and_send()
              │          │
              ▼          ▼
          Alpha Score  Qwen AI
          (0-100)    (BUY/SELL/WATCH)
             │            │
             └──────┬─────┘
                    ▼
     Telegram alert + prediction saved to SQLite
```

**Stack:** Python 3.12 · aiogram 3.x · web3.py 6.x · SQLite WAL · Qwen AI (ModelScope) · Flask · Railway

## Commands

| Command | Access |
|---|---|
| `/start` | Everyone |
| `/stats` | Free (top 3) / PRO (full 24h) |
| `/top_whales` | Free (top 3) / PRO (top 10) |
| `/alpha` | Everyone |
| `/accuracy` | PRO only |
| `/set_threshold N` | Admin only |
| `/help` | Everyone |
| `/pro` | Everyone |

## Freemium

- **Free** — real-time alerts, basic stats (top 3)
- **PRO** — $29/month — full stats, top-10 wallets, AI prediction accuracy

## Wallet tagging system

500+ manually curated addresses across categories:

`Mega Whale` · `OTC Distributor` · `Smart Money` · `Bybit Hot Relay` · `Personal Relay` · `Routing Wallet` · `High-frequency` · `Whale Cold Storage` · `Accumulator` · `CEX` (Bybit / OKX / KuCoin / Binance)

## Roadmap

- [ ] FusionX DEX integration
- [ ] mETH and fBTC tracking
- [ ] MantleScan API enrichment
- [ ] Per-user alert thresholds (PRO)
- [ ] Historical backtesting for /accuracy

## Team

Three students from Tyumen, Russia — Python, backend, data.

Built for [The Turing Test Hackathon 2026](https://dorahacks.io/hackathon/mantleturingtesthackathon2026) · Track: AI Alpha & Data
