# 🐋 WhaleWatcher — Mantle Network On-Chain Intelligence

> Built as a **Nansen-equivalent for Mantle Network** — real-time whale tracker with AI signals, Alpha Score, and verifiable prediction accuracy — delivered via Telegram.

**Live bot:** [@whalewatcherhtBot](https://t.me/whalewatcherhtBot) | **Mini App:** [whalewatcher-production-6e1c.up.railway.app](https://whalewatcher-production-6e1c.up.railway.app/)

---

## What it does

WhaleWatcher monitors Mantle Network block-by-block and delivers actionable intelligence directly to Telegram:

- **Block-by-block scanning** — native MNT transfers, DEX swaps, CEX flows with sub-second latency
- **mETH tracking** — Mantle Staked ETH large movements monitored in real time
- **DEX coverage** — Merchant Moe V2, Agni Finance V3, FusionX V3 via dynamic factory contract discovery
- **AI pattern recognition** — Qwen LLM classifies each batch: CEX Deposit Flow, Whale Distribution, Smart Money Accumulation, DEX Demand Spike
- **Alpha Score** — composite 0–100 signal: wallet reputation × volume × market alignment
- **Verifiable accuracy** — every BUY/SELL signal auto-verified against real price at 15m / 30m / 1h / 2h / 4h / 8h / 24h horizons

## Sample alert

```
📊 Alpha Score: 72/100 (📉 SELL)
─────────────────
Signal: SELL — Mega Whale distributing via CEX relay
Pattern: CEX Deposit Flow
Volume: 33,940 MNT (~$18,454)
Key actor: 0x0000004e [Mega Whale, OTC Distributor]
Flag: Chunk Splitting Detected — automated distribution
─────────────────
👤 0x0000004e [Mega Whale][OTC Distributor][Bybit-funded]
  🔔 Transfer 11,278 MNT → Personal Relay
```

---

## Architecture

```
Mantle RPC (WebSocket) ──► monitor_blocks()
                                │
                                ▼
                    transaction buffer (1-min window)
                                │
                                ▼
                       aggregate_and_send()
                          │           │
                          ▼           ▼
                      Alpha Score   Qwen AI
                       (0-100)   (BUY/SELL/WATCH)
                          │           │
                          └─────┬─────┘
                                ▼
                Telegram alert + prediction saved to SQLite
                                │
                                ▼
                    /accuracy — auto-verified vs real price
```

**Stack:** Python 3.11 · aiogram 3.x · web3.py 6.x · SQLite WAL · Qwen AI (OpenRouter) · Flask REST API · Railway · Telegram Mini App · Solidity (WhaleLabelRegistry on Mantle)

---

## On-Chain Component

**WhaleLabelRegistry** — smart contract deployed on Mantle Sepolia:
[`0x67c77F393230EDBf75454C66A742D3F8d7Bc6E46`](https://explorer.sepolia.mantle.xyz/address/0x67c77F393230EDBf75454C66A742D3F8d7Bc6E46)

Stores 500+ whale wallet labels on-chain. Queryable by any dApp:
- `getLabel(address)` — get label for a single wallet
- `getLabels(address[])` — batch query
- `totalLabeled()` — total labeled wallets count

This brings WhaleWatcher's proprietary wallet intelligence layer fully on-chain — transparent, verifiable, and composable with other Mantle protocols.

---

## Mini App

4-tab interface accessible directly from Telegram:

- **Dashboard** — live MNT price, TradingView chart, Alpha Score, market activity
- **Wallets** — top whale wallets with Alpha Score, flow bars, accuracy %
- **Paper Trading** — copy-trading simulation against MNT Hold baseline
- **Signals** — AI signal feed with BUY/SELL/WATCH badges

Supports **English and Russian** interface.

---

## Bot commands

| Command | Access |
|---------|--------|
| `/start` | Everyone |
| `/stats` | Everyone |
| `/top_whales` | Everyone |
| `/alpha` | Everyone |
| `/accuracy` | Everyone |
| `/set_threshold N` | Admin |
| `/help` | Everyone |

---

## Freemium model

- **Free** — real-time alerts, stats, /accuracy public
- **PRO** ($29/month) — full accuracy breakdown by 7 horizons, top-10 wallet history, custom alert thresholds
- **Enterprise** ($299/month) — REST API access, custom wallet watchlists, webhook integration

**TAM:** 50,000+ active Mantle DeFi users
**CAC:** ~$0 (Telegram = zero acquisition cost)

---

## Wallet tagging system

500+ manually curated addresses — a proprietary data layer built over weeks of on-chain analysis:

`Mega Whale` · `OTC Distributor` · `Smart Money` · `Bybit Hot Relay` · `Personal Relay` · `Routing Wallet` · `High-frequency` · `Whale Cold Storage` · `Accumulator` · `CEX` (Bybit / OKX / KuCoin / Binance)

All labels are now stored on-chain via WhaleLabelRegistry — permanently verifiable and composable.

---

## Traction

- **550+ verified predictions** accumulated since launch
- **72% peak accuracy on 24h horizon** (89/123 predictions) — verified against real MNT price
- Accuracy tracked across 7 time horizons (15m → 24h) via public `/accuracy` command
- Live on Mantle mainnet — real data, not synthetic

---

## Roadmap

- mETH yield tracking
- MantleScan API enrichment
- Per-user alert thresholds (PRO)
- Historical backtesting UI in Mini App
- cmETH (Mantle Restaked ETH) monitoring
- WhaleLabelRegistry migration to Mantle mainnet

---

## Team

Three students from Tyumen, Russia — Python, backend, data.

Built for [The Turing Test Hackathon 2026](https://dorahacks.io/hackathon/mantleturingtesthackathon2026) · Track: AI Alpha & Data
