#!/usr/bin/env python3
"""
Wave Rider v7 — Solana Memecoin Graduation Scanner
====================================================
Detects Pump.fun graduations, monitors price action across tiers,
generates simulated entry/exit alerts. Dashboard at /.

Deploy: gunicorn app_v7:app --workers 1 --threads 2 --bind 0.0.0.0:$PORT
"""

VERSION = "v7-waverider"

import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.environ.get("DB_PATH", "waverider.db")
HELIUS_KEY = os.environ.get("HELIUS_API_KEY", "b85d5357-36d9-4e26-b945-f38a0677b391")
SIM_MODE = os.environ.get("SIM_MODE", "1") == "1"  # simulation by default

PUMP_LIVE_URL = "https://frontend-api-v3.pump.fun/coins/currently-live?limit=100&includeNsfw=false"
DEXSCREENER_BATCH = "https://api.dexscreener.com/latest/dex/tokens/{}"
RUGCHECK_URL = "https://api.rugcheck.xyz/v1/tokens/{}/report/summary"
COINGECKO_SOL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
FNG_URL = "https://api.alternative.me/fng/?limit=1"
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
HELIUS_TX = "https://api.helius.xyz/v0/addresses/{}/transactions?api-key=" + HELIUS_KEY + "&type=SWAP"

# Tier limits
MAX_HOT = 5
MAX_WARM = 20
COLLECTION_BATCH = 30

# ---------------------------------------------------------------------------
# Logging (ring buffer for /logs)
# ---------------------------------------------------------------------------
log_buf = deque(maxlen=500)

class BufHandler(logging.Handler):
    def emit(self, record):
        log_buf.append(self.format(record))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_bh = BufHandler()
_bh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().addHandler(_bh)
log = logging.getLogger("waverider")

# ---------------------------------------------------------------------------
# DB helpers — EVERY db operation goes through `with db() as conn:`
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

@contextmanager
def db():
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS pre_graduation (
            address TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            mcap REAL,
            num_participants INTEGER,
            reply_count INTEGER,
            curve_pct REAL,
            velocity REAL,
            first_seen REAL,
            last_seen REAL,
            graduated INTEGER DEFAULT 0,
            data_json TEXT
        );
        CREATE TABLE IF NOT EXISTS tokens (
            address TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            detected_at REAL,
            tier TEXT DEFAULT 'COLLECTION',
            mint_revoked INTEGER DEFAULT -1,
            peak_price REAL DEFAULT 0,
            peak_mcap REAL DEFAULT 0,
            entry_price REAL,
            entry_time REAL,
            status TEXT DEFAULT 'watching',
            last_update REAL,
            data_json TEXT
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            ts REAL,
            price REAL,
            volume_m5 REAL,
            buys_m5 INTEGER,
            sells_m5 INTEGER,
            liquidity REAL,
            fdv REAL,
            mcap REAL,
            price_change_m5 REAL,
            data_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_snap_addr_ts ON snapshots(address, ts);
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            ts REAL,
            signature TEXT,
            trade_type TEXT,
            sol_amount REAL,
            token_amount REAL,
            wallet TEXT,
            data_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trades_addr ON trades(address, ts);
        CREATE TABLE IF NOT EXISTS holders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            ts REAL,
            rank INTEGER,
            holder_address TEXT,
            amount REAL,
            pct REAL
        );
        CREATE INDEX IF NOT EXISTS idx_holders_addr ON holders(address, ts);
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL,
            address TEXT,
            symbol TEXT,
            alert_type TEXT,
            message TEXT,
            sim INTEGER DEFAULT 1,
            data_json TEXT
        );
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            symbol TEXT,
            entry_price REAL,
            entry_time REAL,
            exit_price REAL,
            exit_time REAL,
            exit_reason TEXT,
            pnl_pct REAL,
            sim INTEGER DEFAULT 1
        );
        """)
    log.info("DB initialised")

# ---------------------------------------------------------------------------
# HTTP fetch — capped at 500KB, 5s timeout
# ---------------------------------------------------------------------------

def fetch_json(url):
    try:
        req = Request(url, headers={"User-Agent": "WaveRider/7.0"})
        resp = urlopen(req, timeout=5)
        data = resp.read(500_000)
        return json.loads(data)
    except Exception:
        return None

def post_json(url, payload):
    """POST JSON, return parsed response."""
    try:
        body = json.dumps(payload).encode()
        req = Request(url, data=body, headers={
            "User-Agent": "WaveRider/7.0",
            "Content-Type": "application/json",
        })
        resp = urlopen(req, timeout=5)
        data = resp.read(500_000)
        return json.loads(data)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Global state (in-memory, rebuilt from DB on restart)
# ---------------------------------------------------------------------------

class State:
    sol_price = 0.0
    fng_value = 50
    loop_ts = 0.0           # last loop heartbeat
    loop_count = 0
    started = False
    watchlist = {}           # address -> pre_graduation row dict
    tokens = {}              # address -> token row dict
    tiers = {"HOT": set(), "WARM": set(), "COLLECTION": set()}

S = State()

# ---------------------------------------------------------------------------
# SOL price (CoinGecko)
# ---------------------------------------------------------------------------

def refresh_sol_price():
    d = fetch_json(COINGECKO_SOL)
    if d and "solana" in d:
        S.sol_price = d["solana"].get("usd", 0)

def refresh_fng():
    d = fetch_json(FNG_URL)
    if d and "data" in d and len(d["data"]):
        try:
            S.fng_value = int(d["data"][0]["value"])
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Step 1: Pre-graduation scan (Pump.fun)
# ---------------------------------------------------------------------------

def scan_pumpfun():
    """Fetch currently-live tokens, track velocity, detect graduations."""
    d = fetch_json(PUMP_LIVE_URL)
    if not d or not isinstance(d, list):
        return
    now = time.time()
    graduated = []
    with db() as conn:
        for t in d:
            addr = t.get("mint", "")
            if not addr:
                continue
            sym = t.get("symbol", "???")
            name = t.get("name", "")
            mcap = float(t.get("usd_market_cap", 0) or 0)
            participants = int(t.get("num_participants", 0) or 0)
            replies = int(t.get("reply_count", 0) or 0)
            complete = t.get("complete", False)
            curve_pct = float(t.get("bonding_curve_progress", 0) or 0)

            prev = S.watchlist.get(addr)
            if prev:
                dt = now - prev.get("last_seen", now)
                if dt > 0:
                    vel = (mcap - prev.get("mcap", 0)) / max(dt, 0.1)
                else:
                    vel = 0
            else:
                vel = 0

            row = {
                "address": addr, "symbol": sym, "name": name,
                "mcap": mcap, "num_participants": participants,
                "reply_count": replies, "curve_pct": curve_pct,
                "velocity": vel, "first_seen": prev["first_seen"] if prev else now,
                "last_seen": now, "graduated": 1 if complete else 0,
            }
            S.watchlist[addr] = row

            conn.execute("""
                INSERT INTO pre_graduation (address, symbol, name, mcap, num_participants,
                    reply_count, curve_pct, velocity, first_seen, last_seen, graduated, data_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(address) DO UPDATE SET
                    mcap=excluded.mcap, num_participants=excluded.num_participants,
                    reply_count=excluded.reply_count, curve_pct=excluded.curve_pct,
                    velocity=excluded.velocity, last_seen=excluded.last_seen,
                    graduated=excluded.graduated, data_json=excluded.data_json
            """, (addr, sym, name, mcap, participants, replies, curve_pct,
                  vel, row["first_seen"], now, row["graduated"],
                  json.dumps({"raw_keys": list(t.keys())})))

            if complete and addr not in S.tokens:
                graduated.append(row)

    for g in graduated:
        on_graduation(g)

# ---------------------------------------------------------------------------
# Step 2: Graduation handling
# ---------------------------------------------------------------------------

def on_graduation(pre):
    addr = pre["address"]
    now = time.time()
    log.info("GRADUATED: %s (%s) mcap=$%.0f", pre["symbol"], addr[:8], pre["mcap"])
    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO tokens (address, symbol, name, detected_at, tier, status, last_update)
            VALUES (?,?,?,?,'COLLECTION','watching',?)
        """, (addr, pre["symbol"], pre["name"], now, now))
    S.tokens[addr] = {
        "address": addr, "symbol": pre["symbol"], "name": pre["name"],
        "detected_at": now, "tier": "COLLECTION", "mint_revoked": -1,
        "peak_price": 0, "peak_mcap": 0, "entry_price": None,
        "entry_time": None, "status": "watching", "last_update": now,
    }
    S.tiers["COLLECTION"].add(addr)
    # Kick off contract check in background
    threading.Thread(target=check_contract, args=(addr,), daemon=True).start()

# ---------------------------------------------------------------------------
# Step 3: Contract check (RugCheck)
# ---------------------------------------------------------------------------

def check_contract(addr):
    d = fetch_json(RUGCHECK_URL.format(addr))
    revoked = -1
    if d:
        # RugCheck returns risks array; check for mint authority
        risks = d.get("risks", [])
        revoked = 1  # assume good
        for r in risks:
            if "mint" in r.get("name", "").lower() and r.get("level", "") in ("danger", "warn"):
                revoked = 0
                break
    with db() as conn:
        conn.execute("UPDATE tokens SET mint_revoked=? WHERE address=?", (revoked, addr))
    if addr in S.tokens:
        S.tokens[addr]["mint_revoked"] = revoked
    log.info("RugCheck %s: mint_revoked=%d", addr[:8], revoked)

# ---------------------------------------------------------------------------
# Step 4: Live monitoring — DexScreener batch
# ---------------------------------------------------------------------------

def monitor_collection():
    """Batch-query DexScreener for COLLECTION tier tokens."""
    addrs = list(S.tiers.get("COLLECTION", set()))
    if not addrs:
        return
    now = time.time()
    # batch in groups of 30
    for i in range(0, len(addrs), COLLECTION_BATCH):
        batch = addrs[i:i + COLLECTION_BATCH]
        url = DEXSCREENER_BATCH.format(",".join(batch))
        d = fetch_json(url)
        if not d:
            continue
        pairs = d if isinstance(d, list) else d.get("pairs", d.get("pair", []))
        if not isinstance(pairs, list):
            pairs = [pairs] if pairs else []
        _process_dex_pairs(pairs, now)

def monitor_warm():
    """Individual DexScreener queries for WARM tier."""
    for addr in list(S.tiers.get("WARM", set())):
        url = DEXSCREENER_BATCH.format(addr)
        d = fetch_json(url)
        if not d:
            continue
        pairs = d if isinstance(d, list) else d.get("pairs", d.get("pair", []))
        if not isinstance(pairs, list):
            pairs = [pairs] if pairs else []
        _process_dex_pairs(pairs, time.time())

def monitor_hot():
    """DexScreener + Helius for HOT tier."""
    for addr in list(S.tiers.get("HOT", set())):
        url = DEXSCREENER_BATCH.format(addr)
        d = fetch_json(url)
        if not d:
            continue
        pairs = d if isinstance(d, list) else d.get("pairs", d.get("pair", []))
        if not isinstance(pairs, list):
            pairs = [pairs] if pairs else []
        _process_dex_pairs(pairs, time.time())

def _process_dex_pairs(pairs, now):
    """Store snapshots from DexScreener pair data and update token state."""
    with db() as conn:
        for p in pairs:
            if not isinstance(p, dict):
                continue
            addr = p.get("baseToken", {}).get("address", "")
            if not addr or addr not in S.tokens:
                continue
            price = _float(p.get("priceUsd"))
            vol5 = _float(p.get("volume", {}).get("m5"))
            txns = p.get("txns", {}).get("m5", {})
            buys = int(txns.get("buys", 0))
            sells = int(txns.get("sells", 0))
            liq = _float(p.get("liquidity", {}).get("usd"))
            fdv = _float(p.get("fdv"))
            mcap = _float(p.get("marketCap") or p.get("mcap"))
            pc5 = _float(p.get("priceChange", {}).get("m5"))

            conn.execute("""
                INSERT INTO snapshots (address,ts,price,volume_m5,buys_m5,sells_m5,
                    liquidity,fdv,mcap,price_change_m5)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (addr, now, price, vol5, buys, sells, liq, fdv, mcap, pc5))

            tok = S.tokens[addr]
            tok["last_update"] = now
            if price and price > tok.get("peak_price", 0):
                tok["peak_price"] = price
            if mcap and mcap > tok.get("peak_mcap", 0):
                tok["peak_mcap"] = mcap

            conn.execute("UPDATE tokens SET peak_price=?, peak_mcap=?, last_update=? WHERE address=?",
                         (tok["peak_price"], tok["peak_mcap"], now, addr))

            # Evaluate tier promotion / entry / exit
            _evaluate_token(conn, addr, now)

def _float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0

# ---------------------------------------------------------------------------
# Step 4b: Helius Enhanced Transactions (WARM + HOT)
# ---------------------------------------------------------------------------

def fetch_helius_trades(addr):
    url = HELIUS_TX.format(addr)
    d = fetch_json(url)
    if not d or not isinstance(d, list):
        return
    now = time.time()
    with db() as conn:
        for tx in d[:50]:  # cap per batch
            sig = tx.get("signature", "")
            ts = tx.get("timestamp", now)
            ttype = tx.get("type", "SWAP")
            # Parse token transfers for buy/sell classification
            native_transfers = tx.get("nativeTransfers", [])
            token_transfers = tx.get("tokenTransfers", [])
            sol_amt = 0
            tok_amt = 0
            wallet = ""
            direction = "unknown"
            for nt in native_transfers:
                sol_amt += abs(float(nt.get("amount", 0))) / 1e9
            for tt in token_transfers:
                if tt.get("mint", "") == addr:
                    tok_amt += abs(float(tt.get("tokenAmount", 0)))
                    if tt.get("toUserAccount", ""):
                        wallet = tt["toUserAccount"]
                        direction = "buy"
                    elif tt.get("fromUserAccount", ""):
                        wallet = tt["fromUserAccount"]
                        direction = "sell"
            conn.execute("""
                INSERT OR IGNORE INTO trades (address,ts,signature,trade_type,sol_amount,token_amount,wallet)
                VALUES (?,?,?,?,?,?,?)
            """, (addr, ts, sig, direction, sol_amt, tok_amt, wallet))

def fetch_helius_holders(addr):
    """Get top token holders via Helius RPC getTokenLargestAccounts."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [addr]
    }
    d = post_json(HELIUS_RPC, payload)
    if not d or "result" not in d:
        return
    now = time.time()
    accounts = d["result"].get("value", [])
    with db() as conn:
        for i, acct in enumerate(accounts[:20]):
            amt_str = acct.get("amount", "0")
            ui_amt = float(acct.get("uiAmount", 0) or 0)
            holder_addr = acct.get("address", "")
            conn.execute("""
                INSERT INTO holders (address, ts, rank, holder_address, amount, pct)
                VALUES (?,?,?,?,?,?)
            """, (addr, now, i + 1, holder_addr, ui_amt, 0))

# ---------------------------------------------------------------------------
# Step 5 & 6: Entry / Exit evaluation
# ---------------------------------------------------------------------------

def _evaluate_token(conn, addr, now):
    tok = S.tokens.get(addr)
    if not tok:
        return
    age = now - tok["detected_at"]
    status = tok["status"]

    # Fetch recent snapshots for analysis
    rows = conn.execute("""
        SELECT * FROM snapshots WHERE address=? ORDER BY ts DESC LIMIT 10
    """, (addr,)).fetchall()
    if len(rows) < 2:
        return

    latest = dict(rows[0])
    prev = dict(rows[1])

    price = latest.get("price", 0) or 0
    prev_price = prev.get("price", 0) or 0
    vol = latest.get("volume_m5", 0) or 0
    prev_vol = prev.get("volume_m5", 0) or 0
    buys = latest.get("buys_m5", 0) or 0
    sells = latest.get("sells_m5", 0) or 0
    liq = latest.get("liquidity", 0) or 0

    # --- Tier promotion ---
    current_tier = tok.get("tier", "COLLECTION")

    if current_tier == "COLLECTION":
        # Promote to WARM if volume spike or buy surge
        if vol > 0 and prev_vol > 0 and vol > prev_vol * 1.5 and buys > sells:
            _set_tier(conn, addr, "WARM")
        elif buys > 10 and buys > sells * 1.5:
            _set_tier(conn, addr, "WARM")
    elif current_tier == "WARM":
        # Promote to HOT if wave pattern
        if vol > 0 and prev_vol > 0 and vol > prev_vol * 1.3 and buys > sells and price > prev_price:
            if len(S.tiers["HOT"]) < MAX_HOT:
                _set_tier(conn, addr, "HOT")
        # Demote back if dead
        if age > 120 and vol == 0 and buys == 0:
            _set_tier(conn, addr, "COLLECTION")

    # --- Entry logic (Step 5) ---
    if status == "watching" and 5 <= age <= 60:
        mint_ok = tok.get("mint_revoked", -1) == 1
        vol_accel = vol > prev_vol * 1.2 if prev_vol > 0 else vol > 0
        buy_dominant = buys > sells and buys > 0
        price_up = price > prev_price if prev_price > 0 else False

        if mint_ok and vol_accel and buy_dominant and price_up:
            _signal_entry(conn, addr, tok, price, now)

    # --- Exit logic (Step 6) ---
    if status == "entered":
        _check_exits(conn, addr, tok, latest, rows, now)

def _set_tier(conn, addr, new_tier):
    tok = S.tokens.get(addr)
    if not tok:
        return
    old = tok["tier"]
    if old == new_tier:
        return
    S.tiers.get(old, set()).discard(addr)
    S.tiers.setdefault(new_tier, set()).add(addr)
    tok["tier"] = new_tier
    conn.execute("UPDATE tokens SET tier=? WHERE address=?", (new_tier, addr))
    log.info("TIER %s: %s → %s (%s)", tok["symbol"], old, new_tier, addr[:8])

def _signal_entry(conn, addr, tok, price, now):
    tok["status"] = "entered"
    tok["entry_price"] = price
    tok["entry_time"] = now
    conn.execute("UPDATE tokens SET status='entered', entry_price=?, entry_time=? WHERE address=?",
                 (price, now, addr))
    _set_tier(conn, addr, "HOT")

    msg = (f"🏄 BUY SIGNAL: {tok['symbol']} @ ${price:.8f} | "
           f"Photon: https://photon-sol.tinyastro.io/en/lp/{addr}")
    _create_alert(conn, addr, tok["symbol"], "BUY", msg, now)

def _check_exits(conn, addr, tok, latest, rows, now):
    price = latest.get("price", 0) or 0
    entry_price = tok.get("entry_price") or 0
    peak = tok.get("peak_price") or price
    entry_time = tok.get("entry_time") or now
    buys = latest.get("buys_m5", 0) or 0
    sells = latest.get("sells_m5", 0) or 0
    vol = latest.get("volume_m5", 0) or 0
    liq = latest.get("liquidity", 0) or 0
    age = now - entry_time

    exit_reason = None

    # 1. Price drop 8% from peak
    if peak > 0 and price < peak * 0.92:
        exit_reason = "PRICE_DROP_8PCT"
    # 2. Buy/sell ratio < 0.7 for 2 checks
    elif sells > 0 and (buys / max(sells, 1)) < 0.7:
        if len(rows) >= 3:
            prev_snap = dict(rows[1])
            pb = prev_snap.get("buys_m5", 0) or 0
            ps = prev_snap.get("sells_m5", 0) or 0
            if ps > 0 and (pb / max(ps, 1)) < 0.7:
                exit_reason = "BUY_SELL_RATIO"
    # 3. Volume death (60% drop from what we've seen)
    if not exit_reason and len(rows) >= 5:
        peak_vol = max((dict(r).get("volume_m5", 0) or 0) for r in rows[:5])
        if peak_vol > 0 and vol < peak_vol * 0.4:
            exit_reason = "VOLUME_DEATH"
    # 4. Top holder selling — checked separately via Helius
    # 5. +100% gain → partial
    if not exit_reason and entry_price > 0 and price >= entry_price * 2:
        exit_reason = "TAKE_PROFIT_100PCT"
    # 6. 5-min time stop
    if not exit_reason and age > 300:
        # Still allow if clearly building
        if vol > 0 and buys > sells:
            pass  # still building
        else:
            exit_reason = "TIME_STOP_5MIN"
    # 7. Liquidity drop
    if not exit_reason and liq > 0 and liq < 5000:
        exit_reason = "LIQUIDITY_DROP"

    if exit_reason:
        pnl = ((price - entry_price) / entry_price * 100) if entry_price > 0 else 0
        tok["status"] = "exited"
        conn.execute("UPDATE tokens SET status='exited' WHERE address=?", (addr,))
        conn.execute("""
            INSERT INTO positions (address, symbol, entry_price, entry_time,
                exit_price, exit_time, exit_reason, pnl_pct, sim)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (addr, tok["symbol"], entry_price, entry_time, price, now,
              exit_reason, pnl, 1 if SIM_MODE else 0))

        msg = (f"🚪 EXIT: {tok['symbol']} | Reason: {exit_reason} | "
               f"Entry: ${entry_price:.8f} → ${price:.8f} | P&L: {pnl:+.1f}%")
        _create_alert(conn, addr, tok["symbol"], "EXIT", msg, now)
        # Demote
        _set_tier(conn, addr, "COLLECTION")

def _create_alert(conn, addr, symbol, atype, msg, now):
    sim = 1 if SIM_MODE else 0
    conn.execute("""
        INSERT INTO alerts (ts, address, symbol, alert_type, message, sim)
        VALUES (?,?,?,?,?,?)
    """, (now, addr, symbol, atype, msg, sim))
    log.info("ALERT [%s%s]: %s", "SIM " if sim else "", atype, msg)

# ---------------------------------------------------------------------------
# Tier cleanup — expire old tokens
# ---------------------------------------------------------------------------

def cleanup_tiers():
    now = time.time()
    with db() as conn:
        for addr in list(S.tokens):
            tok = S.tokens[addr]
            age = now - tok["detected_at"]
            if age > 600 and tok["status"] in ("watching", "exited"):
                # 10 min+ and not active → remove from active tracking
                for tier_set in S.tiers.values():
                    tier_set.discard(addr)
                # Keep in DB, remove from memory
                if tok["status"] == "watching":
                    conn.execute("UPDATE tokens SET status='expired' WHERE address=?", (addr,))
        # Also trim watchlist memory (keep last 500)
        if len(S.watchlist) > 500:
            sorted_wl = sorted(S.watchlist.items(), key=lambda x: x[1]["last_seen"])
            for k, _ in sorted_wl[:len(S.watchlist) - 500]:
                del S.watchlist[k]

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

_loop_lock = threading.Lock()

def main_loop():
    log.info("Main loop started")
    tick = 0
    while True:
        try:
            S.loop_ts = time.time()
            S.loop_count += 1
            tick += 1

            # Every tick (~3s): HOT tier
            monitor_hot()

            # Every 2 ticks (~6s): WARM tier + Pump.fun scan
            if tick % 2 == 0:
                scan_pumpfun()
                monitor_warm()

            # Every 5 ticks (~15s): COLLECTION batch
            if tick % 5 == 0:
                monitor_collection()

            # Every 10 ticks (~30s): SOL price + Helius trades for warm/hot
            if tick % 10 == 0:
                refresh_sol_price()
                for addr in list(S.tiers.get("WARM", set())) | list(S.tiers.get("HOT", set())):
                    try:
                        fetch_helius_trades(addr)
                    except Exception as e:
                        log.warning("Helius trades error %s: %s", addr[:8], e)

            # Every 10 ticks: Helius holders for HOT
            if tick % 10 == 0:
                for addr in list(S.tiers.get("HOT", set())):
                    try:
                        fetch_helius_holders(addr)
                    except Exception as e:
                        log.warning("Helius holders error %s: %s", addr[:8], e)

            # Every 20 ticks (~60s): cleanup + FNG
            if tick % 20 == 0:
                cleanup_tiers()
                refresh_fng()

            time.sleep(3)
        except Exception as e:
            log.error("Loop error: %s", e, exc_info=True)
            time.sleep(5)

def start_all():
    if S.started:
        return
    S.started = True
    init_db()
    refresh_sol_price()
    refresh_fng()
    # Restore tokens from DB
    with db() as conn:
        rows = conn.execute("SELECT * FROM tokens WHERE status IN ('watching','entered')").fetchall()
        for r in rows:
            d = dict(r)
            addr = d["address"]
            S.tokens[addr] = d
            tier = d.get("tier", "COLLECTION")
            S.tiers.setdefault(tier, set()).add(addr)
    log.info("Restored %d tokens from DB", len(S.tokens))
    t = threading.Thread(target=main_loop, daemon=True)
    t.start()
    log.info("Wave Rider %s started (SIM_MODE=%s, SOL=$%.2f, FNG=%d)",
             VERSION, SIM_MODE, S.sol_price, S.fng_value)

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)

_started = False

@app.before_request
def _ensure_started():
    global _started
    if not _started:
        _started = True
        start_all()

# --- Dashboard ---
@app.route("/")
def index():
    now = time.time()
    loop_age = now - S.loop_ts if S.loop_ts else 999
    hot = len(S.tiers.get("HOT", set()))
    warm = len(S.tiers.get("WARM", set()))
    coll = len(S.tiers.get("COLLECTION", set()))
    watchlist_n = len(S.watchlist)

    with db() as conn:
        alert_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        position_rows = conn.execute(
            "SELECT * FROM positions ORDER BY exit_time DESC LIMIT 20"
        ).fetchall()
        recent_alerts = conn.execute(
            "SELECT * FROM alerts ORDER BY ts DESC LIMIT 10"
        ).fetchall()

    positions_html = ""
    total_pnl = 0
    wins = 0
    losses = 0
    for p in position_rows:
        p = dict(p)
        pnl = p.get("pnl_pct", 0) or 0
        total_pnl += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        color = "#4f4" if pnl > 0 else "#f44" if pnl < 0 else "#aaa"
        positions_html += (
            f"<tr><td>{p.get('symbol','?')}</td>"
            f"<td>${p.get('entry_price',0):.8f}</td>"
            f"<td>${p.get('exit_price',0):.8f}</td>"
            f"<td style='color:{color}'>{pnl:+.1f}%</td>"
            f"<td>{p.get('exit_reason','')}</td>"
            f"<td>{'SIM' if p.get('sim') else 'LIVE'}</td></tr>"
        )

    alerts_html = ""
    for a in recent_alerts:
        a = dict(a)
        ts_str = datetime.fromtimestamp(a.get("ts", 0), tz=timezone.utc).strftime("%H:%M:%S")
        alerts_html += f"<tr><td>{ts_str}</td><td>{a.get('alert_type','')}</td><td>{a.get('message','')}</td></tr>"

    status_color = "#4f4" if loop_age < 15 else "#fa0" if loop_age < 30 else "#f44"
    sim_badge = '<span style="background:#fa0;color:#000;padding:2px 8px;border-radius:4px;">SIMULATION</span>' if SIM_MODE else '<span style="background:#4f4;color:#000;padding:2px 8px;border-radius:4px;">LIVE</span>'

    html = f"""<!DOCTYPE html><html><head><title>Wave Rider {VERSION}</title>
    <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <meta http-equiv="refresh" content="10">
    <style>
        body {{ background:#111;color:#eee;font-family:monospace;margin:20px; }}
        h1 {{ color:#0ff; }}
        .card {{ background:#222;border-radius:8px;padding:15px;margin:10px 0;display:inline-block;min-width:150px;text-align:center; }}
        .card h3 {{ margin:0;color:#888;font-size:12px; }}
        .card .val {{ font-size:28px;color:#0ff;margin:5px 0; }}
        table {{ border-collapse:collapse;width:100%;margin:10px 0; }}
        th,td {{ padding:6px 10px;border:1px solid #333;text-align:left;font-size:13px; }}
        th {{ background:#333; }}
        a {{ color:#0ff; }}
    </style></head><body>
    <h1>🏄 Wave Rider {VERSION} {sim_badge}</h1>
    <div>
        <div class="card"><h3>Loop</h3><div class="val" style="color:{status_color}">{"●" if loop_age<15 else "○"} {loop_age:.0f}s</div></div>
        <div class="card"><h3>SOL</h3><div class="val">${S.sol_price:.2f}</div></div>
        <div class="card"><h3>FNG</h3><div class="val">{S.fng_value}</div></div>
        <div class="card"><h3>🔥 HOT</h3><div class="val">{hot}</div></div>
        <div class="card"><h3>🌡 WARM</h3><div class="val">{warm}</div></div>
        <div class="card"><h3>📦 COLL</h3><div class="val">{coll}</div></div>
        <div class="card"><h3>👀 Watch</h3><div class="val">{watchlist_n}</div></div>
        <div class="card"><h3>Alerts</h3><div class="val">{alert_count}</div></div>
    </div>
    <h2>Recent Alerts</h2>
    <table><tr><th>Time</th><th>Type</th><th>Message</th></tr>{alerts_html}</table>
    <h2>Positions (last 20)</h2>
    <table><tr><th>Token</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Reason</th><th>Mode</th></tr>{positions_html}</table>
    <p>Win/Loss: {wins}/{losses} | Total P&L: {total_pnl:+.1f}% | Ticks: {S.loop_count}</p>
    <p style="color:#666;font-size:11px;">API: <a href="/api/stats">/api/stats</a> |
    <a href="/api/tokens">/api/tokens</a> |
    <a href="/api/alerts">/api/alerts</a> |
    <a href="/api/watchlist">/api/watchlist</a> |
    <a href="/api/tiers">/api/tiers</a> |
    <a href="/logs">/logs</a> |
    <a href="/health">/health</a></p>
    </body></html>"""
    return html

# --- Health ---
@app.route("/health")
def health():
    age = time.time() - S.loop_ts if S.loop_ts else 999
    if age > 30:
        return jsonify({"status": "unhealthy", "loop_age": age, "version": VERSION}), 500
    return jsonify({"status": "ok", "loop_age": age, "version": VERSION})

# --- Logs ---
@app.route("/logs")
def logs():
    n = min(int(request.args.get("n", 200)), 500)
    lines = list(log_buf)[-n:]
    return "<pre style='background:#111;color:#0f0;padding:10px;font-size:12px'>" + "\n".join(lines) + "</pre>"

# --- API: Stats ---
@app.route("/api/stats")
def api_stats():
    now = time.time()
    with db() as conn:
        total_tokens = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        total_alerts = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        total_snaps = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        total_positions = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        positions = conn.execute("SELECT pnl_pct FROM positions").fetchall()

    wins = sum(1 for p in positions if (p["pnl_pct"] or 0) > 0)
    losses = sum(1 for p in positions if (p["pnl_pct"] or 0) < 0)
    total_pnl = sum((p["pnl_pct"] or 0) for p in positions)

    return jsonify({
        "version": VERSION,
        "sim_mode": SIM_MODE,
        "sol_price": S.sol_price,
        "fng": S.fng_value,
        "loop_count": S.loop_count,
        "loop_age": now - S.loop_ts if S.loop_ts else None,
        "tiers": {k: len(v) for k, v in S.tiers.items()},
        "watchlist_size": len(S.watchlist),
        "total_tokens": total_tokens,
        "total_alerts": total_alerts,
        "total_snapshots": total_snaps,
        "total_positions": total_positions,
        "wins": wins, "losses": losses,
        "total_pnl_pct": round(total_pnl, 2),
    })

# --- API: Tokens ---
@app.route("/api/tokens")
def api_tokens():
    with db() as conn:
        rows = conn.execute("SELECT * FROM tokens ORDER BY detected_at DESC LIMIT 200").fetchall()
    return jsonify([dict(r) for r in rows])

# --- API: Snapshots ---
@app.route("/api/snapshots/<address>")
def api_snapshots(address):
    limit = min(int(request.args.get("limit", 200)), 1000)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM snapshots WHERE address=? ORDER BY ts DESC LIMIT ?",
            (address, limit)
        ).fetchall()
    return jsonify([dict(r) for r in rows])

# --- API: Alerts ---
@app.route("/api/alerts")
def api_alerts():
    limit = min(int(request.args.get("limit", 100)), 500)
    with db() as conn:
        rows = conn.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
    return jsonify([dict(r) for r in rows])

# --- API: Watchlist ---
@app.route("/api/watchlist")
def api_watchlist():
    # Return top watchlist tokens sorted by velocity
    items = sorted(S.watchlist.values(), key=lambda x: x.get("velocity", 0), reverse=True)[:100]
    return jsonify(items)

# --- API: Tiers ---
@app.route("/api/tiers")
def api_tiers():
    result = {}
    for tier, addrs in S.tiers.items():
        result[tier] = []
        for a in addrs:
            tok = S.tokens.get(a, {})
            result[tier].append({
                "address": a,
                "symbol": tok.get("symbol", "?"),
                "status": tok.get("status", "?"),
                "age": time.time() - tok.get("detected_at", time.time()),
            })
    return jsonify(result)

# ---------------------------------------------------------------------------
# Entry point (for `python app_v7.py` dev mode)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
