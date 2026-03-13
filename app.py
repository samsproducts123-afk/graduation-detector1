#!/usr/bin/env python3
"""
Graduation Detector v5 — Complete Analysis Suite

All features:
  1. Buy/sell momentum at every snapshot
  2. Holder count tracking
  3. Volume velocity (acceleration/deceleration)
  4. SOL price overlay
  5. Creator wallet reputation tracking
  6. Sniper bot detection (early buyers across tokens)
  7. 15-second resolution price curves
  + DexScreener + Helius WebSocket detection
"""

import os
import time
import json
import threading
import sqlite3
import websocket
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "graduations.db")
DEXSCREENER_BASE = "https://api.dexscreener.com"
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

scan_count = 0
last_scan_time = None
ws_connected = False
sol_price_usd = 0.0
sol_price_lock = threading.Lock()
tracker_stats = {"tracked": 0, "snapshots_taken": 0, "errors": 0, "ws_events": 0}

# Fear & Greed index (updated periodically)
fear_greed_value = 50
fear_greed_label = "Neutral"
fear_greed_lock = threading.Lock()

SNAPSHOT_TIMES = [15, 30, 45, 60, 75, 90, 105, 120, 150, 180, 210, 240, 300, 420, 600]
HOLDER_CHECK_TIMES = [0, 60, 180, 300, 600]
PUMPSWAP_PROGRAM = "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP"

# In-memory tracking databases
creator_history = {}  # wallet -> {launches, rugs, tokens}
creator_lock = threading.Lock()
sniper_wallets = {}  # wallet -> {appearances, tokens}
sniper_lock = threading.Lock()

ws_token_queue = []
ws_queue_lock = threading.Lock()


# ── Database ────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tokens (
            address TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            dex_id TEXT,
            pair_address TEXT,
            pair_created_at INTEGER,
            source TEXT,
            detected_at TEXT,
            detected_ts INTEGER,
            age_at_detection_sec REAL,
            t0_price TEXT,
            t0_liq REAL,
            t0_vol_24h REAL,
            t0_vol_h1 REAL,
            t0_vol_m5 REAL,
            t0_fdv REAL,
            t0_mcap REAL,
            t0_buys_m5 INTEGER,
            t0_sells_m5 INTEGER,
            t0_buys_h1 INTEGER,
            t0_sells_h1 INTEGER,
            t0_price_change_m5 REAL,
            t0_price_change_h1 REAL,
            t0_has_socials INTEGER,
            t0_holders INTEGER,
            t0_sol_price REAL,
            t0_fear_greed INTEGER,
            t0_hour_utc INTEGER,
            t0_day_of_week TEXT,
            creator_wallet TEXT,
            creator_prev_launches INTEGER DEFAULT 0,
            creator_prev_rugs INTEGER DEFAULT 0,
            creator_reputation TEXT DEFAULT 'unknown',
            early_buyers TEXT,
            sniper_bot_count INTEGER DEFAULT 0,
            peak_price TEXT,
            peak_return REAL,
            peak_time_sec REAL,
            pattern TEXT,
            best_buy_sec REAL,
            best_sell_sec REAL,
            max_profit_pct REAL,
            snapshots_complete INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            target_sec INTEGER NOT NULL,
            actual_sec REAL NOT NULL,
            taken_at INTEGER NOT NULL,
            price TEXT,
            liq REAL,
            vol_m5 REAL,
            buys_m5 INTEGER,
            sells_m5 INTEGER,
            fdv REAL,
            return_pct REAL,
            liq_change_pct REAL,
            buy_sell_ratio REAL,
            buy_sell_momentum REAL,
            volume_velocity REAL,
            holders INTEGER,
            sol_price REAL,
            FOREIGN KEY (token_address) REFERENCES tokens(address),
            UNIQUE(token_address, target_sec)
        );
        CREATE TABLE IF NOT EXISTS creators (
            wallet TEXT PRIMARY KEY,
            total_launches INTEGER DEFAULT 0,
            total_rugs INTEGER DEFAULT 0,
            tokens TEXT DEFAULT '[]',
            last_seen TEXT
        );
        CREATE TABLE IF NOT EXISTS snipers (
            wallet TEXT PRIMARY KEY,
            appearances INTEGER DEFAULT 0,
            tokens TEXT DEFAULT '[]',
            last_seen TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tokens_detected ON tokens(detected_ts);
        CREATE INDEX IF NOT EXISTS idx_tokens_incomplete ON tokens(snapshots_complete);
        CREATE INDEX IF NOT EXISTS idx_snapshots_token ON snapshots(token_address);
    """)
    conn.commit()
    conn.close()


def load_tracking_data():
    """Load creator and sniper data from DB into memory on startup."""
    conn = get_db()
    try:
        for row in conn.execute("SELECT * FROM creators").fetchall():
            creator_history[row["wallet"]] = {
                "launches": row["total_launches"],
                "rugs": row["total_rugs"],
                "tokens": json.loads(row["tokens"] or "[]")
            }
    except:
        pass
    try:
        for row in conn.execute("SELECT * FROM snipers").fetchall():
            sniper_wallets[row["wallet"]] = {
                "appearances": row["appearances"],
                "tokens": json.loads(row["tokens"] or "[]")
            }
    except:
        pass
    conn.close()


def save_tracking_data():
    """Persist in-memory tracking to DB."""
    conn = get_db()
    with creator_lock:
        for wallet, data in creator_history.items():
            conn.execute("""
                INSERT OR REPLACE INTO creators (wallet, total_launches, total_rugs, tokens, last_seen)
                VALUES (?, ?, ?, ?, ?)
            """, (wallet, data["launches"], data["rugs"],
                  json.dumps(data["tokens"][-20:]), datetime.now(timezone.utc).isoformat()))
    with sniper_lock:
        for wallet, data in sniper_wallets.items():
            if data["appearances"] >= 2:  # Only save repeat buyers
                conn.execute("""
                    INSERT OR REPLACE INTO snipers (wallet, appearances, tokens, last_seen)
                    VALUES (?, ?, ?, ?)
                """, (wallet, data["appearances"],
                      json.dumps(data["tokens"][-20:]), datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


# ── API Helpers ─────────────────────────────────────────────────────────

def fetch_json(url, timeout=8):
    try:
        req = Request(url, headers={"User-Agent": "GradDetector/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except:
        tracker_stats["errors"] += 1
        return None

def post_json(url, data, timeout=8):
    try:
        body = json.dumps(data).encode()
        req = Request(url, data=body, headers={"User-Agent": "GradDetector/5.0", "Content-Type": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except:
        return None

def get_pair_data(address):
    data = fetch_json(f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{address}")
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    return max(data, key=lambda p: (p.get("liquidity") or {}).get("usd", 0))

def calc_return(initial_price, current_price):
    try:
        ip, cp = float(initial_price), float(current_price)
        return round((cp - ip) / ip * 100, 2) if ip > 0 else None
    except:
        return None

def get_sol_price():
    data = fetch_json(f"{DEXSCREENER_BASE}/latest/dex/search?q=SOL%20USDC%20solana")
    if data and data.get("pairs"):
        for pair in data["pairs"]:
            if pair.get("baseToken", {}).get("symbol") == "SOL":
                try:
                    return float(pair.get("priceUsd", 0))
                except:
                    pass
    return 0.0

def update_fear_greed():
    """Fetch crypto Fear & Greed index from alternative.me (free, no key)."""
    global fear_greed_value, fear_greed_label
    data = fetch_json("https://api.alternative.me/fng/?limit=1")
    if data and data.get("data") and len(data["data"]) > 0:
        entry = data["data"][0]
        with fear_greed_lock:
            fear_greed_value = int(entry.get("value", 50))
            fear_greed_label = entry.get("value_classification", "Neutral")

def get_holder_count(token_address):
    if not HELIUS_RPC:
        return None
    result = post_json(HELIUS_RPC, {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [token_address]
    })
    if result and result.get("result", {}).get("value"):
        return len(result["result"]["value"])
    return None


# ── Feature 5: Creator Wallet Tracking ──────────────────────────────────

def get_creator_wallet(token_address):
    """Find who created/deployed this token."""
    if not HELIUS_API_KEY:
        return None
    data = fetch_json(
        f"https://api.helius.xyz/v0/addresses/{token_address}/transactions?api-key={HELIUS_API_KEY}&limit=1&type=CREATE"
    )
    if data and isinstance(data, list) and len(data) > 0:
        return data[0].get("feePayer", None)
    return None

def check_creator_reputation(creator_wallet):
    """Check creator's track record across previous launches."""
    if not creator_wallet:
        return 0, 0, "unknown"
    with creator_lock:
        if creator_wallet in creator_history:
            h = creator_history[creator_wallet]
            launches, rugs = h["launches"], h["rugs"]
            if launches == 0:
                return 0, 0, "new"
            rug_rate = rugs / launches
            if rug_rate > 0.5:
                rep = "serial_rugger"
            elif rug_rate > 0.25:
                rep = "risky"
            elif launches >= 3 and rug_rate == 0:
                rep = "trusted"
            else:
                rep = "neutral"
            return launches, rugs, rep
    return 0, 0, "unknown"

def update_creator_db(creator_wallet, token_address):
    """Record this launch for the creator."""
    if not creator_wallet:
        return
    with creator_lock:
        if creator_wallet not in creator_history:
            creator_history[creator_wallet] = {"launches": 0, "rugs": 0, "tokens": []}
        creator_history[creator_wallet]["launches"] += 1
        creator_history[creator_wallet]["tokens"].append(token_address)

def mark_creator_rug(creator_wallet):
    """Mark a rug for this creator (called during pattern classification)."""
    if not creator_wallet:
        return
    with creator_lock:
        if creator_wallet in creator_history:
            creator_history[creator_wallet]["rugs"] += 1


# ── Feature 6: Sniper Bot Detection ────────────────────────────────────

def get_early_buyers(token_address, limit=10):
    """Get first N buyers of a token."""
    if not HELIUS_API_KEY:
        return []
    data = fetch_json(
        f"https://api.helius.xyz/v0/addresses/{token_address}/transactions?api-key={HELIUS_API_KEY}&limit={limit}&type=SWAP"
    )
    if not data or not isinstance(data, list):
        return []
    buyers = []
    seen = set()
    for txn in data:
        fp = txn.get("feePayer", "")
        if fp and fp not in seen:
            seen.add(fp)
            buyers.append(fp)
    return buyers[:limit]

def check_sniper_bots(early_buyers):
    """Count how many early buyers are known repeat snipers."""
    count = 0
    with sniper_lock:
        for wallet in early_buyers:
            if wallet in sniper_wallets and sniper_wallets[wallet]["appearances"] >= 3:
                count += 1
    return count

def update_sniper_db(token_address, early_buyers):
    """Track early buyers for cross-token analysis."""
    with sniper_lock:
        for wallet in early_buyers:
            if wallet not in sniper_wallets:
                sniper_wallets[wallet] = {"appearances": 0, "tokens": []}
            sniper_wallets[wallet]["appearances"] += 1
            sniper_wallets[wallet]["tokens"].append(token_address)
            if len(sniper_wallets[wallet]["tokens"]) > 50:
                sniper_wallets[wallet]["tokens"] = sniper_wallets[wallet]["tokens"][-50:]


# ── Token Recording ─────────────────────────────────────────────────────

def record_token(conn, addr, pair, source="dexscreener"):
    if conn.execute("SELECT 1 FROM tokens WHERE address=?", (addr,)).fetchone():
        return False

    now = int(time.time())
    created = pair.get("pairCreatedAt", 0)
    age_sec = (now * 1000 - created) / 1000 if created > 0 else 0
    base = pair.get("baseToken", {})
    txns = pair.get("txns", {})
    pc = pair.get("priceChange", {})
    vol = pair.get("volume", {})
    liq = (pair.get("liquidity") or {}).get("usd", 0)
    info = pair.get("info", {}) or {}
    socials = info.get("socials", []) or []
    websites = info.get("websites", []) or []

    # Feature 2: Holders at birth
    holders = get_holder_count(addr)

    # Feature 4: SOL price context
    with sol_price_lock:
        current_sol = sol_price_usd
    
    # Features 8-10: Time analysis + Fear & Greed
    now_dt = datetime.now(timezone.utc)
    hour_utc = now_dt.hour
    day_of_week = now_dt.strftime("%A")
    with fear_greed_lock:
        fg_value = fear_greed_value

    # Feature 5: Creator reputation
    creator = get_creator_wallet(addr)
    prev_launches, prev_rugs, reputation = check_creator_reputation(creator)
    update_creator_db(creator, addr)

    # Feature 6: Early buyers + sniper detection
    early = get_early_buyers(addr, limit=10)
    bot_count = check_sniper_bots(early)
    update_sniper_db(addr, early)
    early_json = json.dumps(early[:10])

    conn.execute("""
        INSERT OR IGNORE INTO tokens
        (address, symbol, name, dex_id, pair_address, pair_created_at, source,
         detected_at, detected_ts, age_at_detection_sec,
         t0_price, t0_liq, t0_vol_24h, t0_vol_h1, t0_vol_m5, t0_fdv, t0_mcap,
         t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
         t0_price_change_m5, t0_price_change_h1, t0_has_socials,
         t0_holders, t0_sol_price,
         t0_fear_greed, t0_hour_utc, t0_day_of_week,
         creator_wallet, creator_prev_launches, creator_prev_rugs, creator_reputation,
         early_buyers, sniper_bot_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        addr, base.get("symbol", "???"), base.get("name", "Unknown"),
        pair.get("dexId", ""), pair.get("pairAddress", ""), created, source,
        datetime.now(timezone.utc).isoformat(), now, age_sec,
        pair.get("priceUsd", "0"), liq,
        vol.get("h24", 0), vol.get("h1", 0), vol.get("m5", 0),
        pair.get("fdv", 0), pair.get("marketCap", 0),
        (txns.get("m5") or {}).get("buys", 0), (txns.get("m5") or {}).get("sells", 0),
        (txns.get("h1") or {}).get("buys", 0), (txns.get("h1") or {}).get("sells", 0),
        pc.get("m5", 0) or 0, pc.get("h1", 0) or 0,
        1 if (len(socials) > 0 or len(websites) > 0) else 0,
        holders, current_sol,
        fg_value, hour_utc, day_of_week,
        creator, prev_launches, prev_rugs, reputation,
        early_json, bot_count,
    ))
    tracker_stats["tracked"] += 1
    return True


# ── Helius WebSocket ────────────────────────────────────────────────────

def on_ws_message(ws, message):
    try:
        data = json.loads(message)
        if not isinstance(data, list):
            data = [data]
        for txn in data:
            tracker_stats["ws_events"] += 1
            addrs = set()
            for t in txn.get("tokenTransfers", []):
                m = t.get("mint", "")
                if m and m.endswith("pump"):
                    addrs.add(m)
            if not addrs:
                for a in txn.get("accountData", []):
                    addr = a.get("account", "")
                    if addr.endswith("pump"):
                        addrs.add(addr)
            for addr in addrs:
                with ws_queue_lock:
                    ws_token_queue.append({"address": addr, "source": "helius_ws"})
    except Exception as e:
        print(f"WS msg error: {e}")

def on_ws_open(ws):
    global ws_connected
    ws_connected = True
    print("[WS] Connected")
    ws.send(json.dumps({
        "jsonrpc": "2.0", "id": 1,
        "method": "transactionSubscribe",
        "params": [
            {"accountInclude": [PUMPSWAP_PROGRAM]},
            {"commitment": "confirmed", "encoding": "jsonParsed",
             "transactionDetails": "full", "maxSupportedTransactionVersion": 0}
        ]
    }))

def on_ws_close(ws, code, msg):
    global ws_connected
    ws_connected = False
    print("[WS] Disconnected")

def on_ws_error(ws, error):
    print(f"[WS] Error: {error}")

def helius_ws_loop():
    if not HELIUS_API_KEY:
        print("[WS] No key — disabled")
        return
    url = f"wss://atlas-mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    while True:
        try:
            ws = websocket.WebSocketApp(url, on_message=on_ws_message,
                on_open=on_ws_open, on_close=on_ws_close, on_error=on_ws_error)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[WS] Failed: {e}")
        time.sleep(5)


# ── Discovery ───────────────────────────────────────────────────────────

def discover_dexscreener():
    global scan_count, last_scan_time
    conn = get_db()
    known = set(r[0] for r in conn.execute("SELECT address FROM tokens").fetchall())
    new_tokens = []
    candidates = {}
    for url, name in [
        (f"{DEXSCREENER_BASE}/token-profiles/latest/v1", "profiles"),
        (f"{DEXSCREENER_BASE}/token-boosts/latest/v1", "boosts"),
        (f"{DEXSCREENER_BASE}/token-boosts/top/v1", "top_boosts"),
    ]:
        data = fetch_json(url)
        if data and isinstance(data, list):
            for item in data:
                if item.get("chainId") == "solana":
                    addr = item.get("tokenAddress", "")
                    if addr and addr not in known:
                        candidates[addr] = name
    for addr, src in list(candidates.items())[:40]:
        if addr in known:
            continue
        pair = get_pair_data(addr)
        if not pair:
            continue
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        dex = pair.get("dexId", "")
        if dex not in ("raydium", "pumpswap", "orca") or liq < 5000:
            continue
        if record_token(conn, addr, pair, src):
            base = pair.get("baseToken", {})
            created = pair.get("pairCreatedAt", 0)
            age = (time.time() * 1000 - created) / 1000 if created > 0 else 0
            new_tokens.append({"symbol": base.get("symbol", "???"), "address": addr, "age_sec": age, "liq": liq})
        time.sleep(0.25)
    conn.commit()
    conn.close()
    scan_count += 1
    last_scan_time = datetime.now(timezone.utc).isoformat()
    return new_tokens

def process_ws_queue():
    with ws_queue_lock:
        queue = list(ws_token_queue)
        ws_token_queue.clear()
    if not queue:
        return []
    conn = get_db()
    known = set(r[0] for r in conn.execute("SELECT address FROM tokens").fetchall())
    new_tokens = []
    for item in queue:
        addr = item["address"]
        if addr in known:
            continue
        pair = get_pair_data(addr)
        if not pair:
            with ws_queue_lock:
                ws_token_queue.append(item)
            continue
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        dex = pair.get("dexId", "")
        if dex not in ("raydium", "pumpswap", "orca") or liq < 3000:
            continue
        if record_token(conn, addr, pair, "helius_ws"):
            new_tokens.append({"symbol": pair.get("baseToken", {}).get("symbol", "???"), "address": addr})
        known.add(addr)
        time.sleep(0.2)
    conn.commit()
    conn.close()
    return new_tokens


# ── Snapshot Engine ─────────────────────────────────────────────────────

def take_snapshots():
    conn = get_db()
    now = int(time.time())
    with sol_price_lock:
        current_sol = sol_price_usd

    tokens = conn.execute("""
        SELECT address, detected_ts, t0_price, t0_liq, t0_buys_m5, t0_sells_m5, t0_vol_m5, peak_return
        FROM tokens WHERE snapshots_complete=0 AND (?-detected_ts)<=700
        ORDER BY detected_ts DESC
    """, (now,)).fetchall()

    for token in tokens:
        addr = token["address"]
        detected = token["detected_ts"]
        elapsed = now - detected
        existing = set(r[0] for r in conn.execute(
            "SELECT target_sec FROM snapshots WHERE token_address=?", (addr,)).fetchall())
        due = [t for t in SNAPSHOT_TIMES if t not in existing and elapsed >= t and elapsed <= t + 60]

        if not due:
            if elapsed > max(SNAPSHOT_TIMES) + 60:
                conn.execute("UPDATE tokens SET snapshots_complete=1 WHERE address=?", (addr,))
            continue

        pair = get_pair_data(addr)
        if not pair:
            continue

        price = pair.get("priceUsd", "0")
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        vol_m5 = (pair.get("volume") or {}).get("m5", 0)
        buys_m5 = (pair.get("txns", {}).get("m5") or {}).get("buys", 0)
        sells_m5 = (pair.get("txns", {}).get("m5") or {}).get("sells", 0)
        fdv = pair.get("fdv", 0)
        ret = calc_return(token["t0_price"], price)
        liq_change = round((liq - token["t0_liq"]) / token["t0_liq"] * 100, 2) if token["t0_liq"] > 0 else 0

        # Feature 1: Buy/sell ratio + momentum
        bs_ratio = round(buys_m5 / sells_m5, 2) if sells_m5 > 0 else (99.0 if buys_m5 > 0 else 0.0)
        prev = conn.execute("SELECT buy_sell_ratio, vol_m5 FROM snapshots WHERE token_address=? ORDER BY target_sec DESC LIMIT 1", (addr,)).fetchone()
        if prev and prev["buy_sell_ratio"] and prev["buy_sell_ratio"] > 0:
            bs_momentum = round(bs_ratio - prev["buy_sell_ratio"], 2)
            prev_vol = prev["vol_m5"] or 0
        else:
            t0_b, t0_s = token["t0_buys_m5"] or 0, token["t0_sells_m5"] or 1
            t0_ratio = t0_b / t0_s if t0_s > 0 else 0
            bs_momentum = round(bs_ratio - t0_ratio, 2) if t0_ratio > 0 else 0.0
            prev_vol = token["t0_vol_m5"] or 0

        # Feature 7: Volume velocity
        if prev_vol > 0:
            vol_velocity = round((vol_m5 - prev_vol) / prev_vol * 100, 2)
        else:
            vol_velocity = 0.0

        # Feature 2: Holder count at key intervals
        holders = None
        for ct in HOLDER_CHECK_TIMES:
            if any(t >= ct and t <= ct + 30 for t in due):
                holders = get_holder_count(addr)
                break

        for target in due:
            conn.execute("""
                INSERT OR IGNORE INTO snapshots
                (token_address, target_sec, actual_sec, taken_at, price, liq, vol_m5,
                 buys_m5, sells_m5, fdv, return_pct, liq_change_pct,
                 buy_sell_ratio, buy_sell_momentum, volume_velocity, holders, sol_price)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (addr, target, elapsed, now, price, liq, vol_m5, buys_m5, sells_m5,
                  fdv, ret, liq_change, bs_ratio, bs_momentum, vol_velocity, holders, current_sol))
            tracker_stats["snapshots_taken"] += 1

        if ret is not None and (token["peak_return"] is None or ret > (token["peak_return"] or -999)):
            conn.execute("UPDATE tokens SET peak_price=?, peak_return=?, peak_time_sec=? WHERE address=?",
                        (price, ret, elapsed, addr))
        time.sleep(0.3)

    conn.commit()
    conn.close()


# ── Pattern Classification + Creator Rug Updates ────────────────────────

def classify_patterns():
    conn = get_db()
    tokens = conn.execute("SELECT address, creator_wallet FROM tokens WHERE snapshots_complete=1 AND pattern IS NULL").fetchall()
    for token in tokens:
        addr = token["address"]
        snaps = conn.execute("SELECT target_sec, return_pct, buy_sell_ratio, buy_sell_momentum, volume_velocity FROM snapshots WHERE token_address=? ORDER BY target_sec", (addr,)).fetchall()
        if len(snaps) < 5:
            conn.execute("UPDATE tokens SET pattern='insufficient_data' WHERE address=?", (addr,))
            continue

        returns = [s["return_pct"] or 0 for s in snaps]
        times = [s["target_sec"] for s in snaps]
        peak = max(returns)
        final = returns[-1]
        best_idx = returns.index(peak)

        if peak > 50 and final < peak * 0.3:
            pattern = "pump_dump"
        elif len(returns) >= 3 and all(returns[i] >= returns[i-1] - 2 for i in range(1, min(6, len(returns)))) and final > 20:
            pattern = "rocket"
        elif final > 10:
            pattern = "slow_climb"
        elif final < -30:
            pattern = "rug"
            # Feature 5: Mark creator as rugger
            mark_creator_rug(token["creator_wallet"])
        elif abs(final) < 5:
            pattern = "flat"
        elif peak > 20 and final > 0:
            pattern = "volatile_up"
        else:
            pattern = "volatile_down"

        conn.execute("UPDATE tokens SET pattern=?, best_buy_sec=0, best_sell_sec=?, max_profit_pct=? WHERE address=?",
                     (pattern, times[best_idx], peak, addr))
    conn.commit()
    conn.close()

def mark_creator_rug(creator_wallet):
    if not creator_wallet:
        return
    with creator_lock:
        if creator_wallet in creator_history:
            creator_history[creator_wallet]["rugs"] += 1


# ── Main Loop ───────────────────────────────────────────────────────────

import traceback as _tb

_log_lines = []
def _log(msg):
    ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    _log_lines.append(line)
    if len(_log_lines) > 200:
        _log_lines.pop(0)

def main_loop():
    global sol_price_usd, fear_greed_value, fear_greed_label
    _log("main_loop STARTED")
    cycle = 0
    while True:
        try:
            if cycle % 4 == 0:
                p = get_sol_price()
                if p > 0:
                    with sol_price_lock:
                        sol_price_usd = p
                    if cycle == 0:
                        _log(f"SOL price: ${p}")
            
            if cycle % 40 == 0:
                update_fear_greed()

            ws_new = process_ws_queue()
            if ws_new:
                _log(f"WS: {len(ws_new)} new")

            if cycle % 2 == 0:
                dx_new = discover_dexscreener()
                _log(f"DX scan #{scan_count}: {len(dx_new)} new tokens")

            take_snapshots()

            if cycle % 10 == 0:
                classify_patterns()

            if cycle % 20 == 0:
                save_tracking_data()

            cycle += 1
        except Exception as e:
            _log(f"ERROR: {e}\n{''.join(_tb.format_exc())}")
            tracker_stats["errors"] += 1
        time.sleep(15)


# ── API Routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    complete = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE snapshots_complete=1").fetchone()["c"]
    snaps = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
    ws_count = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE source='helius_ws'").fetchone()["c"]
    conn.close()
    with sol_price_lock:
        sol = sol_price_usd
    return jsonify({
        "status": "running", "version": "v5",
        "websocket": "connected" if ws_connected else "disconnected",
        "sol_price_usd": sol,
        "fear_greed": {"value": fear_greed_value, "label": fear_greed_label},
        "scans": scan_count, "last_scan": last_scan_time,
        "total_tracked": total, "tracked_via_ws": ws_count,
        "complete_curves": complete, "total_snapshots": snaps,
        "known_creators": len(creator_history),
        "known_snipers": sum(1 for w in sniper_wallets.values() if w["appearances"] >= 3),
        "stats": tracker_stats,
    })

@app.route("/health")
def health():
    return "OK"

@app.route("/logs")
def logs():
    return jsonify({"lines": _log_lines[-100:], "count": len(_log_lines)})

@app.route("/api/graduations")
def get_graduations():
    conn = get_db()
    since = flask_request.args.get("since", type=int, default=0)
    limit = flask_request.args.get("limit", type=int, default=50)
    max_age = flask_request.args.get("max_age_min", type=int, default=60)
    now = int(time.time())
    cutoff = max(since, now - max_age * 60)
    tokens = conn.execute("""
        SELECT address, symbol, name, dex_id, source, detected_at, detected_ts, age_at_detection_sec,
               t0_price, t0_liq, t0_vol_24h, t0_fdv, t0_holders, t0_sol_price,
               creator_wallet, creator_prev_launches, creator_prev_rugs, creator_reputation,
               sniper_bot_count,
               peak_return, peak_time_sec, pattern, max_profit_pct, snapshots_complete
        FROM tokens WHERE detected_ts>=? ORDER BY detected_ts DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    results = []
    for t in tokens:
        token = dict(t)
        snps = conn.execute("""
            SELECT target_sec, price, liq, vol_m5, buys_m5, sells_m5, return_pct,
                   buy_sell_ratio, buy_sell_momentum, volume_velocity, holders, sol_price
            FROM snapshots WHERE token_address=? ORDER BY target_sec
        """, (t["address"],)).fetchall()
        token["curve"] = [dict(s) for s in snps]
        results.append(token)
    conn.close()
    return jsonify({"count": len(results), "graduations": results})

@app.route("/api/graduations/<address>")
def get_graduation(address):
    conn = get_db()
    token = conn.execute("SELECT * FROM tokens WHERE address=?", (address,)).fetchone()
    if not token:
        conn.close()
        return jsonify({"error": "not found"}), 404
    result = dict(token)
    snps = conn.execute("SELECT * FROM snapshots WHERE token_address=? ORDER BY target_sec", (address,)).fetchall()
    result["curve"] = [dict(s) for s in snps]
    conn.close()
    return jsonify(result)

@app.route("/api/curves")
def get_curves():
    conn = get_db()
    tokens = conn.execute("SELECT address, pattern, peak_return, peak_time_sec, creator_reputation, sniper_bot_count, t0_hour_utc, t0_day_of_week, t0_fear_greed FROM tokens WHERE snapshots_complete=1").fetchall()
    if not tokens:
        conn.close()
        return jsonify({"message": "No complete curves yet.", "total": 0})

    time_buckets = {}
    for target in SNAPSHOT_TIMES:
        rows = conn.execute("""
            SELECT return_pct, buy_sell_ratio, buy_sell_momentum, volume_velocity, holders
            FROM snapshots WHERE target_sec=? AND return_pct IS NOT NULL
        """, (target,)).fetchall()
        if rows:
            returns = [r["return_pct"] for r in rows]
            winners = [r for r in returns if r > 0]
            losers = [r for r in returns if r <= 0]
            ratios = [r["buy_sell_ratio"] for r in rows if r["buy_sell_ratio"]]
            moms = [r["buy_sell_momentum"] for r in rows if r["buy_sell_momentum"] is not None]
            vvs = [r["volume_velocity"] for r in rows if r["volume_velocity"] is not None]
            bucket = {
                "tokens": len(returns),
                "avg_return": round(sum(returns)/len(returns), 2),
                "median_return": round(sorted(returns)[len(returns)//2], 2),
                "win_rate": round(len(winners)/len(returns)*100, 1),
                "avg_winner": round(sum(winners)/len(winners), 2) if winners else 0,
                "avg_loser": round(sum(losers)/len(losers), 2) if losers else 0,
                "best": round(max(returns), 2),
                "worst": round(min(returns), 2),
            }
            if ratios:
                bucket["avg_buy_sell_ratio"] = round(sum(ratios)/len(ratios), 2)
            if moms:
                bucket["avg_momentum"] = round(sum(moms)/len(moms), 2)
            if vvs:
                bucket["avg_volume_velocity"] = round(sum(vvs)/len(vvs), 2)
            time_buckets[f"{target}s"] = bucket

    best_exit = max(time_buckets.items(), key=lambda x: x[1]["avg_return"]) if time_buckets else ("?", {})
    best_wr = max(time_buckets.items(), key=lambda x: x[1]["win_rate"]) if time_buckets else ("?", {})

    sell_signal = None
    for t in sorted(time_buckets.keys(), key=lambda x: int(x.replace("s",""))):
        if time_buckets[t].get("avg_momentum", 1) < 0:
            sell_signal = t
            break

    vol_death = None
    for t in sorted(time_buckets.keys(), key=lambda x: int(x.replace("s",""))):
        if time_buckets[t].get("avg_volume_velocity", 1) < -20:
            vol_death = t
            break

    patterns = {}
    creator_impact = {"serial_rugger": [], "trusted": [], "unknown": []}
    sniper_impact = {"with_bots": [], "without_bots": []}
    for t in tokens:
        p = t["pattern"] or "unknown"
        if p not in patterns:
            patterns[p] = {"count": 0, "total_peak": 0}
        patterns[p]["count"] += 1
        patterns[p]["total_peak"] += t["peak_return"] or 0
        rep = t["creator_reputation"] or "unknown"
        if rep in creator_impact:
            creator_impact[rep].append(t["peak_return"] or 0)
        else:
            creator_impact.setdefault("other", []).append(t["peak_return"] or 0)
        if (t["sniper_bot_count"] or 0) > 0:
            sniper_impact["with_bots"].append(t["peak_return"] or 0)
        else:
            sniper_impact["without_bots"].append(t["peak_return"] or 0)

    for p in patterns:
        patterns[p]["avg_peak"] = round(patterns[p]["total_peak"]/patterns[p]["count"], 1)
        del patterns[p]["total_peak"]

    creator_analysis = {}
    for rep, peaks in creator_impact.items():
        if peaks:
            creator_analysis[rep] = {"count": len(peaks), "avg_peak": round(sum(peaks)/len(peaks), 1)}

    sniper_analysis = {}
    for key, peaks in sniper_impact.items():
        if peaks:
            sniper_analysis[key] = {"count": len(peaks), "avg_peak": round(sum(peaks)/len(peaks), 1)}

    # Feature 10: Time-of-day analysis
    hourly_performance = {}
    for t in tokens:
        hour = t["t0_hour_utc"]
        if hour is not None:
            h = str(hour).zfill(2) + ":00"
            if h not in hourly_performance:
                hourly_performance[h] = {"tokens": 0, "peaks": []}
            hourly_performance[h]["tokens"] += 1
            hourly_performance[h]["peaks"].append(t["peak_return"] or 0)
    for h in hourly_performance:
        peaks = hourly_performance[h]["peaks"]
        hourly_performance[h] = {
            "tokens": len(peaks),
            "avg_peak": round(sum(peaks)/len(peaks), 1),
            "win_rate": round(sum(1 for p in peaks if p > 0)/len(peaks)*100, 1) if peaks else 0
        }

    # Feature 8: Day-of-week analysis
    daily_performance = {}
    for t in tokens:
        day = t["t0_day_of_week"]
        if day:
            if day not in daily_performance:
                daily_performance[day] = {"tokens": 0, "peaks": []}
            daily_performance[day]["tokens"] += 1
            daily_performance[day]["peaks"].append(t["peak_return"] or 0)
    for d in daily_performance:
        peaks = daily_performance[d]["peaks"]
        daily_performance[d] = {
            "tokens": len(peaks),
            "avg_peak": round(sum(peaks)/len(peaks), 1),
            "win_rate": round(sum(1 for p in peaks if p > 0)/len(peaks)*100, 1) if peaks else 0
        }

    # Feature 9: Fear & Greed impact
    fg_buckets = {"extreme_fear": (0, 25), "fear": (25, 45), "neutral": (45, 55), "greed": (55, 75), "extreme_greed": (75, 100)}
    fg_performance = {}
    for t in tokens:
        fg = t["t0_fear_greed"]
        if fg is not None:
            for label, (lo, hi) in fg_buckets.items():
                if lo <= fg < hi:
                    if label not in fg_performance:
                        fg_performance[label] = {"tokens": 0, "peaks": []}
                    fg_performance[label]["tokens"] += 1
                    fg_performance[label]["peaks"].append(t["peak_return"] or 0)
                    break
    for label in fg_performance:
        peaks = fg_performance[label]["peaks"]
        fg_performance[label] = {
            "tokens": len(peaks),
            "avg_peak": round(sum(peaks)/len(peaks), 1),
            "win_rate": round(sum(1 for p in peaks if p > 0)/len(peaks)*100, 1) if peaks else 0
        }

    conn.close()
    return jsonify({
        "total_tokens_analyzed": len(tokens),
        "price_curve_by_time": time_buckets,
        "optimal_exit": {"time": best_exit[0], "avg_return": best_exit[1].get("avg_return", 0)},
        "optimal_win_rate": {"time": best_wr[0], "win_rate": best_wr[1].get("win_rate", 0)},
        "sell_signal": sell_signal,
        "volume_death_signal": vol_death,
        "patterns": patterns,
        "creator_reputation_impact": creator_analysis,
        "sniper_bot_impact": sniper_analysis,
        "hourly_performance": hourly_performance,
        "daily_performance": daily_performance,
        "fear_greed_impact": fg_performance,
    })

@app.route("/api/snipers")
def get_snipers():
    """Show known sniper bots (wallets appearing in 3+ token launches)."""
    with sniper_lock:
        bots = {w: {"appearances": d["appearances"], "tokens": d["tokens"][-5:]}
                for w, d in sniper_wallets.items() if d["appearances"] >= 3}
    return jsonify({"known_snipers": len(bots), "top_snipers": dict(sorted(bots.items(), key=lambda x: -x[1]["appearances"])[:20])})

@app.route("/api/creators")
def get_creators():
    """Show creator wallet reputation database."""
    with creator_lock:
        data = {w: {"launches": d["launches"], "rugs": d["rugs"],
                     "rug_rate": round(d["rugs"]/d["launches"]*100, 1) if d["launches"] > 0 else 0}
                for w, d in creator_history.items() if d["launches"] >= 2}
    return jsonify({"known_creators": len(data), "creators": dict(sorted(data.items(), key=lambda x: -x[1]["launches"])[:30])})

@app.route("/api/stats")
def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    complete = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE snapshots_complete=1").fetchone()["c"]
    snap_count = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
    ws_count = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE source='helius_ws'").fetchone()["c"]
    dx_count = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE source!='helius_ws'").fetchone()["c"]
    patterns = dict(conn.execute("SELECT pattern, COUNT(*) FROM tokens WHERE pattern IS NOT NULL GROUP BY pattern").fetchall())
    conn.close()
    with sol_price_lock:
        sol = sol_price_usd
    return jsonify({
        "version": "v5",
        "websocket": "connected" if ws_connected else "disconnected",
        "sol_price_usd": sol,
        "scans": scan_count, "last_scan": last_scan_time,
        "total_tracked": total, "via_websocket": ws_count, "via_dexscreener": dx_count,
        "complete_curves": complete, "total_snapshots": snap_count,
        "classified": sum(patterns.values()), "patterns": patterns,
        "known_creators": len(creator_history),
        "known_snipers": sum(1 for w in sniper_wallets.values() if w["appearances"] >= 3),
        "snapshot_schedule": SNAPSHOT_TIMES,
        "tracker": tracker_stats,
    })


# ── Start ───────────────────────────────────────────────────────────────

init_db()
load_tracking_data()

_started = False
def start_all():
    global _started
    if not _started:
        _started = True
        _log("Starting threads...")
        threading.Thread(target=main_loop, daemon=True, name="main_loop").start()
        threading.Thread(target=helius_ws_loop, daemon=True, name="ws_loop").start()
        _log("Scanner v5 STARTED — all threads launched")

start_all()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
