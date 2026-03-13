#!/usr/bin/env python3
"""
Graduation Detector v4 — Full Analysis Suite

Features:
  - DexScreener + Helius WebSocket detection
  - 15-second resolution price curves (15 checkpoints over 10 min)
  - Buy/sell momentum tracking at every snapshot
  - Holder count tracking at key intervals
  - SOL price overlay (market context)
  - Pattern classification
  - Aggregated optimal buy/sell analysis
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
tracker_stats = {"tracked": 0, "snapshots_taken": 0, "errors": 0, "ws_events": 0}

# Current SOL price (updated every cycle)
sol_price_usd = 0.0
sol_price_lock = threading.Lock()

SNAPSHOT_TIMES = [15, 30, 45, 60, 75, 90, 105, 120, 150, 180, 210, 240, 300, 420, 600]
# Holder count check intervals (heavier API call, do less often)
HOLDER_CHECK_TIMES = [0, 60, 180, 300, 600]

ws_token_queue = []
ws_queue_lock = threading.Lock()

PUMPSWAP_PROGRAM = "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP"


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
            holders INTEGER,
            sol_price REAL,
            
            FOREIGN KEY (token_address) REFERENCES tokens(address),
            UNIQUE(token_address, target_sec)
        );
        
        CREATE INDEX IF NOT EXISTS idx_tokens_detected ON tokens(detected_ts);
        CREATE INDEX IF NOT EXISTS idx_tokens_incomplete ON tokens(snapshots_complete);
        CREATE INDEX IF NOT EXISTS idx_snapshots_token ON snapshots(token_address);
    """)
    conn.commit()
    conn.close()


# ── Helpers ─────────────────────────────────────────────────────────────

def fetch_json(url, timeout=8):
    try:
        req = Request(url, headers={"User-Agent": "GradDetector/4.0"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        tracker_stats["errors"] += 1
        return None


def post_json(url, data, timeout=8):
    try:
        body = json.dumps(data).encode()
        req = Request(url, data=body, headers={
            "User-Agent": "GradDetector/4.0",
            "Content-Type": "application/json"
        })
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
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
    except (ValueError, TypeError):
        return None


def get_sol_price():
    """Fetch current SOL/USD price."""
    data = fetch_json(f"{DEXSCREENER_BASE}/token-pairs/v1/solana/So11111111111111111111111111111111111111112")
    if data and isinstance(data, list):
        for pair in data:
            quote = pair.get("quoteToken", {}).get("symbol", "")
            if quote in ("USDC", "USDT"):
                try:
                    return float(pair.get("priceUsd", 0))
                except:
                    pass
    # Fallback: check a known SOL pair
    data = fetch_json(f"{DEXSCREENER_BASE}/latest/dex/search?q=SOL%20USDC%20solana")
    if data and data.get("pairs"):
        for pair in data["pairs"]:
            if pair.get("baseToken", {}).get("symbol") == "SOL":
                try:
                    return float(pair.get("priceUsd", 0))
                except:
                    pass
    return 0.0


def get_holder_count(token_address):
    """Get holder count via Helius RPC."""
    if not HELIUS_RPC:
        return None
    
    result = post_json(HELIUS_RPC, {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [token_address]
    })
    
    if result and result.get("result", {}).get("value"):
        # This gives top 20 holders. Not total count, but useful signal.
        holders = result["result"]["value"]
        # If all 20 slots are filled, there are at least 20+ holders
        return len(holders)
    return None


def record_token(conn, addr, pair, source="dexscreener"):
    existing = conn.execute("SELECT address FROM tokens WHERE address = ?", (addr,)).fetchone()
    if existing:
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
    
    # Get holder count at birth
    holders = get_holder_count(addr)
    
    with sol_price_lock:
        current_sol = sol_price_usd
    
    conn.execute("""
        INSERT OR IGNORE INTO tokens
        (address, symbol, name, dex_id, pair_address, pair_created_at, source,
         detected_at, detected_ts, age_at_detection_sec,
         t0_price, t0_liq, t0_vol_24h, t0_vol_h1, t0_vol_m5, t0_fdv, t0_mcap,
         t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
         t0_price_change_m5, t0_price_change_h1, t0_has_socials,
         t0_holders, t0_sol_price)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    ))
    tracker_stats["tracked"] += 1
    return True


# ── Helius WebSocket ────────────────────────────────────────────────────

def on_ws_message(ws, message):
    global ws_connected
    try:
        data = json.loads(message)
        if not isinstance(data, list):
            data = [data]
        for txn in data:
            tracker_stats["ws_events"] += 1
            token_addresses = set()
            for transfer in txn.get("tokenTransfers", []):
                mint = transfer.get("mint", "")
                if mint and mint.endswith("pump"):
                    token_addresses.add(mint)
            if not token_addresses:
                for acc in txn.get("accountData", []):
                    addr = acc.get("account", "")
                    if addr.endswith("pump"):
                        token_addresses.add(addr)
            for addr in token_addresses:
                with ws_queue_lock:
                    ws_token_queue.append({"address": addr, "source": "helius_ws"})
    except Exception as e:
        print(f"WS message error: {e}")

def on_ws_open(ws):
    global ws_connected
    ws_connected = True
    print("[WS] Connected — subscribing to PumpSwap graduations")
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
    print(f"[WS] Disconnected — reconnecting in 5s")

def on_ws_error(ws, error):
    print(f"[WS] Error: {error}")

def helius_ws_loop():
    if not HELIUS_API_KEY:
        print("[WS] No HELIUS_API_KEY — WebSocket disabled")
        return
    ws_url = f"wss://atlas-mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    while True:
        try:
            ws = websocket.WebSocketApp(ws_url,
                on_message=on_ws_message, on_open=on_ws_open,
                on_close=on_ws_close, on_error=on_ws_error)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[WS] Failed: {e}")
        time.sleep(5)


# ── DexScreener Discovery ──────────────────────────────────────────────

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

    for addr, source in list(candidates.items())[:40]:
        if addr in known:
            continue
        pair = get_pair_data(addr)
        if not pair:
            continue
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        dex = pair.get("dexId", "")
        if dex not in ("raydium", "pumpswap", "orca"):
            continue
        if liq < 5000:
            continue
        if record_token(conn, addr, pair, source):
            base = pair.get("baseToken", {})
            created = pair.get("pairCreatedAt", 0)
            age_sec = (time.time() * 1000 - created) / 1000 if created > 0 else 0
            new_tokens.append({"symbol": base.get("symbol", "???"), "address": addr, "age_sec": age_sec, "liq": liq})
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
        if dex not in ("raydium", "pumpswap", "orca"):
            continue
        if liq < 3000:
            continue
        if record_token(conn, addr, pair, "helius_ws"):
            base = pair.get("baseToken", {})
            new_tokens.append({"symbol": base.get("symbol", "???"), "address": addr})
        known.add(addr)
        time.sleep(0.2)
    conn.commit()
    conn.close()
    return new_tokens


# ── Snapshot Engine with Buy/Sell Momentum + Holders ────────────────────

def take_snapshots():
    conn = get_db()
    now = int(time.time())
    
    with sol_price_lock:
        current_sol = sol_price_usd
    
    tokens = conn.execute("""
        SELECT address, detected_ts, t0_price, t0_liq, t0_buys_m5, t0_sells_m5, peak_return
        FROM tokens WHERE snapshots_complete = 0 AND (? - detected_ts) <= 700
        ORDER BY detected_ts DESC
    """, (now,)).fetchall()
    
    for token in tokens:
        addr = token["address"]
        detected = token["detected_ts"]
        elapsed = now - detected
        
        existing = set(r[0] for r in conn.execute(
            "SELECT target_sec FROM snapshots WHERE token_address = ?", (addr,)
        ).fetchall())
        
        due = [t for t in SNAPSHOT_TIMES if t not in existing and elapsed >= t and elapsed <= t + 60]
        
        if not due:
            if elapsed > max(SNAPSHOT_TIMES) + 60:
                conn.execute("UPDATE tokens SET snapshots_complete = 1 WHERE address = ?", (addr,))
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
        
        # Buy/sell ratio: >1 = more buying, <1 = more selling
        total_txns = buys_m5 + sells_m5
        buy_sell_ratio = round(buys_m5 / sells_m5, 2) if sells_m5 > 0 else (99.0 if buys_m5 > 0 else 0.0)
        
        # Buy/sell momentum: compare current ratio to previous snapshot
        prev_snap = conn.execute("""
            SELECT buy_sell_ratio FROM snapshots
            WHERE token_address = ? ORDER BY target_sec DESC LIMIT 1
        """, (addr,)).fetchone()
        
        if prev_snap and prev_snap["buy_sell_ratio"] and prev_snap["buy_sell_ratio"] > 0:
            buy_sell_momentum = round(buy_sell_ratio - prev_snap["buy_sell_ratio"], 2)
        else:
            # Compare to t0
            t0_buys = token["t0_buys_m5"] or 0
            t0_sells = token["t0_sells_m5"] or 1
            t0_ratio = t0_buys / t0_sells if t0_sells > 0 else 0
            buy_sell_momentum = round(buy_sell_ratio - t0_ratio, 2) if t0_ratio > 0 else 0.0
        
        # Holder count at key intervals
        holders = None
        for check_time in HOLDER_CHECK_TIMES:
            if any(t >= check_time and t <= check_time + 30 for t in due):
                holders = get_holder_count(addr)
                break
        
        for target in due:
            conn.execute("""
                INSERT OR IGNORE INTO snapshots
                (token_address, target_sec, actual_sec, taken_at, price, liq, vol_m5,
                 buys_m5, sells_m5, fdv, return_pct, liq_change_pct,
                 buy_sell_ratio, buy_sell_momentum, holders, sol_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (addr, target, elapsed, now, price, liq, vol_m5, buys_m5, sells_m5,
                  fdv, ret, liq_change, buy_sell_ratio, buy_sell_momentum, holders, current_sol))
            tracker_stats["snapshots_taken"] += 1
        
        if ret is not None and (token["peak_return"] is None or ret > (token["peak_return"] or -999)):
            conn.execute("UPDATE tokens SET peak_price=?, peak_return=?, peak_time_sec=? WHERE address=?",
                        (price, ret, elapsed, addr))
        
        time.sleep(0.3)
    
    conn.commit()
    conn.close()


# ── Pattern Classification ──────────────────────────────────────────────

def classify_patterns():
    conn = get_db()
    tokens = conn.execute("SELECT address FROM tokens WHERE snapshots_complete=1 AND pattern IS NULL").fetchall()
    
    for token in tokens:
        addr = token["address"]
        snaps = conn.execute("""
            SELECT target_sec, return_pct, buy_sell_ratio, buy_sell_momentum
            FROM snapshots WHERE token_address=? ORDER BY target_sec
        """, (addr,)).fetchall()
        
        if len(snaps) < 5:
            conn.execute("UPDATE tokens SET pattern='insufficient_data' WHERE address=?", (addr,))
            continue
        
        returns = [s["return_pct"] or 0 for s in snaps]
        times = [s["target_sec"] for s in snaps]
        peak = max(returns)
        final = returns[-1]
        best_sell_idx = returns.index(max(returns))
        
        # Check buy/sell momentum at peak
        peak_momentum = snaps[best_sell_idx]["buy_sell_momentum"] or 0
        
        if peak > 50 and final < peak * 0.3:
            pattern = "pump_dump"
        elif len(returns) >= 3 and all(returns[i] >= returns[i-1] - 2 for i in range(1, min(6, len(returns)))) and final > 20:
            pattern = "rocket"
        elif final > 10:
            pattern = "slow_climb"
        elif final < -30:
            pattern = "rug"
        elif abs(final) < 5:
            pattern = "flat"
        elif peak > 20 and final > 0:
            pattern = "volatile_up"
        else:
            pattern = "volatile_down"
        
        conn.execute("UPDATE tokens SET pattern=?, best_buy_sec=0, best_sell_sec=?, max_profit_pct=? WHERE address=?",
                     (pattern, times[best_sell_idx], peak, addr))
    
    conn.commit()
    conn.close()


# ── SOL Price Updater ───────────────────────────────────────────────────

def update_sol_price():
    global sol_price_usd
    price = get_sol_price()
    if price > 0:
        with sol_price_lock:
            sol_price_usd = price


# ── Main Loop ───────────────────────────────────────────────────────────

def main_loop():
    cycle = 0
    while True:
        try:
            # Update SOL price every cycle
            if cycle % 4 == 0:  # Every ~60 seconds
                update_sol_price()
            
            ws_new = process_ws_queue()
            if ws_new:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] WS: {len(ws_new)}")
            
            if cycle % 2 == 0:
                dx_new = discover_dexscreener()
                if dx_new:
                    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] DX: {len(dx_new)}")
            
            take_snapshots()
            
            if cycle % 10 == 0:
                classify_patterns()
            
            cycle += 1
        except Exception as e:
            print(f"Loop error: {e}")
            tracker_stats["errors"] += 1
        
        time.sleep(15)


# ── API Routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    complete = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE snapshots_complete=1").fetchone()["c"]
    snap_count = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
    ws_count = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE source='helius_ws'").fetchone()["c"]
    conn.close()
    with sol_price_lock:
        sol = sol_price_usd
    return jsonify({
        "status": "running", "version": "v4",
        "websocket": "connected" if ws_connected else "disconnected",
        "sol_price_usd": sol,
        "scans": scan_count, "last_scan": last_scan_time,
        "total_tracked": total, "tracked_via_ws": ws_count,
        "with_complete_curves": complete, "total_snapshots": snap_count,
        "stats": tracker_stats,
    })

@app.route("/health")
def health():
    return "OK"

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
               peak_return, peak_time_sec, pattern, max_profit_pct, snapshots_complete
        FROM tokens WHERE detected_ts >= ? ORDER BY detected_ts DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    
    results = []
    for t in tokens:
        token = dict(t)
        snaps = conn.execute("""
            SELECT target_sec, price, liq, vol_m5, buys_m5, sells_m5, return_pct,
                   buy_sell_ratio, buy_sell_momentum, holders, sol_price
            FROM snapshots WHERE token_address=? ORDER BY target_sec
        """, (t["address"],)).fetchall()
        token["curve"] = [dict(s) for s in snaps]
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
    snaps = conn.execute("SELECT * FROM snapshots WHERE token_address=? ORDER BY target_sec", (address,)).fetchall()
    result["curve"] = [dict(s) for s in snaps]
    conn.close()
    return jsonify(result)

@app.route("/api/curves")
def get_curves():
    conn = get_db()
    tokens = conn.execute("SELECT address, pattern, peak_return, peak_time_sec FROM tokens WHERE snapshots_complete=1").fetchall()
    if not tokens:
        conn.close()
        return jsonify({"message": "No complete curves yet.", "total": 0})
    
    time_buckets = {}
    for target in SNAPSHOT_TIMES:
        rows = conn.execute("""
            SELECT return_pct, buy_sell_ratio, buy_sell_momentum, holders, sol_price
            FROM snapshots WHERE target_sec=? AND return_pct IS NOT NULL
        """, (target,)).fetchall()
        if rows:
            returns = [r["return_pct"] for r in rows]
            ratios = [r["buy_sell_ratio"] for r in rows if r["buy_sell_ratio"] is not None]
            momentums = [r["buy_sell_momentum"] for r in rows if r["buy_sell_momentum"] is not None]
            winners = [r for r in returns if r > 0]
            losers = [r for r in returns if r <= 0]
            
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
            if momentums:
                bucket["avg_momentum"] = round(sum(momentums)/len(momentums), 2)
            
            time_buckets[f"{target}s"] = bucket
    
    best_exit = max(time_buckets.items(), key=lambda x: x[1]["avg_return"]) if time_buckets else ("?", {})
    best_wr = max(time_buckets.items(), key=lambda x: x[1]["win_rate"]) if time_buckets else ("?", {})
    
    # Find the "sell signal": first time buy_sell_momentum goes negative
    sell_signal = None
    for t in sorted(time_buckets.keys(), key=lambda x: int(x.replace("s",""))):
        if time_buckets[t].get("avg_momentum", 1) < 0:
            sell_signal = t
            break
    
    patterns = {}
    for t in tokens:
        p = t["pattern"] or "unknown"
        if p not in patterns:
            patterns[p] = {"count": 0, "total_peak": 0}
        patterns[p]["count"] += 1
        patterns[p]["total_peak"] += t["peak_return"] or 0
    for p in patterns:
        patterns[p]["avg_peak"] = round(patterns[p]["total_peak"]/patterns[p]["count"], 1)
        del patterns[p]["total_peak"]
    
    conn.close()
    return jsonify({
        "total_tokens_analyzed": len(tokens),
        "price_curve_by_time": time_buckets,
        "optimal_exit": {"time": best_exit[0], "avg_return": best_exit[1].get("avg_return", 0)},
        "optimal_win_rate": {"time": best_wr[0], "win_rate": best_wr[1].get("win_rate", 0)},
        "sell_signal": sell_signal,
        "sell_signal_meaning": f"Buy/sell momentum turns negative at {sell_signal} — sellers overtaking buyers" if sell_signal else "Not enough data yet",
        "patterns": patterns,
    })

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
        "version": "v4",
        "websocket": "connected" if ws_connected else "disconnected",
        "sol_price_usd": sol,
        "scans": scan_count, "last_scan": last_scan_time,
        "total_tracked": total, "via_websocket": ws_count, "via_dexscreener": dx_count,
        "complete_curves": complete, "total_snapshots": snap_count,
        "classified": sum(patterns.values()), "patterns": patterns,
        "snapshot_schedule": SNAPSHOT_TIMES,
        "holder_check_times": HOLDER_CHECK_TIMES,
        "tracker": tracker_stats,
    })

# ── Start ───────────────────────────────────────────────────────────────

init_db()

_started = False
def start_all():
    global _started
    if not _started:
        _started = True
        threading.Thread(target=main_loop, daemon=True).start()
        threading.Thread(target=helius_ws_loop, daemon=True).start()
        print("Scanner v4 started — DX + Helius WS + buy/sell momentum + holders + SOL price!")

start_all()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
