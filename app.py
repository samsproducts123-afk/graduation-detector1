#!/usr/bin/env python3
"""
Graduation Detector v3 — High-Resolution Price Curve Tracker

Tracks every token with 15-second resolution for first 2 minutes,
then 30-second until 5 minutes. Shows FULL price movement.

Data structure:
  tokens table  → one row per token (discovery data)
  snapshots table → one row per price check (unlimited resolution)

Endpoints:
  GET /                         → Status
  GET /api/graduations          → Recent graduates with full snapshot curves
  GET /api/graduations/{addr}   → Single token with every price point
  GET /api/curves               → Aggregated optimal buy/sell analysis
  GET /api/stats                → Scanner stats
  GET /health                   → Health check
"""

import os
import time
import json
import threading
import sqlite3
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "graduations.db")
DEXSCREENER_BASE = "https://api.dexscreener.com"

scan_count = 0
last_scan_time = None
tracker_stats = {"tracked": 0, "snapshots_taken": 0, "errors": 0}

# Snapshot schedule: seconds after detection
# Every 15s for first 2 min, then every 30s until 5 min, then 1m until 10m
SNAPSHOT_TIMES = [15, 30, 45, 60, 75, 90, 105, 120, 150, 180, 210, 240, 300, 420, 600]
SNAPSHOT_TOLERANCE = 10  # seconds tolerance for scheduling


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
            
            detected_at TEXT,
            detected_ts INTEGER,
            age_at_detection_sec REAL,
            
            -- Snapshot at detection (t=0)
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
            
            -- Peak tracking (updated with each snapshot)
            peak_price TEXT,
            peak_return REAL,
            peak_time_sec REAL,
            
            -- Final computed values (after all snapshots done)
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
            
            FOREIGN KEY (token_address) REFERENCES tokens(address),
            UNIQUE(token_address, target_sec)
        );
        
        CREATE INDEX IF NOT EXISTS idx_tokens_detected ON tokens(detected_ts);
        CREATE INDEX IF NOT EXISTS idx_tokens_incomplete ON tokens(snapshots_complete);
        CREATE INDEX IF NOT EXISTS idx_snapshots_token ON snapshots(token_address);
        CREATE INDEX IF NOT EXISTS idx_snapshots_pending ON snapshots(token_address, target_sec);
    """)
    conn.commit()
    conn.close()


# ── API Helpers ─────────────────────────────────────────────────────────

def fetch_json(url, timeout=8):
    try:
        req = Request(url, headers={"User-Agent": "GradDetector/3.0"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        tracker_stats["errors"] += 1
        return None


def get_pair_data(address):
    data = fetch_json(f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{address}")
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    return max(data, key=lambda p: (p.get("liquidity") or {}).get("usd", 0))


def calc_return(initial_price, current_price):
    try:
        ip = float(initial_price)
        cp = float(current_price)
        if ip > 0:
            return round((cp - ip) / ip * 100, 2)
    except (ValueError, TypeError):
        pass
    return None


# ── Discovery ───────────────────────────────────────────────────────────

def discover_new_tokens():
    global scan_count, last_scan_time
    
    conn = get_db()
    known = set(r[0] for r in conn.execute("SELECT address FROM tokens").fetchall())
    new_tokens = []
    candidates = {}

    # Source 1: Latest profiles
    profiles = fetch_json(f"{DEXSCREENER_BASE}/token-profiles/latest/v1")
    if profiles and isinstance(profiles, list):
        for p in profiles:
            if p.get("chainId") == "solana":
                addr = p.get("tokenAddress", "")
                if addr and addr not in known:
                    candidates[addr] = "profiles"

    # Source 2: Latest boosts
    boosts = fetch_json(f"{DEXSCREENER_BASE}/token-boosts/latest/v1")
    if boosts and isinstance(boosts, list):
        for b in boosts:
            if b.get("chainId") == "solana":
                addr = b.get("tokenAddress", "")
                if addr and addr not in known:
                    candidates[addr] = "boost"

    # Source 3: Top boosts
    top_boosts = fetch_json(f"{DEXSCREENER_BASE}/token-boosts/top/v1")
    if top_boosts and isinstance(top_boosts, list):
        for b in top_boosts:
            if b.get("chainId") == "solana":
                addr = b.get("tokenAddress", "")
                if addr and addr not in known:
                    candidates[addr] = "top_boost"

    # Check each candidate
    for addr, source in list(candidates.items())[:40]:
        if addr in known:
            continue
        
        pair = get_pair_data(addr)
        if not pair:
            continue
        
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        dex = pair.get("dexId", "")
        created = pair.get("pairCreatedAt", 0)
        
        if dex not in ("raydium", "pumpswap", "orca"):
            continue
        if liq < 5000:
            continue
        
        now = int(time.time())
        age_sec = (now * 1000 - created) / 1000 if created > 0 else 0
        
        base = pair.get("baseToken", {})
        txns = pair.get("txns", {})
        pc = pair.get("priceChange", {})
        vol = pair.get("volume", {})
        info = pair.get("info", {}) or {}
        socials = info.get("socials", []) or []
        websites = info.get("websites", []) or []
        
        conn.execute("""
            INSERT OR IGNORE INTO tokens
            (address, symbol, name, dex_id, pair_address, pair_created_at,
             detected_at, detected_ts, age_at_detection_sec,
             t0_price, t0_liq, t0_vol_24h, t0_vol_h1, t0_vol_m5, t0_fdv, t0_mcap,
             t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
             t0_price_change_m5, t0_price_change_h1, t0_has_socials)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            addr, base.get("symbol", "???"), base.get("name", "Unknown"),
            dex, pair.get("pairAddress", ""), created,
            datetime.now(timezone.utc).isoformat(), now, age_sec,
            pair.get("priceUsd", "0"), liq,
            vol.get("h24", 0), vol.get("h1", 0), vol.get("m5", 0),
            pair.get("fdv", 0), pair.get("marketCap", 0),
            (txns.get("m5") or {}).get("buys", 0),
            (txns.get("m5") or {}).get("sells", 0),
            (txns.get("h1") or {}).get("buys", 0),
            (txns.get("h1") or {}).get("sells", 0),
            pc.get("m5", 0) or 0, pc.get("h1", 0) or 0,
            1 if (len(socials) > 0 or len(websites) > 0) else 0,
        ))
        
        known.add(addr)
        new_tokens.append({"symbol": base.get("symbol", "???"), "address": addr, "age_sec": age_sec, "liq": liq})
        tracker_stats["tracked"] += 1
        
        time.sleep(0.25)
    
    conn.commit()
    conn.close()
    
    scan_count += 1
    last_scan_time = datetime.now(timezone.utc).isoformat()
    
    return new_tokens


# ── Snapshot Engine ─────────────────────────────────────────────────────

def take_snapshots():
    """Check all tokens that need snapshots at any scheduled time."""
    conn = get_db()
    now = int(time.time())
    
    # Get tokens still being tracked (not all snapshots done)
    tokens = conn.execute("""
        SELECT address, detected_ts, t0_price, t0_liq, peak_return
        FROM tokens
        WHERE snapshots_complete = 0
          AND (? - detected_ts) <= 700
        ORDER BY detected_ts DESC
    """, (now,)).fetchall()
    
    for token in tokens:
        addr = token["address"]
        detected = token["detected_ts"]
        t0_price = token["t0_price"]
        t0_liq = token["t0_liq"]
        elapsed = now - detected
        
        # Find which snapshots are due but not taken
        existing = set(r[0] for r in conn.execute(
            "SELECT target_sec FROM snapshots WHERE token_address = ?", (addr,)
        ).fetchall())
        
        due_snapshots = []
        for target in SNAPSHOT_TIMES:
            if target in existing:
                continue
            if elapsed >= target and elapsed <= target + 60:  # Due and not too late
                due_snapshots.append(target)
        
        if not due_snapshots:
            # Check if all snapshots are done
            if elapsed > max(SNAPSHOT_TIMES) + 60:
                conn.execute("UPDATE tokens SET snapshots_complete = 1 WHERE address = ?", (addr,))
            continue
        
        # Fetch current price (one API call per token, covers all due snapshots)
        pair = get_pair_data(addr)
        if not pair:
            continue
        
        price = pair.get("priceUsd", "0")
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        vol_m5 = (pair.get("volume") or {}).get("m5", 0)
        buys_m5 = (pair.get("txns", {}).get("m5") or {}).get("buys", 0)
        sells_m5 = (pair.get("txns", {}).get("m5") or {}).get("sells", 0)
        fdv = pair.get("fdv", 0)
        
        ret = calc_return(t0_price, price)
        liq_change = round((liq - t0_liq) / t0_liq * 100, 2) if t0_liq > 0 else 0
        
        # Insert snapshot for each due time
        for target in due_snapshots:
            conn.execute("""
                INSERT OR IGNORE INTO snapshots
                (token_address, target_sec, actual_sec, taken_at, price, liq, vol_m5,
                 buys_m5, sells_m5, fdv, return_pct, liq_change_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (addr, target, elapsed, now, price, liq, vol_m5, buys_m5, sells_m5, fdv, ret, liq_change))
            tracker_stats["snapshots_taken"] += 1
        
        # Update peak
        if ret is not None:
            current_peak = token["peak_return"]
            if current_peak is None or ret > current_peak:
                conn.execute("""
                    UPDATE tokens SET peak_price = ?, peak_return = ?, peak_time_sec = ?
                    WHERE address = ?
                """, (price, ret, elapsed, addr))
        
        time.sleep(0.3)
    
    conn.commit()
    conn.close()


# ── Pattern Classification ──────────────────────────────────────────────

def classify_patterns():
    conn = get_db()
    
    tokens = conn.execute("""
        SELECT address, t0_price FROM tokens
        WHERE snapshots_complete = 1 AND pattern IS NULL
    """).fetchall()
    
    for token in tokens:
        addr = token["address"]
        snaps = conn.execute("""
            SELECT target_sec, return_pct FROM snapshots
            WHERE token_address = ? ORDER BY target_sec
        """, (addr,)).fetchall()
        
        if len(snaps) < 5:
            conn.execute("UPDATE tokens SET pattern = 'insufficient_data' WHERE address = ?", (addr,))
            continue
        
        returns = [s["return_pct"] or 0 for s in snaps]
        times = [s["target_sec"] for s in snaps]
        peak = max(returns)
        peak_time = times[returns.index(peak)]
        final = returns[-1]
        
        # Find optimal buy/sell
        best_sell_idx = returns.index(max(returns))
        max_profit = peak  # Buy at t0, sell at peak
        
        # Classify pattern
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
        
        conn.execute("""
            UPDATE tokens SET pattern = ?, best_buy_sec = 0, best_sell_sec = ?, max_profit_pct = ?
            WHERE address = ?
        """, (pattern, times[best_sell_idx], max_profit, addr))
    
    conn.commit()
    conn.close()


# ── Main Loop ───────────────────────────────────────────────────────────

def main_loop():
    cycle = 0
    while True:
        try:
            new = discover_new_tokens()
            if new:
                syms = ", ".join(f"{t['symbol']}({t['age_sec']:.0f}s)" for t in new[:5])
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] NEW: {len(new)} — {syms}")
            
            take_snapshots()
            
            if cycle % 10 == 0:
                classify_patterns()
            
            cycle += 1
        except Exception as e:
            print(f"Loop error: {e}")
            tracker_stats["errors"] += 1
        
        time.sleep(15)  # Run every 15 seconds for high resolution


# ── API Routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    complete = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE snapshots_complete = 1").fetchone()["c"]
    snap_count = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
    conn.close()
    return jsonify({
        "status": "running",
        "scans": scan_count,
        "last_scan": last_scan_time,
        "total_tracked": total,
        "with_complete_curves": complete,
        "total_snapshots": snap_count,
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
        SELECT address, symbol, name, dex_id, detected_at, detected_ts, age_at_detection_sec,
               t0_price, t0_liq, t0_vol_24h, t0_fdv,
               peak_return, peak_time_sec, pattern, max_profit_pct, snapshots_complete
        FROM tokens
        WHERE detected_ts >= ?
        ORDER BY detected_ts DESC
        LIMIT ?
    """, (cutoff, limit)).fetchall()
    
    results = []
    for t in tokens:
        token = dict(t)
        # Attach all snapshots as a price curve
        snaps = conn.execute("""
            SELECT target_sec, actual_sec, price, liq, vol_m5, buys_m5, sells_m5, return_pct, liq_change_pct
            FROM snapshots WHERE token_address = ? ORDER BY target_sec
        """, (t["address"],)).fetchall()
        token["curve"] = [dict(s) for s in snaps]
        results.append(token)
    
    conn.close()
    return jsonify({"count": len(results), "graduations": results})


@app.route("/api/graduations/<address>")
def get_graduation(address):
    conn = get_db()
    token = conn.execute("SELECT * FROM tokens WHERE address = ?", (address,)).fetchone()
    if not token:
        conn.close()
        return jsonify({"error": "not found"}), 404
    
    result = dict(token)
    snaps = conn.execute("""
        SELECT target_sec, actual_sec, price, liq, vol_m5, buys_m5, sells_m5, fdv, return_pct, liq_change_pct
        FROM snapshots WHERE token_address = ? ORDER BY target_sec
    """, (address,)).fetchall()
    result["curve"] = [dict(s) for s in snaps]
    
    conn.close()
    return jsonify(result)


@app.route("/api/curves")
def get_curves():
    """THE GOLD MINE: aggregated price curves showing optimal buy/sell timing."""
    conn = get_db()
    
    # Get all complete tokens
    tokens = conn.execute("""
        SELECT address, t0_price, t0_liq, pattern, peak_return, peak_time_sec
        FROM tokens WHERE snapshots_complete = 1
    """).fetchall()
    
    if not tokens:
        conn.close()
        return jsonify({"message": "No complete curves yet. Need ~10 min of data collection.", "total": 0})
    
    # Build aggregated curves per target_sec
    time_buckets = {}
    for target in SNAPSHOT_TIMES:
        rows = conn.execute("""
            SELECT return_pct FROM snapshots
            WHERE target_sec = ? AND return_pct IS NOT NULL
        """, (target,)).fetchall()
        
        if rows:
            returns = [r["return_pct"] for r in rows]
            winners = [r for r in returns if r > 0]
            losers = [r for r in returns if r <= 0]
            time_buckets[f"{target}s"] = {
                "tokens": len(returns),
                "avg_return": round(sum(returns) / len(returns), 2),
                "median_return": round(sorted(returns)[len(returns)//2], 2),
                "win_rate": round(len(winners) / len(returns) * 100, 1),
                "avg_winner": round(sum(winners) / len(winners), 2) if winners else 0,
                "avg_loser": round(sum(losers) / len(losers), 2) if losers else 0,
                "best": round(max(returns), 2),
                "worst": round(min(returns), 2),
            }
    
    # Find optimal entry/exit
    if time_buckets:
        best_exit = max(time_buckets.items(), key=lambda x: x[1]["avg_return"])
        best_wr = max(time_buckets.items(), key=lambda x: x[1]["win_rate"])
    else:
        best_exit = ("?", {})
        best_wr = ("?", {})
    
    # Pattern breakdown
    patterns = {}
    for t in tokens:
        p = t["pattern"] or "unknown"
        if p not in patterns:
            patterns[p] = {"count": 0, "avg_peak": 0, "peaks": []}
        patterns[p]["count"] += 1
        patterns[p]["peaks"].append(t["peak_return"] or 0)
    for p in patterns:
        peaks = patterns[p]["peaks"]
        patterns[p]["avg_peak"] = round(sum(peaks) / len(peaks), 1)
        patterns[p]["avg_peak_time"] = "N/A"
        del patterns[p]["peaks"]
    
    total = len(tokens)
    conn.close()
    
    return jsonify({
        "total_tokens_analyzed": total,
        "price_curve_by_time": time_buckets,
        "optimal_exit": {
            "by_avg_return": best_exit[0],
            "avg_return": best_exit[1].get("avg_return", 0),
        },
        "optimal_entry": {
            "by_win_rate": best_wr[0],
            "win_rate": best_wr[1].get("win_rate", 0),
        },
        "patterns": patterns,
        "insight": f"Based on {total} tokens with full curves. Best avg exit: {best_exit[0]}. Best win rate: {best_wr[0]} ({best_wr[1].get('win_rate', 0)}%)"
    })


@app.route("/api/stats")
def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    complete = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE snapshots_complete = 1").fetchone()["c"]
    snap_count = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
    classified = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE pattern IS NOT NULL").fetchone()["c"]
    
    patterns = {}
    for row in conn.execute("SELECT pattern, COUNT(*) as c FROM tokens WHERE pattern IS NOT NULL GROUP BY pattern").fetchall():
        patterns[row["pattern"]] = row["c"]
    
    conn.close()
    return jsonify({
        "scans": scan_count,
        "last_scan": last_scan_time,
        "total_tracked": total,
        "with_complete_curves": complete,
        "total_snapshots": snap_count,
        "classified": classified,
        "patterns": patterns,
        "snapshot_schedule": SNAPSHOT_TIMES,
        "tracker": tracker_stats,
    })


# ── Start ───────────────────────────────────────────────────────────────

init_db()

_scanner_started = False
def start_scanner():
    global _scanner_started
    if not _scanner_started:
        _scanner_started = True
        loop_thread = threading.Thread(target=main_loop, daemon=True)
        loop_thread.start()
        print("Scanner v3 started — 15s resolution tracking!")

start_scanner()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
