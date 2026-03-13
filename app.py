#!/usr/bin/env python3
"""
Graduation Sniper v6 — Pre-Graduation Detection + Instant Alerts + Exit Signals

Architecture:
  1. Pump.fun Monitor: watches tokens approaching graduation ($69K mcap)
  2. Pre-Scorer: scores tokens BEFORE they graduate (zero delay)
  3. Graduation Detector: instant detection when pre-scored token graduates
  4. Alert Engine: generates BUY alerts with one-click Photon links
  5. Position Monitor: watches active positions, generates EXIT alerts
  6. Price Curve Tracker: 15-second snapshots for all graduated tokens
"""

import os
import time
import json
import threading
import sqlite3
import traceback
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "graduations.db")
DEXSCREENER_BASE = "https://api.dexscreener.com"
PUMPFUN_API = "https://frontend-api-v3.pump.fun"
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

# ── Global State ────────────────────────────────────────────────────────

sol_price_usd = 0.0
sol_price_lock = threading.Lock()
fear_greed_value = 50
fear_greed_label = "Neutral"
fear_greed_lock = threading.Lock()

SNAPSHOT_TIMES = [15, 30, 45, 60, 75, 90, 105, 120, 150, 180, 210, 240, 300, 420, 600]
GRADUATION_MCAP = 69000  # $69K graduation threshold
PRE_GRAD_THRESHOLD = 0.65  # Start watching at 65% of graduation mcap (~$45K)
SNIPE_THRESHOLD = 85.0  # Score % to trigger BUY alert

stats = {
    "started_at": datetime.now(timezone.utc).isoformat(),
    "scans": 0, "pumpfun_checks": 0, "tokens_tracked": 0,
    "snapshots_taken": 0, "errors": 0,
    "pre_grad_watched": 0, "pre_grad_scored": 0,
    "alerts_generated": 0, "exit_signals": 0,
}

_log_lines = []
def _log(msg):
    ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    _log_lines.append(line)
    if len(_log_lines) > 300:
        del _log_lines[:100]


# ── Database ────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        -- Pre-graduation watchlist
        CREATE TABLE IF NOT EXISTS watchlist (
            mint TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            first_seen_ts INTEGER,
            first_seen_mcap REAL,
            last_seen_ts INTEGER,
            last_seen_mcap REAL,
            velocity_score REAL,
            check_count INTEGER DEFAULT 0,
            pre_scored INTEGER DEFAULT 0,
            v1_score INTEGER,
            v1_pct REAL,
            v1_verdict TEXT,
            creator_wallet TEXT,
            creator_reputation TEXT,
            graduated INTEGER DEFAULT 0,
            graduated_ts INTEGER,
            alert_sent INTEGER DEFAULT 0
        );
        
        -- Graduated tokens with full data
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
            
            graduation_velocity REAL,
            velocity_rating TEXT,
            
            creator_wallet TEXT,
            creator_reputation TEXT,
            
            v1_score INTEGER,
            v1_pct REAL,
            v1_verdict TEXT,
            
            peak_price TEXT,
            peak_return REAL,
            peak_time_sec REAL,
            pattern TEXT,
            best_buy_sec REAL,
            best_sell_sec REAL,
            max_profit_pct REAL,
            snapshots_complete INTEGER DEFAULT 0
        );
        
        -- Price snapshots
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
        
        -- Alerts (BUY and EXIT)
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            token_address TEXT NOT NULL,
            symbol TEXT,
            created_at TEXT,
            created_ts INTEGER,
            delivered INTEGER DEFAULT 0,
            
            score INTEGER,
            score_pct REAL,
            verdict TEXT,
            velocity_rating TEXT,
            graduation_velocity REAL,
            
            price_at_alert TEXT,
            liq_at_alert REAL,
            fdv_at_alert REAL,
            
            photon_link TEXT,
            dexscreener_link TEXT,
            
            exit_reason TEXT,
            return_pct REAL,
            
            message TEXT
        );
        
        -- Active positions (for exit monitoring)
        CREATE TABLE IF NOT EXISTS positions (
            token_address TEXT PRIMARY KEY,
            symbol TEXT,
            entry_price TEXT,
            entry_ts INTEGER,
            entry_liq REAL,
            peak_price TEXT,
            peak_return REAL,
            current_return REAL,
            last_check_ts INTEGER,
            status TEXT DEFAULT 'active',
            exit_reason TEXT,
            exit_return REAL
        );
        
        CREATE INDEX IF NOT EXISTS idx_tokens_detected ON tokens(detected_ts);
        CREATE INDEX IF NOT EXISTS idx_snapshots_token ON snapshots(token_address);
        CREATE INDEX IF NOT EXISTS idx_alerts_pending ON alerts(delivered);
        CREATE INDEX IF NOT EXISTS idx_watchlist_active ON watchlist(graduated);
        CREATE INDEX IF NOT EXISTS idx_positions_active ON positions(status);
    """)
    conn.commit()
    conn.close()


# ── API Helpers ─────────────────────────────────────────────────────────

def fetch_json(url, timeout=5):
    try:
        req = Request(url, headers={"User-Agent": "GradSniper/6.0", "Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read(500000)  # Max 500KB — kill huge responses
            return json.loads(raw.decode())
    except Exception as e:
        stats["errors"] += 1
        return None

def post_json(url, data, timeout=10):
    try:
        body = json.dumps(data).encode()
        req = Request(url, data=body, headers={"User-Agent": "GradSniper/6.0", "Content-Type": "application/json"})
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
    # Non-blocking: run in thread with hard 4s kill
    result = [0.0]
    def _fetch():
        try:
            data = fetch_json(f"{DEXSCREENER_BASE}/token-pairs/v1/solana/So11111111111111111111111111111111111111112")
            if data and isinstance(data, list):
                for pair in data[:3]:
                    try:
                        p = float(pair.get("priceUsd", 0) or 0)
                        if p > 0:
                            result[0] = p
                            return
                    except: pass
        except: pass
    t = threading.Thread(target=_fetch)
    t.start()
    t.join(timeout=4)
    return result[0]

def update_fear_greed():
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
    result = post_json(HELIUS_RPC, {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [token_address]})
    if result and result.get("result", {}).get("value"):
        return len(result["result"]["value"])
    return None

def get_creator_wallet(token_address):
    if not HELIUS_API_KEY:
        return None
    data = fetch_json(f"https://api.helius.xyz/v0/addresses/{token_address}/transactions?api-key={HELIUS_API_KEY}&limit=1&type=CREATE")
    if data and isinstance(data, list) and len(data) > 0:
        return data[0].get("feePayer", None)
    return None


# ── PHASE 1: Pump.fun Pre-Graduation Monitor ───────────────────────────

def _check_pumpfun():
    """Watch Pump.fun for tokens approaching graduation threshold."""
    data = fetch_json(f"{PUMPFUN_API}/coins/currently-live")
    if not data or not isinstance(data, list):
        return 0
    
    stats["pumpfun_checks"] += 1
    conn = get_db()
    now = int(time.time())
    new_watched = 0
    
    for token in data:
        mint = token.get("mint", "")
        if not mint:
            continue
        
        mcap = token.get("usd_market_cap", 0) or 0
        if mcap < GRADUATION_MCAP * PRE_GRAD_THRESHOLD:
            continue
        
        symbol = token.get("symbol", "???")
        name = token.get("name", "Unknown")
        
        existing = conn.execute("SELECT * FROM watchlist WHERE mint=?", (mint,)).fetchone()
        
        if existing:
            # Update tracking
            prev_mcap = existing["last_seen_mcap"] or mcap
            prev_ts = existing["last_seen_ts"] or now
            elapsed = now - (existing["first_seen_ts"] or now)
            
            # Velocity: how fast is mcap growing? ($/second)
            if elapsed > 0:
                velocity = (mcap - (existing["first_seen_mcap"] or 0)) / elapsed
            else:
                velocity = 0
            
            conn.execute("""
                UPDATE watchlist SET last_seen_ts=?, last_seen_mcap=?, velocity_score=?, check_count=check_count+1
                WHERE mint=?
            """, (now, mcap, velocity, mint))
        else:
            conn.execute("""
                INSERT INTO watchlist (mint, symbol, name, first_seen_ts, first_seen_mcap, last_seen_ts, last_seen_mcap, velocity_score, check_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1)
            """, (mint, symbol, name, now, mcap, now, mcap))
            new_watched += 1
            stats["pre_grad_watched"] += 1
    
    conn.commit()
    conn.close()
    if new_watched > 0:
        _log(f"Pump.fun: {new_watched} new tokens on watchlist")
    return new_watched


def _pre_score():
    """Pre-score tokens that are close to graduation but not yet scored."""
    conn = get_db()
    
    # Get tokens at 80%+ of graduation that haven't been scored
    candidates = conn.execute("""
        SELECT mint, symbol FROM watchlist 
        WHERE pre_scored=0 AND graduated=0 AND last_seen_mcap >= ?
        ORDER BY velocity_score DESC LIMIT 2
    """, (GRADUATION_MCAP * 0.80,)).fetchall()
    
    for token in candidates:
        mint = token["mint"]
        
        # Quick safety check via RugCheck
        rc_data = fetch_json(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
        
        # Check creator reputation
        creator = get_creator_wallet(mint)
        creator_rep = "unknown"
        if rc_data:
            risks = rc_data.get("risks", [])
            for risk in risks:
                if "rug" in str(risk.get("description", "")).lower():
                    creator_rep = "serial_rugger"
                    break
        
        # Simple pre-score based on available data
        score = 0
        max_score = 137
        
        if rc_data:
            # Mint authority
            if rc_data.get("mintAuthority") is None:
                score += 8
            # Freeze authority
            if rc_data.get("freezeAuthority") is None:
                score += 8
            # LP locked
            lp_pct = rc_data.get("lpLockedPct", 0) or 0
            if lp_pct >= 90:
                score += 7
            elif lp_pct >= 50:
                score += 4
            # Risk score
            risk_level = rc_data.get("score", 0)
            if risk_level and risk_level < 500:
                score += 6
            elif risk_level and risk_level < 2000:
                score += 4
            # Creator history
            if creator_rep != "serial_rugger":
                score += 10
        
        # We can't do full 33-layer score without DexScreener pair data (not available pre-graduation)
        # But this gives us a safety baseline. Full score happens at graduation.
        pct = round(score * 100.0 / max_score, 1) if max_score > 0 else 0
        verdict = "PRE-SCORED"
        
        conn.execute("""
            UPDATE watchlist SET pre_scored=1, v1_score=?, v1_pct=?, v1_verdict=?,
            creator_wallet=?, creator_reputation=?
            WHERE mint=?
        """, (score, pct, verdict, creator, creator_rep, mint))
        
        stats["pre_grad_scored"] += 1
        _log(f"Pre-scored {token['symbol']}: {score}pts, creator={creator_rep}")
        time.sleep(0.5)
    
    conn.commit()
    conn.close()


# ── PHASE 2: Graduation Detection ──────────────────────────────────────

def _check_grads():
    """Check if any watched tokens have graduated (appear on DexScreener)."""
    conn = get_db()
    now = int(time.time())
    
    # Get pre-scored, non-graduated tokens
    watched = conn.execute("""
        SELECT mint, symbol, name, first_seen_ts, first_seen_mcap, velocity_score,
               v1_score, v1_pct, creator_wallet, creator_reputation
        FROM watchlist WHERE graduated=0 AND pre_scored=1
        ORDER BY velocity_score DESC LIMIT 3
    """).fetchall()
    
    for token in watched:
        mint = token["mint"]
        pair = get_pair_data(mint)
        
        if not pair:
            continue
        
        # Token has graduated! It's on DexScreener now.
        dex = pair.get("dexId", "")
        if dex not in ("raydium", "pumpswap", "orca"):
            continue
        
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        if liq < 3000:
            continue
        
        # Calculate graduation velocity
        first_seen = token["first_seen_ts"]
        grad_time = now - first_seen if first_seen else 0
        velocity = token["velocity_score"] or 0
        
        if grad_time > 0 and grad_time < 120:
            vel_rating = "ROCKET"     # Graduated in < 2 min
        elif grad_time < 300:
            vel_rating = "FAST"       # < 5 min
        elif grad_time < 600:
            vel_rating = "NORMAL"     # < 10 min
        else:
            vel_rating = "SLOW"       # > 10 min
        
        # Mark as graduated
        conn.execute("UPDATE watchlist SET graduated=1, graduated_ts=? WHERE mint=?", (now, mint))
        
        # Full token recording
        base = pair.get("baseToken", {})
        txns = pair.get("txns", {})
        pc = pair.get("priceChange", {})
        vol = pair.get("volume", {})
        info = pair.get("info", {}) or {}
        socials = info.get("socials", []) or []
        websites = info.get("websites", []) or []
        created = pair.get("pairCreatedAt", 0)
        age_sec = (now * 1000 - created) / 1000 if created > 0 else 0
        holders = get_holder_count(mint)
        
        with sol_price_lock:
            current_sol = sol_price_usd
        with fear_greed_lock:
            fg = fear_greed_value
        
        now_dt = datetime.now(timezone.utc)
        
        conn.execute("""
            INSERT OR IGNORE INTO tokens
            (address, symbol, name, dex_id, pair_address, pair_created_at, source,
             detected_at, detected_ts, age_at_detection_sec,
             t0_price, t0_liq, t0_vol_24h, t0_vol_h1, t0_vol_m5, t0_fdv, t0_mcap,
             t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
             t0_price_change_m5, t0_price_change_h1, t0_has_socials,
             t0_holders, t0_sol_price, t0_fear_greed, t0_hour_utc, t0_day_of_week,
             graduation_velocity, velocity_rating,
             creator_wallet, creator_reputation,
             v1_score, v1_pct, v1_verdict)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            mint, base.get("symbol", token["symbol"]), base.get("name", token["name"]),
            dex, pair.get("pairAddress", ""), created, "pre_graduation",
            now_dt.isoformat(), now, age_sec,
            pair.get("priceUsd", "0"), liq,
            vol.get("h24", 0), vol.get("h1", 0), vol.get("m5", 0),
            pair.get("fdv", 0), pair.get("marketCap", 0),
            (txns.get("m5") or {}).get("buys", 0), (txns.get("m5") or {}).get("sells", 0),
            (txns.get("h1") or {}).get("buys", 0), (txns.get("h1") or {}).get("sells", 0),
            pc.get("m5", 0) or 0, pc.get("h1", 0) or 0,
            1 if (len(socials) > 0 or len(websites) > 0) else 0,
            holders, current_sol, fg, now_dt.hour, now_dt.strftime("%A"),
            velocity, vel_rating,
            token["creator_wallet"], token["creator_reputation"],
            token["v1_score"], token["v1_pct"], token["v1_verdict"],
        ))
        stats["tokens_tracked"] += 1
        
        # Generate BUY alert if score is promising + velocity is good
        # Full scoring needs GoPlus + DexScreener data which we now have
        full_score = do_full_score(mint, pair, conn)
        
        if full_score and full_score["pct"] >= SNIPE_THRESHOLD:
            generate_buy_alert(conn, mint, pair, full_score, vel_rating, velocity, grad_time)
        elif full_score and full_score["pct"] >= 65:
            # WATCH alert - not auto-buy but worth monitoring
            generate_watch_alert(conn, mint, pair, full_score, vel_rating)
        
        _log(f"GRADUATED: {token['symbol']} vel={vel_rating} ({grad_time}s) score={full_score['pct'] if full_score else '?'}%")
        time.sleep(0.3)
    
    conn.commit()
    conn.close()


def do_full_score(mint, pair, conn):
    """Run full 33-layer V1 scoring with all available data."""
    # Fetch GoPlus data
    gp_data = fetch_json(f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={mint}")
    gp = None
    if gp_data and gp_data.get("result"):
        gp = gp_data["result"].get(mint.lower()) or gp_data["result"].get(mint)
    
    # Fetch RugCheck data
    rc_data = fetch_json(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
    
    # Run the 33-layer scorer
    score, max_score, details = score_v1(pair, rc_data, gp)
    pct = round(score * 100.0 / max_score, 1) if max_score > 0 else 0
    
    if pct >= SNIPE_THRESHOLD:
        verdict = "SNIPE"
    elif pct >= 65:
        verdict = "WATCH"
    elif pct >= 50:
        verdict = "NEUTRAL"
    else:
        verdict = "SKIP"
    
    # Check for RUGGER override
    if rc_data:
        for risk in (rc_data.get("risks") or []):
            desc = str(risk.get("description", "")).lower()
            if "creator" in desc and "rug" in desc:
                verdict = "RUGGER"
                break
    
    # Update token record
    conn.execute("UPDATE tokens SET v1_score=?, v1_pct=?, v1_verdict=? WHERE address=?",
                (score, pct, verdict, mint))
    
    return {"score": score, "max": max_score, "pct": pct, "verdict": verdict}


def score_v1(pair, rc, gp):
    """33-layer V1 scorer. Returns (score, max_score, details)."""
    score = 0
    max_score = 137
    
    # ── RugCheck layers (44 pts max) ──
    if rc:
        # Mint authority (8 pts)
        if rc.get("mintAuthority") is None:
            score += 8
        # Freeze authority (8 pts)
        if rc.get("freezeAuthority") is None:
            score += 8
        # LP locked (7 pts)
        lp_pct = rc.get("lpLockedPct", 0) or 0
        if lp_pct >= 95: score += 7
        elif lp_pct >= 80: score += 5
        elif lp_pct >= 50: score += 3
        # Creator history (10 pts)
        is_rugger = False
        for risk in (rc.get("risks") or []):
            if "rug" in str(risk.get("description", "")).lower():
                is_rugger = True
                break
        if not is_rugger:
            score += 10
        # Risk score (6 pts)
        rs = rc.get("score", 5000)
        if rs and rs < 500: score += 6
        elif rs and rs < 1000: score += 4
        elif rs and rs < 2000: score += 2
        # LP providers (3 pts)
        # Token program (2 pts)
        if rc.get("tokenProgram") == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
            score += 2
        score += 3  # default LP providers
    
    # ── GoPlus layers (37 pts max) ──
    if gp:
        # Top holder (6 pts)
        top1 = float(gp.get("top_holders", [{}])[0].get("percent", 100) if gp.get("top_holders") else 100)
        if top1 < 10: score += 6
        elif top1 < 20: score += 4
        elif top1 < 50: score += 2
        # Top 5 holders (5 pts)
        if gp.get("top_holders") and len(gp["top_holders"]) >= 5:
            top5 = sum(float(h.get("percent", 0)) for h in gp["top_holders"][:5])
            if top5 < 30: score += 5
            elif top5 < 50: score += 3
            elif top5 < 70: score += 1
        # Holder count (4 pts)
        hc = int(gp.get("holder_count", 0) or 0)
        if hc >= 500: score += 4
        elif hc >= 200: score += 3
        elif hc >= 50: score += 2
        elif hc >= 10: score += 1
        # Closable (5 pts)
        if str(gp.get("closable", "0")) == "0": score += 5
        # Freezable (5 pts)
        if str(gp.get("freezable", "0")) == "0": score += 5
        # Balance mutable (5 pts)
        if str(gp.get("balance_mutable", "0")) == "0": score += 5
        # Creator balance (4 pts)
        cb = float(gp.get("creator_percent", 0) or 0)
        if cb < 5: score += 4
        elif cb < 10: score += 3
        elif cb < 20: score += 1
        # Default account state (3 pts)
        if str(gp.get("default_account_state", "0")) == "0": score += 3
    
    # ── DexScreener layers (31 pts max) ──
    if pair:
        # Liquidity (7 pts)
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        if liq >= 50000: score += 7
        elif liq >= 20000: score += 5
        elif liq >= 10000: score += 3
        elif liq >= 5000: score += 1
        # Volume 24h (5 pts)
        vol = (pair.get("volume") or {}).get("h24", 0)
        if vol >= 500000: score += 5
        elif vol >= 100000: score += 4
        elif vol >= 50000: score += 3
        elif vol >= 10000: score += 1
        # FDV (3 pts)
        fdv = pair.get("fdv", 0)
        if fdv and 50000 <= fdv <= 500000: score += 3
        elif fdv and 500000 < fdv <= 2000000: score += 2
        elif fdv and fdv > 2000000: score += 1
        # Price change 1h (3 pts)
        pc1h = (pair.get("priceChange") or {}).get("h1", 0) or 0
        if pc1h > 20: score += 3
        elif pc1h > 0: score += 2
        elif pc1h > -10: score += 1
        # Price change 24h (3 pts)
        pc24h = (pair.get("priceChange") or {}).get("h24", 0) or 0
        if pc24h > 50: score += 3
        elif pc24h > 0: score += 2
        elif pc24h > -20: score += 1
        # Buy/sell ratio (4 pts)
        txns = pair.get("txns", {})
        b5 = (txns.get("m5") or {}).get("buys", 0)
        s5 = (txns.get("m5") or {}).get("sells", 0)
        if s5 > 0:
            ratio = b5 / s5
            if ratio > 2: score += 4
            elif ratio > 1.5: score += 3
            elif ratio > 1: score += 2
        elif b5 > 0:
            score += 4
        # Has socials (2 pts)
        info = pair.get("info", {}) or {}
        if info.get("socials") or info.get("websites"):
            score += 2
        # Pair age (4 pts)
        created = pair.get("pairCreatedAt", 0)
        if created:
            age_h = (time.time() * 1000 - created) / 3600000
            if 0.5 <= age_h <= 6: score += 4
            elif 6 < age_h <= 24: score += 3
            elif age_h < 0.5: score += 2
    
    # ── Forensic layers (25 pts) ──
    # Simplified: buy size consistency, momentum, organic score, etc.
    if pair:
        txns = pair.get("txns", {})
        b_m5 = (txns.get("m5") or {}).get("buys", 0)
        s_m5 = (txns.get("m5") or {}).get("sells", 0)
        b_h1 = (txns.get("h1") or {}).get("buys", 0)
        s_h1 = (txns.get("h1") or {}).get("sells", 0)
        
        # Transaction count min (3 pts)
        total = b_m5 + s_m5
        if total >= 20: score += 3
        elif total >= 10: score += 2
        elif total >= 5: score += 1
        
        # Sell pressure (4 pts)
        if s_m5 > 0 and b_m5 > 0:
            sp = s_m5 / (b_m5 + s_m5)
            if sp < 0.3: score += 4
            elif sp < 0.4: score += 3
            elif sp < 0.5: score += 2
        elif b_m5 > 0:
            score += 4
        
        # Volume/liquidity ratio (2 pts)
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        vol = (pair.get("volume") or {}).get("h24", 0)
        if liq > 0 and vol > 0:
            vl = vol / liq
            if 1 <= vl <= 20: score += 2
            elif vl < 1: score += 1
        
        # Organic score (5 pts)
        if b_h1 > 20 and s_h1 > 5:
            score += 5
        elif b_h1 > 10:
            score += 3
        elif b_h1 > 5:
            score += 1
        
        # Momentum (3 pts) - careful, inverse signal!
        # Lower weight due to inverse correlation finding
        pc5 = (pair.get("priceChange") or {}).get("m5", 0) or 0
        if 0 < pc5 < 20: score += 3  # Moderate positive = good
        elif pc5 >= 20: score += 1    # Too hot = risky
        elif pc5 > -10: score += 2    # Slightly down = ok
        
        # Buy size consistency (3 pts)
        score += 3  # Default: assume consistent until proven otherwise
        
        # Graduated from pump.fun (5 pts)
        dex = pair.get("dexId", "")
        if dex == "pumpswap":
            score += 5
        elif dex == "raydium":
            score += 3
    
    return score, max_score, {}


# ── PHASE 3: Alert Generation ───────────────────────────────────────────

def generate_buy_alert(conn, mint, pair, score_data, vel_rating, velocity, grad_time):
    """Generate a BUY alert with one-click link."""
    now = int(time.time())
    symbol = pair.get("baseToken", {}).get("symbol", "???")
    price = pair.get("priceUsd", "0")
    liq = (pair.get("liquidity") or {}).get("usd", 0)
    fdv = pair.get("fdv", 0)
    pair_addr = pair.get("pairAddress", "")
    
    photon_link = f"https://photon-sol.tinyastro.io/en/lp/{pair_addr}" if pair_addr else ""
    dex_link = f"https://dexscreener.com/solana/{mint}"
    
    vel_emoji = {"ROCKET": "🚀🚀🚀", "FAST": "🚀🚀", "NORMAL": "🚀", "SLOW": "🐌"}.get(vel_rating, "")
    
    message = (
        f"🟢 SNIPE: {symbol} — {score_data['pct']}% ({score_data['score']}/137)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Velocity: {vel_rating} {vel_emoji} (graduated in {grad_time}s)\n"
        f"Liq: ${liq:,.0f} | FDV: ${fdv:,.0f}\n"
        f"CA: {mint}\n\n"
        f"👉 BUY: {photon_link}\n"
        f"📊 Chart: {dex_link}\n\n"
        f"Exit plan: Sell 50% at +100%, ride rest. Stop loss at -8%."
    )
    
    conn.execute("""
        INSERT INTO alerts (alert_type, token_address, symbol, created_at, created_ts,
            score, score_pct, verdict, velocity_rating, graduation_velocity,
            price_at_alert, liq_at_alert, fdv_at_alert, photon_link, dexscreener_link, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("BUY", mint, symbol, datetime.now(timezone.utc).isoformat(), now,
          score_data["score"], score_data["pct"], score_data["verdict"],
          vel_rating, velocity, price, liq, fdv, photon_link, dex_link, message))
    
    # Create active position for exit monitoring
    conn.execute("""
        INSERT OR IGNORE INTO positions (token_address, symbol, entry_price, entry_ts, entry_liq, status)
        VALUES (?, ?, ?, ?, ?, 'active')
    """, (mint, symbol, price, now, liq))
    
    stats["alerts_generated"] += 1
    _log(f"🟢 BUY ALERT: {symbol} {score_data['pct']}% vel={vel_rating}")


def generate_watch_alert(conn, mint, pair, score_data, vel_rating):
    """Generate a WATCH alert for promising but not SNIPE-level tokens."""
    now = int(time.time())
    symbol = pair.get("baseToken", {}).get("symbol", "???")
    liq = (pair.get("liquidity") or {}).get("usd", 0)
    dex_link = f"https://dexscreener.com/solana/{mint}"
    
    message = f"🟡 WATCH: {symbol} — {score_data['pct']}% | Liq ${liq:,.0f} | {vel_rating}\n📊 {dex_link}"
    
    conn.execute("""
        INSERT INTO alerts (alert_type, token_address, symbol, created_at, created_ts,
            score, score_pct, verdict, velocity_rating, price_at_alert, liq_at_alert, dexscreener_link, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("WATCH", mint, symbol, datetime.now(timezone.utc).isoformat(), now,
          score_data["score"], score_data["pct"], score_data["verdict"],
          vel_rating, pair.get("priceUsd", "0"), liq, dex_link, message))


# ── PHASE 4: Exit Signal Monitor ────────────────────────────────────────

def _monitor_pos():
    """Check active positions for exit signals."""
    conn = get_db()
    now = int(time.time())
    
    positions = conn.execute("SELECT * FROM positions WHERE status='active'").fetchall()
    
    for pos in positions:
        addr = pos["token_address"]
        elapsed = now - pos["entry_ts"]
        
        # Time stop: after 20 minutes, force exit review
        if elapsed > 1200:
            generate_exit_alert(conn, pos, "TIME_STOP", "20 min elapsed — review position")
            continue
        
        pair = get_pair_data(addr)
        if not pair:
            continue
        
        price = pair.get("priceUsd", "0")
        ret = calc_return(pos["entry_price"], price)
        
        if ret is None:
            continue
        
        # Update position
        peak_ret = pos["peak_return"] or -999
        if ret > peak_ret:
            conn.execute("UPDATE positions SET peak_price=?, peak_return=?, current_return=?, last_check_ts=? WHERE token_address=?",
                        (price, ret, ret, now, addr))
        else:
            conn.execute("UPDATE positions SET current_return=?, last_check_ts=? WHERE token_address=?",
                        (ret, now, addr))
        
        # ── Exit conditions ──
        
        # Hard stop loss
        if ret <= -8:
            generate_exit_alert(conn, pos, "STOP_LOSS", f"Hit -8% stop loss ({ret:+.1f}%)")
            continue
        
        # Peak drawdown: if we were up 50%+ but gave back more than half
        if peak_ret > 50 and ret < peak_ret * 0.5:
            generate_exit_alert(conn, pos, "PEAK_DRAWDOWN", f"Was +{peak_ret:.0f}%, now +{ret:.0f}% — sell before more loss")
            continue
        
        # Buy/sell momentum check
        txns = pair.get("txns", {})
        buys = (txns.get("m5") or {}).get("buys", 0)
        sells = (txns.get("m5") or {}).get("sells", 0)
        
        if sells > buys * 1.5 and elapsed > 30:
            generate_exit_alert(conn, pos, "MOMENTUM_DEATH", f"Sellers dominating ({sells}s vs {buys}b) at {ret:+.1f}%")
            continue
        
        # Volume death
        vol = (pair.get("volume") or {}).get("m5", 0)
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        if liq > 0 and vol == 0 and elapsed > 60:
            generate_exit_alert(conn, pos, "VOLUME_DEATH", f"Zero volume at {ret:+.1f}%")
            continue
        
        # Take profit signals
        if ret >= 100 and elapsed > 15:
            # House money alert
            generate_exit_alert(conn, pos, "HOUSE_MONEY", f"🏦 +{ret:.0f}% — SELL 50%, ride the rest FREE")
            # Don't close position, just alert
            continue
        
        time.sleep(0.3)
    
    conn.commit()
    conn.close()


def generate_exit_alert(conn, pos, reason, detail):
    """Generate an EXIT alert."""
    now = int(time.time())
    ret = pos["current_return"] or 0
    
    emoji = {"STOP_LOSS": "🔴", "PEAK_DRAWDOWN": "🟠", "MOMENTUM_DEATH": "🔴",
             "VOLUME_DEATH": "⚫", "TIME_STOP": "⏰", "HOUSE_MONEY": "🏦"}.get(reason, "🔴")
    
    message = f"{emoji} EXIT: {pos['symbol']} — {detail}"
    
    conn.execute("""
        INSERT INTO alerts (alert_type, token_address, symbol, created_at, created_ts,
            exit_reason, return_pct, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, ("EXIT", pos["token_address"], pos["symbol"],
          datetime.now(timezone.utc).isoformat(), now, reason, ret, message))
    
    if reason != "HOUSE_MONEY":
        conn.execute("UPDATE positions SET status='closed', exit_reason=?, exit_return=? WHERE token_address=?",
                    (reason, ret, pos["token_address"]))
    
    stats["exit_signals"] += 1
    _log(f"{emoji} EXIT: {pos['symbol']} reason={reason} ret={ret:+.1f}%")


# ── PHASE 5: DexScreener Discovery (backup) ────────────────────────────

def _discover_dx():
    """Backup discovery for tokens that bypass Pump.fun monitoring."""
    conn = get_db()
    known = set(r[0] for r in conn.execute("SELECT address FROM tokens").fetchall())
    watched = set(r[0] for r in conn.execute("SELECT mint FROM watchlist").fetchall())
    conn.close()
    
    new_tokens = 0
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
                    if addr and addr not in known and addr not in watched:
                        candidates[addr] = name
    
    for addr, src in list(candidates.items())[:10]:
        pair = get_pair_data(addr)
        if not pair:
            continue
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        dex = pair.get("dexId", "")
        if dex not in ("raydium", "pumpswap", "orca") or liq < 5000:
            continue
        
        # Record
        now = int(time.time())
        base = pair.get("baseToken", {})
        created = pair.get("pairCreatedAt", 0)
        age_sec = (now * 1000 - created) / 1000 if created > 0 else 0
        txns = pair.get("txns", {})
        pc = pair.get("priceChange", {})
        vol = pair.get("volume", {})
        info = pair.get("info", {}) or {}
        socials = info.get("socials", []) or []
        websites = info.get("websites", []) or []
        
        with sol_price_lock: current_sol = sol_price_usd
        with fear_greed_lock: fg = fear_greed_value
        now_dt = datetime.now(timezone.utc)
        
        conn2 = get_db()
        conn2.execute("""
            INSERT OR IGNORE INTO tokens
            (address, symbol, name, dex_id, pair_address, pair_created_at, source,
             detected_at, detected_ts, age_at_detection_sec,
             t0_price, t0_liq, t0_vol_24h, t0_vol_h1, t0_vol_m5, t0_fdv, t0_mcap,
             t0_buys_m5, t0_sells_m5, t0_buys_h1, t0_sells_h1,
             t0_price_change_m5, t0_price_change_h1, t0_has_socials,
             t0_holders, t0_sol_price, t0_fear_greed, t0_hour_utc, t0_day_of_week,
             graduation_velocity, velocity_rating)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            addr, base.get("symbol", "???"), base.get("name", ""),
            dex, pair.get("pairAddress", ""), created, f"dexscreener_{src}",
            now_dt.isoformat(), now, age_sec,
            pair.get("priceUsd", "0"), liq,
            vol.get("h24", 0), vol.get("h1", 0), vol.get("m5", 0),
            pair.get("fdv", 0), pair.get("marketCap", 0),
            (txns.get("m5") or {}).get("buys", 0), (txns.get("m5") or {}).get("sells", 0),
            (txns.get("h1") or {}).get("buys", 0), (txns.get("h1") or {}).get("sells", 0),
            pc.get("m5", 0) or 0, pc.get("h1", 0) or 0,
            1 if (len(socials) > 0 or len(websites) > 0) else 0,
            get_holder_count(addr), current_sol, fg, now_dt.hour, now_dt.strftime("%A"),
            0, "UNKNOWN",
        ))
        conn2.commit()
        conn2.close()
        new_tokens += 1
        stats["tokens_tracked"] += 1
        time.sleep(0.25)
    
    stats["scans"] += 1
    if new_tokens > 0:
        _log(f"DX backup: {new_tokens} new tokens")
    return new_tokens


# ── Snapshot Engine ─────────────────────────────────────────────────────

def _take_snaps():
    conn = get_db()
    now = int(time.time())
    with sol_price_lock: current_sol = sol_price_usd

    tokens = conn.execute("""
        SELECT address, detected_ts, t0_price, t0_liq, t0_buys_m5, t0_sells_m5, t0_vol_m5, peak_return
        FROM tokens WHERE snapshots_complete=0 AND (?-detected_ts)<=700
    """, (now,)).fetchall()

    for token in tokens:
        addr = token["address"]
        elapsed = now - token["detected_ts"]
        existing = set(r[0] for r in conn.execute("SELECT target_sec FROM snapshots WHERE token_address=?", (addr,)).fetchall())
        due = [t for t in SNAPSHOT_TIMES if t not in existing and elapsed >= t and elapsed <= t + 60]
        if not due:
            if elapsed > max(SNAPSHOT_TIMES) + 60:
                conn.execute("UPDATE tokens SET snapshots_complete=1 WHERE address=?", (addr,))
            continue
        pair = get_pair_data(addr)
        if not pair: continue

        price = pair.get("priceUsd", "0")
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        vol_m5 = (pair.get("volume") or {}).get("m5", 0)
        buys_m5 = (pair.get("txns", {}).get("m5") or {}).get("buys", 0)
        sells_m5 = (pair.get("txns", {}).get("m5") or {}).get("sells", 0)
        fdv = pair.get("fdv", 0)
        ret = calc_return(token["t0_price"], price)
        liq_change = round((liq - token["t0_liq"]) / token["t0_liq"] * 100, 2) if token["t0_liq"] > 0 else 0
        bs_ratio = round(buys_m5 / sells_m5, 2) if sells_m5 > 0 else (99.0 if buys_m5 > 0 else 0.0)
        
        prev = conn.execute("SELECT buy_sell_ratio, vol_m5 FROM snapshots WHERE token_address=? ORDER BY target_sec DESC LIMIT 1", (addr,)).fetchone()
        bs_momentum = round(bs_ratio - (prev["buy_sell_ratio"] or 0), 2) if prev and prev["buy_sell_ratio"] else 0.0
        prev_vol = prev["vol_m5"] if prev else (token["t0_vol_m5"] or 0)
        vol_velocity = round((vol_m5 - prev_vol) / prev_vol * 100, 2) if prev_vol and prev_vol > 0 else 0.0

        for target in due:
            conn.execute("""
                INSERT OR IGNORE INTO snapshots
                (token_address, target_sec, actual_sec, taken_at, price, liq, vol_m5,
                 buys_m5, sells_m5, fdv, return_pct, liq_change_pct,
                 buy_sell_ratio, buy_sell_momentum, volume_velocity, holders, sol_price)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (addr, target, elapsed, now, price, liq, vol_m5, buys_m5, sells_m5,
                  fdv, ret, liq_change, bs_ratio, bs_momentum, vol_velocity, None, current_sol))
            stats["snapshots_taken"] += 1

        if ret is not None and (token["peak_return"] is None or ret > (token["peak_return"] or -999)):
            conn.execute("UPDATE tokens SET peak_price=?, peak_return=?, peak_time_sec=? WHERE address=?",
                        (price, ret, elapsed, addr))
        time.sleep(0.3)
    conn.commit()
    conn.close()


# ── Pattern Classification ──────────────────────────────────────────────

def _classify():
    conn = get_db()
    tokens = conn.execute("SELECT address FROM tokens WHERE snapshots_complete=1 AND pattern IS NULL").fetchall()
    for token in tokens:
        addr = token["address"]
        snaps = conn.execute("SELECT target_sec, return_pct FROM snapshots WHERE token_address=? ORDER BY target_sec", (addr,)).fetchall()
        if len(snaps) < 5:
            conn.execute("UPDATE tokens SET pattern='insufficient_data' WHERE address=?", (addr,))
            continue
        returns = [s["return_pct"] or 0 for s in snaps]
        times = [s["target_sec"] for s in snaps]
        peak = max(returns)
        final = returns[-1]
        best_idx = returns.index(peak)
        if peak > 50 and final < peak * 0.3: pattern = "pump_dump"
        elif final > 20: pattern = "rocket" if all(returns[i] >= returns[i-1] - 2 for i in range(1, min(6, len(returns)))) else "slow_climb"
        elif final < -30: pattern = "rug"
        elif abs(final) < 5: pattern = "flat"
        elif peak > 20 and final > 0: pattern = "volatile_up"
        else: pattern = "volatile_down"
        conn.execute("UPDATE tokens SET pattern=?, best_buy_sec=0, best_sell_sec=?, max_profit_pct=? WHERE address=?",
                     (pattern, times[best_idx], peak, addr))
    conn.commit()
    conn.close()


# ── Main Loop ───────────────────────────────────────────────────────────

def _safe(label, fn, *args, max_sec=30):
    """Run a function safely. Simple try/except — no threads."""
    try:
        _log(f"  → {label}")
        result = fn(*args)
        _log(f"  ✓ {label}")
        return result
    except Exception as e:
        stats["errors"] += 1
        _log(f"  ✗ {label}: {e}")
        return None

def main_loop():
    global sol_price_usd
    _log("=== SNIPER v6 MAIN LOOP STARTED ===")
    cycle = 0
    while True:
        try:
            # Update market data every ~60s
            if cycle % 4 == 0:
                p = _safe("sol_price", get_sol_price)
                if p and p > 0:
                    with sol_price_lock: sol_price_usd = p
                    if cycle == 0: _log(f"SOL: ${p}")
            if cycle % 40 == 0:
                _safe("fear_greed", update_fear_greed)
            
            # Each phase gets its OWN db connection — no long holds
            _safe("pumpfun", _check_pumpfun)
            
            if cycle % 2 == 0:
                _safe("pre_score", _pre_score)
            
            _safe("graduations", _check_grads)
            _safe("positions", _monitor_pos)
            _safe("snapshots", _take_snaps)
            
            if cycle % 4 == 0:
                _safe("dexscreener", _discover_dx)
            
            if cycle % 20 == 0:
                _safe("classify", _classify)
            
            if cycle % 100 == 0:
                try:
                    conn = get_db()
                    cutoff = int(time.time()) - 3600
                    conn.execute("DELETE FROM watchlist WHERE graduated=0 AND first_seen_ts < ?", (cutoff,))
                    conn.commit()
                    conn.close()
                except: pass
            
            _log(f"Cycle {cycle} | W={stats['pre_grad_watched']} T={stats['tokens_tracked']} A={stats['alerts_generated']} E={stats['errors']}")
            cycle += 1
        except Exception as e:
            _log(f"CRITICAL: {e}")
            stats["errors"] += 1
        time.sleep(15)


def _watchdog():
    """Auto-restart main_loop if it dies."""
    while True:
        time.sleep(60)
        alive = False
        for t in threading.enumerate():
            if t.name == "main_loop" and t.is_alive():
                alive = True
                break
        if not alive:
            _log("⚠️ WATCHDOG: main_loop died, restarting...")
            threading.Thread(target=main_loop, daemon=True, name="main_loop").start()


# ── API Routes ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    try:
        conn = get_db()
        total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
        complete = conn.execute("SELECT COUNT(*) as c FROM tokens WHERE snapshots_complete=1").fetchone()["c"]
        snaps = conn.execute("SELECT COUNT(*) as c FROM snapshots").fetchone()["c"]
        watched = conn.execute("SELECT COUNT(*) as c FROM watchlist WHERE graduated=0").fetchone()["c"]
        pending_alerts = conn.execute("SELECT COUNT(*) as c FROM alerts WHERE delivered=0").fetchone()["c"]
        active_pos = conn.execute("SELECT COUNT(*) as c FROM positions WHERE status='active'").fetchone()["c"]
        conn.close()
    except Exception as e:
        return jsonify({"status": "running", "version": "v6-sniper", "db_busy": True, "error": str(e), "stats": stats})
    with sol_price_lock: sol = sol_price_usd
    with fear_greed_lock: fg = fear_greed_value; fl = fear_greed_label
    return jsonify({
        "status": "running", "version": "v6-sniper",
        "sol_price_usd": sol, "fear_greed": {"value": fg, "label": fl},
        "pre_graduation_watchlist": watched,
        "tokens_tracked": total, "complete_curves": complete, "total_snapshots": snaps,
        "pending_alerts": pending_alerts, "active_positions": active_pos,
        "stats": stats,
    })

@app.route("/health")
def health():
    # Verify main loop is alive
    alive = any(t.name == "main_loop" and t.is_alive() for t in threading.enumerate())
    if alive:
        return "OK"
    else:
        return "LOOP_DEAD", 500

@app.route("/api/threads")
def threads():
    tlist = [{"name": t.name, "alive": t.is_alive(), "daemon": t.daemon} for t in threading.enumerate()]
    return jsonify({"threads": tlist, "count": len(tlist)})

@app.route("/logs")
def logs():
    return jsonify({"lines": _log_lines[-100:], "count": len(_log_lines)})

@app.route("/api/alerts/pending")
def get_pending_alerts():
    """Get undelivered alerts — polled by cron for Telegram delivery."""
    conn = get_db()
    alerts = conn.execute("SELECT * FROM alerts WHERE delivered=0 ORDER BY created_ts").fetchall()
    result = [dict(a) for a in alerts]
    # Mark as delivered
    for a in alerts:
        conn.execute("UPDATE alerts SET delivered=1 WHERE id=?", (a["id"],))
    conn.commit()
    conn.close()
    return jsonify({"count": len(result), "alerts": result})

@app.route("/api/alerts")
def get_all_alerts():
    conn = get_db()
    limit = flask_request.args.get("limit", type=int, default=50)
    alerts = conn.execute("SELECT * FROM alerts ORDER BY created_ts DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return jsonify({"count": len(alerts), "alerts": [dict(a) for a in alerts]})

@app.route("/api/watchlist")
def get_watchlist():
    conn = get_db()
    items = conn.execute("SELECT * FROM watchlist WHERE graduated=0 ORDER BY velocity_score DESC LIMIT 50").fetchall()
    conn.close()
    return jsonify({"count": len(items), "watchlist": [dict(i) for i in items]})

@app.route("/api/positions")
def get_positions():
    conn = get_db()
    active = conn.execute("SELECT * FROM positions WHERE status='active'").fetchall()
    closed = conn.execute("SELECT * FROM positions WHERE status='closed' ORDER BY rowid DESC LIMIT 20").fetchall()
    conn.close()
    return jsonify({"active": [dict(p) for p in active], "closed": [dict(p) for p in closed]})

@app.route("/api/graduations")
def get_graduations():
    conn = get_db()
    limit = flask_request.args.get("limit", type=int, default=50)
    tokens = conn.execute("SELECT * FROM tokens ORDER BY detected_ts DESC LIMIT ?", (limit,)).fetchall()
    results = []
    for t in tokens:
        token = dict(t)
        snps = conn.execute("SELECT target_sec, price, return_pct, buy_sell_ratio, buy_sell_momentum, volume_velocity FROM snapshots WHERE token_address=? ORDER BY target_sec", (t["address"],)).fetchall()
        token["curve"] = [dict(s) for s in snps]
        results.append(token)
    conn.close()
    return jsonify({"count": len(results), "graduations": results})

@app.route("/api/curves")
def get_curves():
    conn = get_db()
    tokens = conn.execute("SELECT address, pattern, peak_return, peak_time_sec, velocity_rating, v1_pct FROM tokens WHERE snapshots_complete=1").fetchall()
    if not tokens:
        conn.close()
        return jsonify({"message": "No complete curves yet.", "total": 0})
    time_buckets = {}
    for target in SNAPSHOT_TIMES:
        rows = conn.execute("SELECT return_pct, buy_sell_ratio, buy_sell_momentum, volume_velocity FROM snapshots WHERE target_sec=? AND return_pct IS NOT NULL", (target,)).fetchall()
        if rows:
            returns = [r["return_pct"] for r in rows]
            winners = [r for r in returns if r > 0]
            time_buckets[f"{target}s"] = {
                "tokens": len(returns),
                "avg_return": round(sum(returns)/len(returns), 2),
                "win_rate": round(len(winners)/len(returns)*100, 1),
                "best": round(max(returns), 2), "worst": round(min(returns), 2),
            }
    patterns = {}
    velocity_impact = {}
    for t in tokens:
        p = t["pattern"] or "unknown"
        patterns[p] = patterns.get(p, 0) + 1
        v = t["velocity_rating"] or "UNKNOWN"
        if v not in velocity_impact: velocity_impact[v] = {"count": 0, "peaks": []}
        velocity_impact[v]["count"] += 1
        velocity_impact[v]["peaks"].append(t["peak_return"] or 0)
    for v in velocity_impact:
        peaks = velocity_impact[v]["peaks"]
        velocity_impact[v] = {"count": len(peaks), "avg_peak": round(sum(peaks)/len(peaks), 1) if peaks else 0}
    conn.close()
    return jsonify({
        "total": len(tokens), "price_curves": time_buckets,
        "patterns": patterns, "velocity_impact": velocity_impact,
    })

@app.route("/api/stats")
def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM tokens").fetchone()["c"]
    alerts_total = conn.execute("SELECT COUNT(*) as c FROM alerts").fetchone()["c"]
    buy_alerts = conn.execute("SELECT COUNT(*) as c FROM alerts WHERE alert_type='BUY'").fetchone()["c"]
    exit_alerts = conn.execute("SELECT COUNT(*) as c FROM alerts WHERE alert_type='EXIT'").fetchone()["c"]
    conn.close()
    with sol_price_lock: sol = sol_price_usd
    return jsonify({
        "version": "v6-sniper", "sol_usd": sol,
        "tokens_tracked": total, "total_alerts": alerts_total,
        "buy_alerts": buy_alerts, "exit_alerts": exit_alerts,
        "stats": stats,
    })


# ── Start ───────────────────────────────────────────────────────────────

init_db()

_started = False
def start_all():
    global _started
    if not _started:
        _started = True
        _log("Starting Graduation Sniper v6...")
        threading.Thread(target=main_loop, daemon=True, name="main_loop").start()
        threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()
        _log("🎯 SNIPER v6 LIVE — Pre-graduation detection + instant alerts + exit signals")

start_all()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
