#!/usr/bin/env python3
"""
Graduation Detector V8 — Pure Data Collector
=============================================
Record the price of EVERY graduated token every 5 seconds for the first 60 seconds,
then every 30 seconds for 9 more minutes (10 min total). No alerts, no entry/exit logic.
Just observe and record.

Version: v8-collector
Run: gunicorn collector_v8:app --bind 0.0.0.0:8080 --workers 1 --threads 2
"""

import sqlite3, time, json, threading, logging, os, contextlib, traceback
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from flask import Flask, g, jsonify, request

# ── Config ────────────────────────────────────────────────────────────────────
VERSION = "v8-collector"
DB_PATH = os.environ.get("DB_PATH", "collector_v8.db")
FETCH_TIMEOUT = 5
FETCH_MAX_BYTES = 500_000  # 500KB cap
LOG_LINES = 2000

PUMP_LIVE_URL = "https://frontend-api-v3.pump.fun/coins/currently-live"
PUMP_GRADUATED_URL = "https://frontend-api-v3.pump.fun/coins?limit=50&sort=created_timestamp&order=DESC&complete=true&includeNsfw=false"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"
DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
COINGECKO_SOL_URL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
RUGCHECK_URL = "https://api.rugcheck.xyz/v1/tokens/{address}/report/summary"
FNG_URL = "https://api.alternative.me/fng/?limit=1"
HELIUS_KEY = os.environ.get("HELIUS_API_KEY", "b85d5357-36d9-4e26-b945-f38a0677b391")
HELIUS_TX_URL = "https://api.helius.xyz/v0/addresses/{address}/transactions?api-key=" + HELIUS_KEY + "&type=SWAP"

FIRST_MINUTE_SECS = 60
EXTENDED_MONITOR_SECS = 600  # 10 minutes total

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("v8")

log_buffer = []
log_buffer_lock = threading.Lock()

class BufferHandler(logging.Handler):
    def emit(self, record):
        line = self.format(record)
        with log_buffer_lock:
            log_buffer.append(line)
            if len(log_buffer) > LOG_LINES:
                del log_buffer[: len(log_buffer) - LOG_LINES]

bh = BufferHandler()
bh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(bh)

# ── Global state ──────────────────────────────────────────────────────────────
sol_price_usd = 0.0
sol_price_lock = threading.Lock()
fng_value_global = 50
fng_lock = threading.Lock()
_started = False
_start_lock = threading.Lock()

# Tracking sets — tokens currently being monitored
# {address: detected_at}  for first-minute tokens
first_minute_tokens: dict[str, float] = {}
# {address: detected_at}  for extended monitoring tokens
extended_tokens: dict[str, float] = {}
# All known graduated addresses (avoid re-detection)
known_graduated: set[str] = set()
# Pre-graduation tracking: {address: data_dict}
pre_grad_cache: dict[str, dict] = {}
tracking_lock = threading.Lock()

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)

# ── DB context manager ────────────────────────────────────────────────────────
@contextlib.contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
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
            CREATE TABLE IF NOT EXISTS tokens (
                address TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                detected_at REAL,
                first_price REAL,
                first_mcap REAL,
                peak_price REAL,
                peak_mcap REAL,
                peak_time REAL,
                mint_revoked INTEGER DEFAULT -1,
                status TEXT DEFAULT 'collecting',
                last_update REAL,
                pre_participants INTEGER,
                pre_replies INTEGER,
                pre_curve_pct REAL,
                pre_velocity REAL,
                pre_ath_mcap REAL,
                pre_creator TEXT,
                pre_created_ts REAL,
                curve_duration_sec REAL,
                graduation_hour INTEGER,
                graduation_dow INTEGER,
                sol_price_at_grad REAL,
                fng_at_grad INTEGER,
                has_website INTEGER DEFAULT 0,
                has_twitter INTEGER DEFAULT 0,
                has_telegram INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT,
                ts REAL,
                age_seconds REAL,
                price REAL,
                volume_m5 REAL,
                buys_m5 INTEGER,
                sells_m5 INTEGER,
                liquidity REAL,
                mcap REAL,
                price_change_m5 REAL
            );

            CREATE INDEX IF NOT EXISTS idx_snap_addr ON snapshots(address, ts);

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT,
                ts REAL,
                age_seconds REAL,
                signature TEXT UNIQUE,
                direction TEXT,
                sol_amount REAL,
                token_amount REAL,
                wallet TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trades_addr ON trades(address, ts);

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
                creator TEXT,
                ath_market_cap REAL
            );
        """)
    log.info("DB initialized")

# ── Fetch helper ──────────────────────────────────────────────────────────────
def fetch_json(url, timeout=FETCH_TIMEOUT):
    """Fetch JSON with 500KB cap and timeout."""
    try:
        req = Request(url, headers={"User-Agent": "collector-v8/1.0", "Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read(FETCH_MAX_BYTES)
            return json.loads(data)
    except (URLError, HTTPError, json.JSONDecodeError, OSError) as e:
        log.warning(f"fetch_json failed for {url[:80]}: {e}")
        return None

# ── SOL price ─────────────────────────────────────────────────────────────────
def refresh_sol_price():
    global sol_price_usd
    data = fetch_json(COINGECKO_SOL_URL)
    if data and "solana" in data:
        price = data["solana"].get("usd", 0)
        if price > 0:
            with sol_price_lock:
                sol_price_usd = price
            log.info(f"SOL price: ${price:.2f}")

def get_sol_price():
    with sol_price_lock:
        return sol_price_usd

def refresh_fng():
    global fng_value_global
    data = fetch_json(FNG_URL)
    if data and "data" in data and len(data["data"]):
        try:
            val = int(data["data"][0]["value"])
            with fng_lock:
                fng_value_global = val
        except Exception:
            pass

def get_fng_value():
    with fng_lock:
        return fng_value_global

def fetch_helius_trades(address, detected_at):
    """Fetch recent trades for a token via Helius Enhanced Transactions. Non-blocking."""
    try:
        url = HELIUS_TX_URL.format(address=address)
        data = fetch_json(url, timeout=8)
        if not data or not isinstance(data, list):
            return
        now = time.time()
        with db() as conn:
            for tx in data[:30]:
                sig = tx.get("signature", "")
                ts = tx.get("timestamp", now)
                age = ts - detected_at if ts > detected_at else now - detected_at
                native_transfers = tx.get("nativeTransfers", [])
                token_transfers = tx.get("tokenTransfers", [])
                sol_amt = 0
                tok_amt = 0
                wallet = ""
                direction = "unknown"
                for nt in native_transfers:
                    sol_amt += abs(float(nt.get("amount", 0))) / 1e9
                for tt in token_transfers:
                    if tt.get("mint", "") == address:
                        tok_amt += abs(float(tt.get("tokenAmount", 0)))
                        if tt.get("toUserAccount", ""):
                            wallet = tt["toUserAccount"]
                            direction = "buy"
                        elif tt.get("fromUserAccount", ""):
                            wallet = tt["fromUserAccount"]
                            direction = "sell"
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO trades (address, ts, age_seconds, signature, direction, sol_amount, token_amount, wallet)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (address, ts, age, sig, direction, sol_amt, tok_amt, wallet))
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"Helius trades error {address[:8]}: {e}")

# ── RugCheck (threaded, non-blocking) ─────────────────────────────────────────
def check_rugcheck(address):
    """Single rug check per token on detection. Runs in thread."""
    try:
        url = RUGCHECK_URL.format(address=address)
        data = fetch_json(url, timeout=8)
        if data is None:
            return
        # Look for mint authority status
        mint_revoked = -1
        risks = data.get("risks", [])
        for risk in risks:
            name = risk.get("name", "").lower()
            if "mint" in name:
                if "revoked" in name or "disabled" in name or "immutable" in name:
                    mint_revoked = 1
                else:
                    mint_revoked = 0
                break
        # If no mint risk found but we got data, check top-level
        if mint_revoked == -1 and data.get("tokenMeta"):
            mint_revoked = 1 if data.get("mintAuthority") is None else 0

        with db() as conn:
            conn.execute("UPDATE tokens SET mint_revoked=? WHERE address=?", (mint_revoked, address))
        log.info(f"RugCheck {address[:8]}...: mint_revoked={mint_revoked}")
    except Exception as e:
        log.warning(f"RugCheck error {address[:8]}...: {e}")

# ── DexScreener helpers ───────────────────────────────────────────────────────
def get_best_pair(address, retry=False):
    """
    Fetch DexScreener data for a token. Pick the pair with HIGHEST liquidity.
    Returns (pair_dict, raw_data) or (None, None).
    """
    url = DEXSCREENER_TOKEN_URL.format(address=address)
    data = fetch_json(url)
    if not data:
        return None, None
    pairs = data.get("pairs") or []
    if not pairs:
        if retry:
            time.sleep(2)
            data = fetch_json(url)
            if data:
                pairs = data.get("pairs") or []
        if not pairs:
            return None, data
    best = _pick_best_pair(pairs)
    return best, data

def _pick_best_pair(pairs):
    """Pick pair with highest liquidity from a list of pairs."""
    best = None
    best_liq = -1
    for p in pairs:
        liq = 0
        liq_obj = p.get("liquidity")
        if isinstance(liq_obj, dict):
            liq = liq_obj.get("usd", 0) or 0
        elif isinstance(liq_obj, (int, float)):
            liq = liq_obj
        if liq > best_liq:
            best_liq = liq
            best = p
    # Capture detection latency from pairCreatedAt
    if best and best.get("pairCreatedAt"):
        pair_created_ms = best["pairCreatedAt"]
        pair_created_sec = pair_created_ms / 1000 if pair_created_ms > 1e12 else pair_created_ms
        detection_latency = time.time() - pair_created_sec
        if detection_latency > 0:
            best["_detection_latency_sec"] = round(detection_latency, 1)
    return best

def batch_get_pairs(addresses):
    """Batch fetch DexScreener data for up to 30 tokens in ONE API call.
    Returns {address: best_pair_dict} for tokens that have data."""
    if not addresses:
        return {}
    # DexScreener supports comma-separated addresses, max 30
    results = {}
    for i in range(0, len(addresses), 30):
        batch = addresses[i:i+30]
        joined = ",".join(batch)
        url = DEXSCREENER_TOKEN_URL.format(address=joined)
        data = fetch_json(url)
        if not data:
            continue
        all_pairs = data.get("pairs") or []
        # Group pairs by base token address
        by_token = {}
        for p in all_pairs:
            token_addr = p.get("baseToken", {}).get("address", "")
            if token_addr:
                by_token.setdefault(token_addr, []).append(p)
        # Pick best pair for each token
        for addr in batch:
            token_pairs = by_token.get(addr, [])
            if token_pairs:
                results[addr] = _pick_best_pair(token_pairs)
    return results

def extract_pair_data(pair):
    """Extract snapshot data from a DexScreener pair dict."""
    if not pair:
        return None
    price = float(pair.get("priceUsd") or 0)
    liq_obj = pair.get("liquidity") or {}
    liquidity = liq_obj.get("usd", 0) if isinstance(liq_obj, dict) else (liq_obj or 0)
    mcap = pair.get("marketCap") or pair.get("fdv") or 0
    vol = pair.get("volume", {})
    volume_m5 = vol.get("m5", 0) if isinstance(vol, dict) else 0
    txns = pair.get("txns", {})
    m5_txns = txns.get("m5", {}) if isinstance(txns, dict) else {}
    buys_m5 = m5_txns.get("buys", 0) if isinstance(m5_txns, dict) else 0
    sells_m5 = m5_txns.get("sells", 0) if isinstance(m5_txns, dict) else 0
    pc = pair.get("priceChange", {})
    price_change_m5 = pc.get("m5", 0) if isinstance(pc, dict) else 0
    return {
        "price": price,
        "liquidity": float(liquidity or 0),
        "mcap": float(mcap or 0),
        "volume_m5": float(volume_m5 or 0),
        "buys_m5": int(buys_m5 or 0),
        "sells_m5": int(sells_m5 or 0),
        "price_change_m5": float(price_change_m5 or 0),
    }

# ── Store snapshot ────────────────────────────────────────────────────────────
def store_snapshot(address, detected_at, pair_data):
    """Store a price snapshot and update token record."""
    if not pair_data or pair_data["price"] <= 0:
        return
    now = time.time()
    age = now - detected_at
    price = pair_data["price"]
    mcap = pair_data["mcap"]

    with db() as conn:
        conn.execute("""
            INSERT INTO snapshots (address, ts, age_seconds, price, volume_m5, buys_m5, sells_m5, liquidity, mcap, price_change_m5)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (address, now, age, price, pair_data["volume_m5"], pair_data["buys_m5"],
              pair_data["sells_m5"], pair_data["liquidity"], mcap, pair_data["price_change_m5"]))

        # Update first_price if not set
        row = conn.execute("SELECT first_price, peak_price FROM tokens WHERE address=?", (address,)).fetchone()
        if row:
            updates = {"last_update": now}
            if row["first_price"] is None or row["first_price"] <= 0:
                updates["first_price"] = price
                updates["first_mcap"] = mcap
            # Update peak
            old_peak = row["peak_price"] or 0
            if price > old_peak:
                updates["peak_price"] = price
                updates["peak_mcap"] = mcap
                updates["peak_time"] = now
            set_clause = ", ".join(f"{k}=?" for k in updates)
            vals = list(updates.values()) + [address]
            conn.execute(f"UPDATE tokens SET {set_clause} WHERE address=?", vals)

# ── Pump.fun scanning ────────────────────────────────────────────────────────
def scan_pumpfun():
    """Scan Pump.fun currently-live. Track pre-graduation data. Detect graduations."""
    data = fetch_json(PUMP_LIVE_URL)
    if not data or not isinstance(data, list):
        return []

    new_grads = []
    now = time.time()

    for coin in data:
        try:
            addr = coin.get("mint") or coin.get("address") or ""
            if not addr:
                continue
            symbol = coin.get("symbol", "???")
            name = coin.get("name", "")
            complete = coin.get("complete", False)
            mcap = coin.get("usd_market_cap") or coin.get("market_cap") or 0
            participants = coin.get("num_participants") or coin.get("participantCount") or 0
            replies = coin.get("reply_count") or coin.get("replyCount") or 0
            curve_pct = coin.get("bonding_curve_progress") or coin.get("progress") or 0
            velocity = coin.get("velocity") or 0
            creator = coin.get("creator") or ""
            ath_mcap = coin.get("ath_market_cap") or coin.get("athMarketCap") or 0
            created_ts = coin.get("created_timestamp") or 0
            website = coin.get("website") or ""
            twitter = coin.get("twitter") or ""
            telegram = coin.get("telegram") or ""

            # Update pre-graduation cache
            if addr not in pre_grad_cache:
                pre_grad_cache[addr] = {"first_seen": now}
            pg = pre_grad_cache[addr]
            pg.update({
                "symbol": symbol, "name": name, "mcap": float(mcap or 0),
                "num_participants": int(participants or 0),
                "reply_count": int(replies or 0),
                "curve_pct": float(curve_pct or 0),
                "velocity": float(velocity or 0),
                "last_seen": now, "creator": creator,
                "ath_market_cap": float(ath_mcap or 0),
                "created_timestamp": created_ts,
                "website": website, "twitter": twitter, "telegram": telegram,
            })

            # Store to pre_graduation table
            with db() as conn:
                conn.execute("""
                    INSERT INTO pre_graduation (address, symbol, name, mcap, num_participants, reply_count,
                        curve_pct, velocity, first_seen, last_seen, graduated, creator, ath_market_cap)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    ON CONFLICT(address) DO UPDATE SET
                        mcap=excluded.mcap, num_participants=excluded.num_participants,
                        reply_count=excluded.reply_count, curve_pct=excluded.curve_pct,
                        velocity=excluded.velocity, last_seen=excluded.last_seen,
                        ath_market_cap=excluded.ath_market_cap
                """, (addr, symbol, name, float(mcap or 0), int(participants or 0), int(replies or 0),
                      float(curve_pct or 0), float(velocity or 0), pg["first_seen"], now, creator, float(ath_mcap or 0)))

            # Detect graduation
            if complete and addr not in known_graduated:
                known_graduated.add(addr)
                new_grads.append((addr, symbol, name, pg))
                # Mark graduated in pre_graduation
                with db() as conn:
                    conn.execute("UPDATE pre_graduation SET graduated=1 WHERE address=?", (addr,))
        except Exception as e:
            log.warning(f"scan_pumpfun coin error: {e}")

    return new_grads

def scan_graduated():
    """Scan Pump.fun for ALL recently graduated tokens. This catches 100% of graduations,
    not just the ~40 from currently-live with active livestreams."""
    data = fetch_json(PUMP_GRADUATED_URL)
    if not data or not isinstance(data, list):
        return []

    new_grads = []
    now = time.time()

    for coin in data:
        try:
            addr = coin.get("mint") or coin.get("address") or ""
            if not addr:
                continue
            if addr in known_graduated:
                continue

            symbol = coin.get("symbol", "???")
            name = coin.get("name", "")
            participants = coin.get("num_participants") or 0
            replies = coin.get("reply_count") or 0
            creator = coin.get("creator") or ""
            ath_mcap = coin.get("ath_market_cap") or 0
            created_ts = coin.get("created_timestamp") or 0
            website = coin.get("website") or ""
            twitter = coin.get("twitter") or ""
            telegram = coin.get("telegram") or ""

            known_graduated.add(addr)
            pg = {
                "symbol": symbol, "name": name, "mcap": 0,
                "num_participants": int(participants or 0),
                "reply_count": int(replies or 0),
                "curve_pct": 100.0, "velocity": 0,
                "first_seen": now, "last_seen": now,
                "creator": creator, "ath_market_cap": float(ath_mcap or 0),
                "created_timestamp": created_ts,
                "website": website, "twitter": twitter, "telegram": telegram,
            }
            new_grads.append((addr, symbol, name, pg))
        except Exception as e:
            log.warning(f"scan_graduated coin error: {e}")

    if new_grads:
        log.info(f"🔍 Graduated scan found {len(new_grads)} NEW tokens")
    return new_grads

def scan_dexscreener_new():
    """Scan DexScreener profiles + boosts for NEW tokens across ALL DEXes (Meteora, Raydium, etc).
    Returns list of (address, symbol, name, data_dict) for tokens not already known."""
    new_tokens = []
    seen_addrs = set()

    for url in [DEXSCREENER_PROFILES_URL, DEXSCREENER_BOOSTS_URL]:
        try:
            data = fetch_json(url)
            if not data or not isinstance(data, list):
                continue
            for item in data:
                if item.get("chainId") != "solana":
                    continue
                addr = item.get("tokenAddress", "")
                if not addr or addr in known_graduated or addr in seen_addrs:
                    continue
                seen_addrs.add(addr)
                # We'll check if this is a new token by querying DexScreener for its pair age
                new_tokens.append(addr)
        except Exception as e:
            log.warning(f"DexScreener new scan error: {e}")

    if not new_tokens:
        return []

    # Batch query to get pair data and filter for tokens < 15 min old
    pairs = batch_get_pairs(new_tokens[:30])
    new_grads = []
    now = time.time()

    for addr, pair in pairs.items():
        if addr in known_graduated:
            continue
        created = pair.get("pairCreatedAt", 0)
        created_sec = created / 1000 if created > 1e12 else created
        age_sec = now - created_sec if created_sec else 99999

        # Only track tokens less than 15 minutes old
        if age_sec > 900:
            continue

        sym = pair.get("baseToken", {}).get("symbol", "???")
        name = pair.get("baseToken", {}).get("name", "")
        dex = pair.get("dexId", "unknown")
        known_graduated.add(addr)

        pg = {
            "symbol": sym, "name": name, "mcap": 0,
            "num_participants": 0, "reply_count": 0,
            "curve_pct": 100.0, "velocity": 0,
            "first_seen": now, "last_seen": now,
            "creator": "", "ath_market_cap": 0,
            "created_timestamp": created,
            "website": "", "twitter": "", "telegram": "",
            "source_dex": dex,  # Track which DEX this came from
        }
        new_grads.append((addr, sym, name, pg))

    if new_grads:
        dexes = set(pg.get("source_dex", "?") for _, _, _, pg in new_grads)
        log.info(f"🌐 DexScreener scan found {len(new_grads)} NEW tokens from {dexes}")
    return new_grads

def register_graduation(address, symbol, name, pre_data):
    """Register a newly graduated token for monitoring."""
    now = time.time()
    pg = pre_data or {}

    # Calculate bonding curve duration (creation to graduation)
    created_ts = pg.get("created_timestamp", 0)
    if created_ts and created_ts > 1e12:
        created_ts = created_ts / 1000  # ms to sec
    curve_duration = (now - created_ts) if created_ts else None

    # Time context
    grad_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    grad_hour = grad_dt.hour
    grad_dow = grad_dt.weekday()  # 0=Mon, 6=Sun

    # Social presence from Pump.fun data
    has_website = 1 if pg.get("website") else 0
    has_twitter = 1 if pg.get("twitter") else 0
    has_telegram = 1 if pg.get("telegram") else 0

    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO tokens (address, symbol, name, detected_at, status, last_update,
                pre_participants, pre_replies, pre_curve_pct, pre_velocity, pre_ath_mcap, pre_creator,
                pre_created_ts, curve_duration_sec, graduation_hour, graduation_dow,
                sol_price_at_grad, fng_at_grad, has_website, has_twitter, has_telegram)
            VALUES (?, ?, ?, ?, 'collecting', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (address, symbol, name, now, now,
              pg.get("num_participants", 0), pg.get("reply_count", 0),
              pg.get("curve_pct", 0), pg.get("velocity", 0),
              pg.get("ath_market_cap", 0), pg.get("creator", ""),
              created_ts, curve_duration, grad_hour, grad_dow,
              get_sol_price(), get_fng_value(), has_website, has_twitter, has_telegram))

    with tracking_lock:
        first_minute_tokens[address] = now

    # Fire off RugCheck in background
    threading.Thread(target=check_rugcheck, args=(address,), daemon=True).start()

    dur_str = f" curve_dur={curve_duration:.0f}s" if curve_duration else ""
    log.info(f"🎓 GRADUATED: {symbol} ({address[:8]}...) participants={pg.get('num_participants',0)} replies={pg.get('reply_count',0)}{dur_str}")

# ── Main loop ─────────────────────────────────────────────────────────────────
def main_loop():
    """Background thread: 3-second tick loop."""
    log.info(f"Main loop starting — {VERSION}")
    refresh_sol_price()
    tick = 0
    last_sol_refresh = time.time()
    last_extended_poll = time.time()

    # Reload any in-progress tokens from DB
    try:
        with db() as conn:
            rows = conn.execute("SELECT address, detected_at FROM tokens WHERE status='collecting'").fetchall()
            now = time.time()
            for row in rows:
                addr, det = row["address"], row["detected_at"]
                known_graduated.add(addr)
                age = now - det
                if age < FIRST_MINUTE_SECS:
                    with tracking_lock:
                        first_minute_tokens[addr] = det
                elif age < EXTENDED_MONITOR_SECS:
                    with tracking_lock:
                        extended_tokens[addr] = det
                else:
                    conn.execute("UPDATE tokens SET status='complete' WHERE address=?", (addr,))
            if rows:
                log.info(f"Reloaded {len(rows)} in-progress tokens from DB")
    except Exception as e:
        log.warning(f"Reload error: {e}")

    while True:
        loop_start = time.time()
        tick += 1

        # ── Section 1a: Pump.fun currently-live scan (every other tick ≈ every 6 sec) ──
        if tick % 2 == 0:
            try:
                new_grads = scan_pumpfun()
                for addr, sym, name, pg in new_grads:
                    register_graduation(addr, sym, name, pg)
            except Exception as e:
                log.error(f"Pump.fun live scan error: {e}\n{traceback.format_exc()}")

        # ── Section 1b: Graduated tokens scan (EVERY TICK ≈ every 3 sec for fastest detection) ──
        try:
            new_grads = scan_graduated()
            for addr, sym, name, pg in new_grads:
                register_graduation(addr, sym, name, pg)
        except Exception as e:
            log.error(f"Graduated scan error: {e}\n{traceback.format_exc()}")

        # ── Section 1c: DexScreener profiles/boosts for Meteora/Raydium (every 2nd tick ≈ 6 sec) ──
        if tick % 2 == 1:
            try:
                new_tokens = scan_dexscreener_new()
                for addr, sym, name, pg in new_tokens:
                    register_graduation(addr, sym, name, pg)
            except Exception as e:
                log.error(f"DexScreener new scan error: {e}\n{traceback.format_exc()}")

        # ── Section 2: First-minute tokens (BATCH DexScreener every tick ≈ 3 sec) ──
        try:
            now = time.time()
            promote_to_extended = []
            with tracking_lock:
                fm_copy = dict(first_minute_tokens)

            # Split into active first-minute tokens vs ready-to-promote
            active_addrs = []
            for addr, det_at in fm_copy.items():
                age = now - det_at
                if age >= FIRST_MINUTE_SECS:
                    promote_to_extended.append(addr)
                else:
                    active_addrs.append(addr)

            # BATCH query — 1 API call for ALL first-minute tokens (up to 30)
            if active_addrs:
                pair_data = batch_get_pairs(active_addrs)
                for addr in active_addrs:
                    pair = pair_data.get(addr)
                    if pair:
                        pd = extract_pair_data(pair)
                        if pd and pd["price"] > 0:
                            det_at = fm_copy.get(addr, now)
                            store_snapshot(addr, det_at, pd)

            # Promote tokens past 60 sec
            for addr in promote_to_extended:
                with tracking_lock:
                    det_at = first_minute_tokens.pop(addr, None)
                    if det_at:
                        extended_tokens[addr] = det_at
                log.info(f"→ Extended monitoring: {addr[:8]}...")
        except Exception as e:
            log.error(f"First-minute section error: {e}\n{traceback.format_exc()}")

        # ── Section 3: Extended monitoring (checkpoints at 2min, 5min, 10min only) ──
        try:
            now = time.time()
            if now - last_extended_poll >= 15:  # Check every 15 sec for checkpoint eligibility
                last_extended_poll = now
                complete_addrs = []
                snapshot_addrs = []
                with tracking_lock:
                    ext_copy = dict(extended_tokens)

                for addr, det_at in ext_copy.items():
                    age = now - det_at
                    if age >= EXTENDED_MONITOR_SECS:
                        complete_addrs.append(addr)
                        continue
                    # Checkpoint logic: snapshot at ~2min, ~5min, ~10min
                    age_min = age / 60
                    # Take snapshot if near a checkpoint (within 15 sec window)
                    for checkpoint in [2, 5, 10]:
                        if abs(age_min - checkpoint) < 0.25:  # within 15 sec of checkpoint
                            snapshot_addrs.append(addr)
                            break

                # Batch snapshot for checkpoint tokens
                if snapshot_addrs:
                    pair_data = batch_get_pairs(snapshot_addrs)
                    for addr in snapshot_addrs:
                        pair = pair_data.get(addr)
                        if pair:
                            pd = extract_pair_data(pair)
                            if pd and pd["price"] > 0:
                                det_at = ext_copy.get(addr, now)
                                store_snapshot(addr, det_at, pd)

                # Mark complete
                for addr in complete_addrs:
                    with tracking_lock:
                        extended_tokens.pop(addr, None)
                    with db() as conn:
                        conn.execute("UPDATE tokens SET status='complete' WHERE address=?", (addr,))
                    log.info(f"✅ Complete: {addr[:8]}...")
        except Exception as e:
            log.error(f"Extended section error: {e}\n{traceback.format_exc()}")

        # ── Section 4: SOL price + FNG refresh (every 30 sec) ──
        try:
            now = time.time()
            if now - last_sol_refresh >= 30:
                last_sol_refresh = now
                refresh_sol_price()
                refresh_fng()
        except Exception as e:
            log.warning(f"SOL/FNG refresh error: {e}")

        # ── Section 5: Helius trades for extended monitoring tokens (every 60 sec) ──
        try:
            if tick % 20 == 0:  # every ~60 sec
                with tracking_lock:
                    ext_addrs = list(extended_tokens.keys())[:5]  # max 5 at a time
                for addr in ext_addrs:
                    det = extended_tokens.get(addr, time.time())
                    threading.Thread(target=fetch_helius_trades, args=(addr, det), daemon=True).start()
        except Exception as e:
            log.warning(f"Helius trades section error: {e}")

        # ── Section 6: DB cleanup (every 500 ticks ~25 min) ──
        if tick % 500 == 0:
            try:
                with db() as conn:
                    # Prune raw snapshots older than 24h (keep the data fresh, save disk)
                    cutoff = time.time() - 86400
                    deleted = conn.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,)).rowcount
                    conn.execute("DELETE FROM trades WHERE ts < ?", (cutoff,))
                    # Clean up pre_graduation cache (keep last 1000)
                    if len(pre_grad_cache) > 1000:
                        sorted_pg = sorted(pre_grad_cache.items(), key=lambda x: x[1].get("last_seen", 0))
                        for k, _ in sorted_pg[:len(pre_grad_cache) - 1000]:
                            del pre_grad_cache[k]
                    if deleted:
                        log.info(f"DB cleanup: pruned {deleted} old snapshots")
            except Exception as e:
                log.warning(f"DB cleanup error: {e}")

        # ── Sleep remainder of 3-sec tick ──
        elapsed = time.time() - loop_start
        sleep_time = max(0.1, 3.0 - elapsed)
        time.sleep(sleep_time)

# ── Startup via before_request ────────────────────────────────────────────────
@app.before_request
def ensure_started():
    global _started
    if _started:
        return
    with _start_lock:
        if _started:
            return
        init_db()
        t = threading.Thread(target=main_loop, daemon=True)
        t.start()
        _started = True
        log.info(f"Collector {VERSION} started")

# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    with db() as conn:
        total_tokens = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        total_snaps = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        collecting = conn.execute("SELECT COUNT(*) FROM tokens WHERE status='collecting'").fetchone()[0]
        complete = conn.execute("SELECT COUNT(*) FROM tokens WHERE status='complete'").fetchone()[0]
        recent = conn.execute(
            "SELECT address, symbol, name, detected_at, first_price, peak_price, status, pre_participants, pre_replies "
            "FROM tokens ORDER BY detected_at DESC LIMIT 25"
        ).fetchall()

    with tracking_lock:
        fm_count = len(first_minute_tokens)
        ext_count = len(extended_tokens)

    sol = get_sol_price()
    now = time.time()

    rows_html = ""
    for r in recent:
        det = datetime.fromtimestamp(r["detected_at"], tz=timezone.utc).strftime("%H:%M:%S") if r["detected_at"] else "?"
        fp = f"${r['first_price']:.6f}" if r["first_price"] else "—"
        pp = f"${r['peak_price']:.6f}" if r["peak_price"] else "—"
        gain = ""
        if r["first_price"] and r["peak_price"] and r["first_price"] > 0:
            g = ((r["peak_price"] - r["first_price"]) / r["first_price"]) * 100
            gain = f"{g:+.1f}%"
        status_emoji = {"collecting": "🔴", "complete": "✅", "expired": "⏰"}.get(r["status"], "?")
        rows_html += f"""<tr>
            <td>{status_emoji} {r['symbol'] or '?'}</td>
            <td><code>{r['address'][:12]}...</code></td>
            <td>{det}</td>
            <td>{fp}</td>
            <td>{pp}</td>
            <td>{gain}</td>
            <td>{r['pre_participants'] or 0}</td>
            <td>{r['pre_replies'] or 0}</td>
        </tr>"""

    html = f"""<!DOCTYPE html><html><head><title>V8 Collector</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body {{ font-family: monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }}
        h1 {{ color: #58a6ff; }}
        .stats {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }}
        .stat {{ background: #161b22; padding: 15px 25px; border-radius: 8px; border: 1px solid #30363d; }}
        .stat .val {{ font-size: 2em; color: #58a6ff; }}
        .stat .label {{ color: #8b949e; font-size: 0.9em; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #30363d; }}
        th {{ color: #58a6ff; }}
        code {{ background: #1c2128; padding: 2px 6px; border-radius: 3px; }}
    </style></head><body>
    <h1>🔬 V8 Collector — Pure Data Collection</h1>
    <div class="stats">
        <div class="stat"><div class="val">{total_tokens}</div><div class="label">Total Tokens</div></div>
        <div class="stat"><div class="val">{total_snaps}</div><div class="label">Snapshots</div></div>
        <div class="stat"><div class="val">{fm_count}</div><div class="label">First Minute</div></div>
        <div class="stat"><div class="val">{ext_count}</div><div class="label">Extended</div></div>
        <div class="stat"><div class="val">{collecting}</div><div class="label">Collecting</div></div>
        <div class="stat"><div class="val">{complete}</div><div class="label">Complete</div></div>
        <div class="stat"><div class="val">${sol:.2f}</div><div class="label">SOL/USD</div></div>
    </div>
    <h2>Recent Graduations</h2>
    <table>
        <tr><th>Symbol</th><th>Address</th><th>Time</th><th>First $</th><th>Peak $</th><th>Gain</th><th>Participants</th><th>Replies</th></tr>
        {rows_html}
    </table>
    <p style="color:#8b949e; margin-top:30px;">Version: {VERSION} | Uptime: — | Auto-refresh: 10s</p>
    <p style="color:#8b949e;">API: <a href="/api/stats" style="color:#58a6ff">/api/stats</a> |
        <a href="/api/tokens" style="color:#58a6ff">/api/tokens</a> |
        <a href="/api/waves" style="color:#58a6ff">/api/waves</a> |
        <a href="/health" style="color:#58a6ff">/health</a> |
        <a href="/logs" style="color:#58a6ff">/logs</a></p>
    </body></html>"""
    return html

@app.route("/health")
def health():
    with db() as conn:
        tok = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        snaps = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    with tracking_lock:
        fm = len(first_minute_tokens)
        ext = len(extended_tokens)
    return jsonify({
        "status": "ok",
        "version": VERSION,
        "tokens": tok,
        "snapshots": snaps,
        "monitoring": {"first_minute": fm, "extended": ext},
        "sol_price": get_sol_price(),
        "ts": time.time(),
    })

@app.route("/logs")
def logs():
    n = request.args.get("n", 200, type=int)
    with log_buffer_lock:
        lines = log_buffer[-n:]
    return "<pre style='background:#0d1117;color:#c9d1d9;padding:20px;font-family:monospace;'>" + "\n".join(lines) + "</pre>"

@app.route("/api/stats")
def api_stats():
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        complete = conn.execute("SELECT COUNT(*) FROM tokens WHERE status='complete'").fetchone()[0]
        collecting = conn.execute("SELECT COUNT(*) FROM tokens WHERE status='collecting'").fetchone()[0]
        snaps = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        pre_count = conn.execute("SELECT COUNT(*) FROM pre_graduation").fetchone()[0]
        grad_count = conn.execute("SELECT COUNT(*) FROM pre_graduation WHERE graduated=1").fetchone()[0]

        # Average peak gain for complete tokens
        avg_gain_row = conn.execute("""
            SELECT AVG(CASE WHEN first_price > 0 THEN ((peak_price - first_price) / first_price) * 100 ELSE 0 END) as avg_gain
            FROM tokens WHERE status='complete' AND first_price > 0 AND peak_price > 0
        """).fetchone()
        avg_gain = avg_gain_row["avg_gain"] if avg_gain_row and avg_gain_row["avg_gain"] else 0

    return jsonify({
        "version": VERSION,
        "total_tokens": total,
        "collecting": collecting,
        "complete": complete,
        "total_snapshots": snaps,
        "pre_graduation_tracked": pre_count,
        "graduated_count": grad_count,
        "avg_peak_gain_pct": round(avg_gain, 2),
        "sol_price": get_sol_price(),
    })

@app.route("/api/tokens")
def api_tokens():
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)
    status_filter = request.args.get("status", "")

    with db() as conn:
        if status_filter:
            rows = conn.execute(
                "SELECT * FROM tokens WHERE status=? ORDER BY detected_at DESC LIMIT ? OFFSET ?",
                (status_filter, limit, offset)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM tokens ORDER BY detected_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()

        result = []
        for r in rows:
            d = dict(r)
            # Add pre-graduation data from pre_graduation table
            pg = conn.execute("SELECT * FROM pre_graduation WHERE address=?", (d["address"],)).fetchone()
            if pg:
                d["pre_graduation"] = dict(pg)
            # Add snapshot count
            sc = conn.execute("SELECT COUNT(*) FROM snapshots WHERE address=?", (d["address"],)).fetchone()[0]
            d["snapshot_count"] = sc
            # Compute gain
            if d.get("first_price") and d.get("peak_price") and d["first_price"] > 0:
                d["peak_gain_pct"] = round(((d["peak_price"] - d["first_price"]) / d["first_price"]) * 100, 2)
            else:
                d["peak_gain_pct"] = None
            result.append(d)

    return jsonify(result)

@app.route("/api/snapshots/<address>")
def api_snapshots(address):
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM snapshots WHERE address=? ORDER BY ts ASC", (address,)
        ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/waves")
def api_waves():
    """
    Key analysis endpoint. For each token with 60+ seconds of data,
    show price curve at 5-second intervals, peak gain, wave classification.
    """
    limit = request.args.get("limit", 100, type=int)
    wave_filter = request.args.get("type", "")

    with db() as conn:
        tokens = conn.execute("""
            SELECT * FROM tokens
            WHERE first_price IS NOT NULL AND first_price > 0
            ORDER BY detected_at DESC LIMIT ?
        """, (limit,)).fetchall()

        results = []
        for tok in tokens:
            addr = tok["address"]
            det_at = tok["detected_at"]
            if not det_at:
                continue

            snaps = conn.execute(
                "SELECT age_seconds, price, mcap FROM snapshots WHERE address=? ORDER BY age_seconds ASC",
                (addr,)
            ).fetchall()

            if not snaps:
                continue

            # Build price grid at 0,5,10,...,60 seconds using nearest snapshot
            prices = {}
            for target_sec in range(0, 61, 5):
                best_snap = None
                best_dist = float("inf")
                for s in snaps:
                    dist = abs(s["age_seconds"] - target_sec)
                    if dist < best_dist:
                        best_dist = dist
                        best_snap = s
                # Only include if within 4 seconds of target
                if best_snap and best_dist <= 4:
                    prices[str(target_sec)] = best_snap["price"]

            # Need at least some data points
            if len(prices) < 3:
                continue

            first_price = tok["first_price"]
            peak_price = tok["peak_price"] or first_price
            peak_time = tok["peak_time"] or det_at
            time_to_peak = peak_time - det_at if peak_time else 0

            # Get latest price
            last_snap = snaps[-1] if snaps else None
            current_price = last_snap["price"] if last_snap else first_price

            peak_gain_pct = ((peak_price - first_price) / first_price) * 100 if first_price > 0 else 0
            final_gain_pct = ((current_price - first_price) / first_price) * 100 if first_price > 0 else 0

            # Wave classification
            wave_type = classify_wave(peak_gain_pct, final_gain_pct, time_to_peak, peak_price, current_price)

            if wave_filter and wave_type != wave_filter:
                continue

            # Get trade summary from Helius data
            trade_rows = conn.execute(
                "SELECT direction, sol_amount FROM trades WHERE address=? ORDER BY ts ASC",
                (addr,)
            ).fetchall()
            total_buys_sol = sum(t["sol_amount"] for t in trade_rows if t["direction"] == "buy")
            total_sells_sol = sum(t["sol_amount"] for t in trade_rows if t["direction"] == "sell")
            buy_count = sum(1 for t in trade_rows if t["direction"] == "buy")
            sell_count = sum(1 for t in trade_rows if t["direction"] == "sell")
            whale_buys = sum(1 for t in trade_rows if t["direction"] == "buy" and t["sol_amount"] >= 3)

            results.append({
                "address": addr,
                "symbol": tok["symbol"],
                "name": tok["name"],
                "detected_at": det_at,
                "first_price": first_price,
                "peak_price": peak_price,
                "peak_gain_pct": round(peak_gain_pct, 2),
                "time_to_peak_seconds": round(time_to_peak, 1),
                "current_price": current_price,
                "final_gain_pct": round(final_gain_pct, 2),
                "pre_participants": tok["pre_participants"],
                "pre_replies": tok["pre_replies"],
                "pre_velocity": tok["pre_velocity"],
                "pre_ath_mcap": tok["pre_ath_mcap"],
                "pre_creator": tok["pre_creator"],
                "mint_revoked": tok["mint_revoked"],
                "curve_duration_sec": tok["curve_duration_sec"],
                "graduation_hour": tok["graduation_hour"],
                "graduation_dow": tok["graduation_dow"],
                "sol_price_at_grad": tok["sol_price_at_grad"],
                "fng_at_grad": tok["fng_at_grad"],
                "has_website": tok["has_website"],
                "has_twitter": tok["has_twitter"],
                "has_telegram": tok["has_telegram"],
                "status": tok["status"],
                "snapshot_count": len(snaps),
                "prices": prices,
                "wave_type": wave_type,
                "trades": {
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "total_buys_sol": round(total_buys_sol, 2),
                    "total_sells_sol": round(total_sells_sol, 2),
                    "whale_buys_3sol": whale_buys,
                },
            })

    return jsonify(results)

@app.route("/api/trades/<address>")
def api_trades(address):
    """Individual trades for a token from Helius."""
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM trades WHERE address=? ORDER BY ts ASC", (address,)
        ).fetchall()
    return jsonify([dict(r) for r in rows])

def classify_wave(peak_gain_pct, final_gain_pct, time_to_peak, peak_price, current_price):
    """Classify the wave pattern of a token's first minute."""
    # rocket: peak_gain > 100% within 30 sec
    if peak_gain_pct > 100 and time_to_peak <= 30:
        return "rocket"
    # pump_dump: peak_gain > 50% but final < peak * 0.5
    if peak_gain_pct > 50 and peak_price > 0 and current_price < peak_price * 0.5:
        return "pump_dump"
    # slow_climb: peak_gain > 20% and time_to_peak > 30 sec
    if peak_gain_pct > 20 and time_to_peak > 30:
        return "slow_climb"
    # death: final_gain < -20%
    if final_gain_pct < -20:
        return "death"
    # flat: peak_gain < 20% and final within 10% of first
    if peak_gain_pct < 20 and abs(final_gain_pct) <= 10:
        return "flat"
    # Default — doesn't fit neatly
    if peak_gain_pct > 50:
        return "pump_dump"
    return "flat"

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
