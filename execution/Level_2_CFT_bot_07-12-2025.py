import ccxt
import asyncio
import threading
import time
from datetime import datetime, timedelta, timezone
import logging
import os
from logging.handlers import RotatingFileHandler
from mysql.connector import connect, Error
from execution.daily_guard_2 import daily_guard_eval_and_act, ensure_day_snapshot
from TG_messenger_2 import init_tg, send_open_signal, send_close_signal
import requests
from datetime import time as dtime  # top of file if not present



# Get the directory where the script is running
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define log file path (use absolute path to avoid issues)
log_file = os.path.join(script_dir, "trade_bot.log")

# Setup global logging with log rotation
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3),
        logging.StreamHandler()
    ]
)

logging.info("Trade bot started successfully.")

# Constants
Z_UPPER_THRESHOLD = 5
Z_LOWER_THRESHOLD = 2
SPL = 300  # Sleep time in seconds
SPL1 = 30
MAX_TRADING_THREADS = 4  # Maximum concurrent trading pairs
z_exit = 6
balance_req = 0.20
exp_per_asset = 0.30
bal_to_use = 0.1
exp_per_asset1 = 0.2
bal_to_use1 = 0.2
TIMEFRAME_MIN_PER_BAR = 5
max_lev = 5
DAILY_TP = 1
DAILY_SL = 3
chunk = 100.0
pause_s = 3.0
MIN_PER_LEG_USDT = 800.0

# DB deadlock retry settings
DB_MAX_RETRIES = 5
DB_BASE_WAIT = 0.5  # seconds
DEADLOCK_ERROR_CODES = (1205, 1213)  # 1205 = lock wait timeout, 1213 = deadlock



# --- Configurable DB→UTC offset if your MySQL timestamps are not UTC ---
TZ_OFFSET_HOURS = 0  # set to 0 if your DB stores UTC; otherwise e.g. -1, +2, etc.
# --- Blackout window for opening new trades (local time = TZ_OFFSET_HOURS) ---
BLACKOUT_START_WD = 4   # Friday (Mon=0)
BLACKOUT_START_H  = 14  # 14:00
BLACKOUT_END_WD   = 6   # Sunday
BLACKOUT_END_H    = 14  # 14:00


_last_bal = {'ts': 0, 'data': None}

# Bot constants – change these two to switch between bot_1 and bot_2
BOT_FIELD = "bot_6"          # Field in Z-Scores table that indicates the bot flag.
BOT_FIELD_TP = "bot_1_tp"      # Field for tracking take profit count.
BOT_FIELD_SL = "bot_1_sl"      # Field for tracking stop-loss count.
RESULT_TABLE = "Bot_4_res"     # Table for trade results.
LEVEL = "Level_2"  # Only pairs with this level will be selected for trading
BOT_NUM = "Bot_CFT_2"  # Sign in res table


# MySQL Connection Setup
def connect_to_db():
    try:
        connection = connect(
            host="xxxx",
            user="xxxx",              # Your MySQL username here
            password="xxxx",    # Your MySQL password here
            database="xxxx"           # Use the correct database name
        )
        print("MySQL connection successful!")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None


# read_api_credentials remains unchanged.
def read_api_credentials():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "api_credentials_CFT_1.txt")
    credentials = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    credentials[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"Error: API credentials file not found at {file_path}")
    except Exception as e:
        print(f"Error reading API credentials: {e}")
    return credentials


# Read API credentials.
api_credentials = read_api_credentials()

# Initialize the Bybit exchange object.
# === BYBIT DEMO INITIALIZATION ===
# Works for Demo Trading (https://api-demo.bybit.com), not testnet.
bybit = ccxt.bybit({
    'apiKey': api_credentials.get('api_key'),
    'secret': api_credentials.get('api_secret'),
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
    'timeout': 20000,
})

# Enable Demo Trading (needs CCXT ≥ v4.2.100)
if hasattr(bybit, "enable_demo_trading"):
    bybit.enable_demo_trading(True)
else:
    # fallback for older CCXT versions
    bybit.set_sandbox_mode(True)

bybit.load_markets()
print(f"Bybit DEMO initialized ({len(bybit.markets)} markets loaded) with API key: {api_credentials.get('api_key')[:4]}****")

NETWORK_RETRYABLE = (
    ccxt.NetworkError, ccxt.RequestTimeout,
    requests.exceptions.ConnectionError, requests.exceptions.Timeout,
)

async def with_backoff(coro_fn, *args, retries=5, base_delay=1.5, max_delay=15, **kwargs):
    delay = base_delay
    for attempt in range(1, retries + 1):
        try:
            return await asyncio.to_thread(coro_fn, *args, **kwargs)
        except NETWORK_RETRYABLE as e:
            print(f"{datetime.now()} - NETWARN attempt {attempt}/{retries}: {e}")
            await asyncio.sleep(delay)
            delay = min(max_delay, delay * 1.7)
        except Exception as e:
            raise
    msg = f"{coro_fn.__name__} failed after {retries} retries: {args[:2]}"
    print(f"[CRITICAL] {msg}")
    await tg_alert(msg)        # 🔔 alert to Telegram
    raise RuntimeError(f"network_backoff_exhausted: {msg}")

# After bybit init & load_markets:
ensure_day_snapshot(bybit, connect_to_db, BOT_NUM)

# --- Telegram init & health ping ---
init_tg(
    token="8401495687:AAEjWWFsFpZ8ZAxULWs1lInKhoPSGGBqGIE",
    chat_id="8495254471"
)

ok = send_open_signal(
    uuid="BOOT",
    long_symbol="PING", long_amount=1.0,
    short_symbol="PONG", short_amount=1.0,
    z_score=0.0, level="startup"
)
logging.info(f"[TG] Startup ping sent: {ok}")

def _minutes_from_week_start(dt):
    # dt is timezone-adjusted "local" time per TZ_OFFSET_HOURS
    return dt.weekday() * 24 * 60 + dt.hour * 60 + dt.minute

def is_in_open_blackout(now_utc=None):
    """
    Returns True when we should block NEW OPENS.
    Uses local time = UTC + TZ_OFFSET_HOURS.
    """
    if now_utc is None:
        now_utc = datetime.utcnow()
    # convert to "local" by TZ_OFFSET_HOURS (no external tz libs)
    local = now_utc + timedelta(hours=TZ_OFFSET_HOURS)

    t = _minutes_from_week_start(local)
    start = BLACKOUT_START_WD * 24 * 60 + BLACKOUT_START_H * 60
    end   = BLACKOUT_END_WD   * 24 * 60 + BLACKOUT_END_H   * 60
    return start <= t < end


# --- Telegram emergency alerts ---
async def tg_alert(message: str):
    """
    Sends emergency/system messages to Telegram.
    Uses send_open_signal() as a transport mechanism.
    Every alert is tagged with the bot number.
    """
    try:
        full_text = (
            f"⚠️ BOT ALERT ⚠️\n"
            f"Bot: {BOT_NUM}\n"
            f"Message: {message}\n"
            f"Time: {datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}")
        await asyncio.to_thread(
            send_open_signal,
            uuid="SYSTEM",
            long_symbol="ALERT",
            long_amount=0.0,
            short_symbol="NONE",
            short_amount=0.0,
            z_score=0.0,
            level=full_text)
        print(f"[TG] ALERT sent: {message}")
    except Exception as e:
        print(f"[TG] FAILED to send alert: {e}")


async def get_cached_balance(bybit, ttl=30):
    now = time.time()
    if _last_bal['data'] is not None and (now - _last_bal['ts'] < ttl):
        return _last_bal['data']
    data = await with_backoff(bybit.fetch_balance, retries=4, base_delay=2.0)
    _last_bal.update({'ts': now, 'data': data})
    return data


# Logger function remains unchanged.
def get_pair_logger(asset1_symbol, asset2_symbol):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "trade_logs")
    os.makedirs(log_dir, exist_ok=True)
    asset1_name = asset1_symbol.replace('/', '_').replace(':', '_')
    asset2_name = asset2_symbol.replace('/', '_').replace(':', '_')
    log_filename = os.path.join(log_dir, f"trade_log_{asset1_name}_{asset2_name}.log")
    logger = logging.getLogger(f"{asset1_symbol}_{asset2_symbol}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        file_handler = RotatingFileHandler(log_filename, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    return logger


async def get_first_asset_pair():
    """
    Select one pair ONLY from this bot's LEVEL.
    Do NOT exclude pairs taken by other bots/fields (even same level).
    Only ensure we don't collide with ourselves (this BOT_FIELD).
    Now also returns Asset1_5m_vol and Asset2_5m_vol (USDT).
    Deadlock-safe with retries.
    """
    subquery = f"""
                SELECT asset FROM (
                    SELECT asset_1 AS asset FROM `Z-Scores` WHERE {BOT_FIELD} = 1
                    UNION
                    SELECT asset_2 AS asset FROM `Z-Scores` WHERE {BOT_FIELD} = 1
                ) AS trading_assets
            """

    select_sql = f"""
        SELECT
            uuid, asset_1, asset_2,
            last_z_score, TP, SL, Level, ADF, `p-value`, HL,
            `hl_spread_med`, `last_spread`,
            `Asset1_5m_vol`, `Asset2_5m_vol`, `beta_norm`
        FROM `Z-Scores`
        WHERE {BOT_FIELD} = 0           -- free for THIS bot field
          AND Level = %s                -- prevent cross-level leakage
          AND asset_1 NOT IN ({subquery})
          AND asset_2 NOT IN ({subquery})
          AND ADF <= -2.9
          AND `p-value` <= 0.05
          AND Hurst <= 0.45
          AND HL IS NOT NULL AND HL > 0
          AND `hl_spread_med` IS NOT NULL
          AND `last_spread` IS NOT NULL
          AND ABS(`last_spread`) < ABS(`hl_spread_med`) * 1.2
          AND ABS(last_z_score) >= 0.9 * CASE
                WHEN HL < 60  THEN 2
                WHEN HL < 100 THEN 2.5
                WHEN HL < 140 THEN 3
                ELSE 3.5
              END
          AND ABS(last_z_score) < {Z_UPPER_THRESHOLD}
        ORDER BY `Num trades` ASC, ABS(last_z_score) DESC
        LIMIT 1
        FOR UPDATE
    """

    for attempt in range(1, DB_MAX_RETRIES + 1):
        connection = None
        try:
            connection = connect_to_db()
            if connection is None:
                print("[ERROR] get_first_asset_pair(): connect_to_db() returned None")
                return (None,) * 15

            connection.autocommit = False
            cur = connection.cursor()

            # 1) find + lock candidate row IN THIS LEVEL
            cur.execute(select_sql, (LEVEL,))
            row = cur.fetchone()
            if not row:
                connection.rollback()
                return (None,) * 15

            (uuid_val, asset1, asset2,
             last_z, tp, sl, level, adf, pval, hl,
             hl_spread_med, last_spread,
             asset1_5m_vol, asset2_5m_vol, beta_norm) = row

            # 2) atomically claim ONLY with our own BOT_FIELD (allow others)
            upd_sql = f"""
                UPDATE `Z-Scores`
                SET {BOT_FIELD}=1
                WHERE uuid=%s AND {BOT_FIELD}=0 AND Level=%s
            """
            cur.execute(upd_sql, (uuid_val, LEVEL))
            if cur.rowcount != 1:
                connection.rollback()
                return (None,) * 15

            connection.commit()

            return (uuid_val, asset1, asset2,
                    last_z, tp, sl, level, adf, pval, hl,
                    hl_spread_med, last_spread,
                    asset1_5m_vol, asset2_5m_vol, beta_norm)

        except Error as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            # Deadlock / lock wait → retry
            if getattr(e, "errno", None) in DEADLOCK_ERROR_CODES:
                print(f"[DEADLOCK] get_first_asset_pair() attempt {attempt}/{DB_MAX_RETRIES}: {e}")
                if attempt < DB_MAX_RETRIES:
                    time.sleep(DB_BASE_WAIT * attempt)
                    continue
            # Other DB errors → no retry
            print(f"[ERROR] get_first_asset_pair(): {e}")
            return (None,) * 15

        finally:
            if connection and connection.is_connected():
                connection.close()

    # If loop exhausted
    print("[CRITICAL] get_first_asset_pair(): exceeded max deadlock retries")
    return (None,) * 15


# New function to refresh the Z-score for a given pair using its UUID.
def refresh_z_score(uuid_val):
    for attempt in range(1, DB_MAX_RETRIES + 1):
        connection = connect_to_db()
        if connection is None:
            print("Error: Could not establish MySQL connection in refresh_z_score.")
            return None, None
        try:
            cursor = connection.cursor()
            query = "SELECT last_z_score, Coint FROM `Z-Scores` WHERE uuid = %s"
            cursor.execute(query, (uuid_val,))
            row = cursor.fetchone()
            if row:
                return row[0], row[1]
            else:
                print(f"No record found for uuid {uuid_val}.")
                return None, None
        except Error as e:
            if getattr(e, "errno", None) in DEADLOCK_ERROR_CODES:
                print(f"[DEADLOCK] refresh_z_score attempt {attempt}/{DB_MAX_RETRIES}: {e}")
                if attempt < DB_MAX_RETRIES:
                    time.sleep(DB_BASE_WAIT * attempt)
                    continue
            print(f"Error refreshing Z-score: {e}")
            return None, None
        finally:
            if connection.is_connected():
                connection.close()
    print("[CRITICAL] refresh_z_score(): exceeded max deadlock retries")
    return None, None


async def refresh_z_score_for_pair(uuid_val):
    new_z = await asyncio.to_thread(refresh_z_score, uuid_val)
    return new_z


# Function to fetch closed PnL from the first entry.
async def get_first_closed_pnl(symbol):
    try:
        position_history = await asyncio.to_thread(bybit.fetch_position_history, symbol=symbol, since=None, limit=None, params={})
        if isinstance(position_history, list) and len(position_history) > 0:
            pnl = position_history[0].get('info', {}).get('closedPnl')
            print(f"First closedPnl for asset {symbol} = {pnl}")
            return pnl
        else:
            print(f"No position history data returned for {symbol}.")
            return None
    except Exception as e:
        print(f"Error fetching position history for {symbol}: {e}")
        return None


async def fetch_position_value(symbol, retries=10, delay=5):
    for attempt in range(retries):
        try:
            positions = await asyncio.to_thread(bybit.fetch_positions, [symbol], {})
            for position in positions:
                if position['symbol'] == symbol and position['info']['positionValue']:
                    return float(position['info']['positionValue'])
        except ccxt.NetworkError as e:
            print(f"NetworkError fetching position value for {symbol}: {e}. Retrying in {delay} sec...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
    return 0.0


async def fetch_position_amount(symbol, retries=10, delay=5):
    """
    Returns the *absolute* current position size (contracts/amount) for a symbol.
    If no open position is found, returns 0.0.
    """
    for attempt in range(retries):
        try:
            positions = await asyncio.to_thread(bybit.fetch_positions, [symbol], {})
            for position in positions:
                if position.get('symbol') == symbol:
                    # Try several common fields for size
                    size = (
                        position.get('contracts')
                        or position.get('contractSize')
                        or position.get('amount')
                        or position.get('info', {}).get('size')
                        or 0.0
                    )
                    size = float(size or 0.0)
                    if abs(size) > 0:
                        return abs(size)
            # nothing found / zero size
            return 0.0
        except ccxt.NetworkError as e:
            print(f"NetworkError fetching position amount for {symbol}: {e}. Retrying in {delay} sec...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
        except Exception as e:
            print(f"Error fetching position amount for {symbol}: {e}")
            return 0.0
    return 0.0


async def place_order_and_check(symbol, side, amount, leverage, max_retries=5, initial_delay=3, delay_multiplier=2):
    """
    Places a market order and retries if the order fails, with increasing delay.
    """
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            print(f"{datetime.now()} - Placing MARKET order (attempt {attempt}): {side} {amount} {symbol} with leverage {leverage}x")
            order = await asyncio.to_thread(
                bybit.create_order,
                symbol=symbol,
                type='market',
                side=side,
                amount=amount,
                params={'buyLeverage': leverage, 'sellLeverage': leverage}
            )
            if order and 'id' in order:
                print(f"{datetime.now()} - Market order placed: {order['id']} for {symbol} ({side} {amount})")
                return order
            else:
                print(f"{datetime.now()} - ERROR: Market order placement failed for {symbol}. No order ID returned.")
        except Exception as e:
            print(f"{datetime.now()} - ERROR placing market order for {symbol} (attempt {attempt}): {e}")

        if attempt < max_retries:
            print(f"{datetime.now()} - Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= delay_multiplier  # Exponential backoff
        else:
            print(f"{datetime.now()} - Max retries reached for placing order on {symbol}. Giving up.")
            return None


def get_today_closed_pnl_percent(filter_by_bot=True):
    """
    Returns today's net closed PnL percent from the results table (sum of `pnl_pers`)
    for rows with a non-null close_date equal to today. If filter_by_bot is True,
    restricts to current BOT_NUM.
    """
    connection = connect_to_db()
    if connection is None:
        print("Error: Could not establish MySQL connection in get_today_closed_pnl_percent.")
        return 0.0
    try:
        cursor = connection.cursor()
        if filter_by_bot:
            query = f"""
                SELECT COALESCE(SUM(pnl_pers), 0)
                FROM {RESULT_TABLE}
                WHERE close_date = CURDATE()
                  AND close_date IS NOT NULL
                  AND Bot_num = %s
            """
            cursor.execute(query, (BOT_NUM,))
        else:
            query = f"""
                SELECT COALESCE(SUM(pnl_pers), 0)
                FROM {RESULT_TABLE}
                WHERE close_date = CURDATE()
                  AND close_date IS NOT NULL
            """
            cursor.execute(query)
        row = cursor.fetchone()
        return float(row[0] or 0.0)
    except Error as e:
        print(f"Error fetching today's closed PnL percent: {e}")
        return 0.0
    finally:
        if connection.is_connected():
            connection.close()


async def calculate_dynamic_leverage(bybit, asset1_symbol, asset2_symbol,
                                     asset1_5m_vol, asset2_5m_vol,
                                     beta_norm,
                                     max_leverage = max_lev,
                                     throttle_mode: bool = False):
    """
    Beta-hedged sizing with a hard minimum per leg.
      - Controller leg = the one with LOWER raw 5m volume.
      - Each leg has an effective cap: max(MIN_PER_LEG_USDT, 5m_vol).
      - Controller exposure = min(base_exposure, controller_cap).
      - Other leg exposure = controller_exposure * beta_norm (clamped), then capped.
      - If other exposure exceeds its cap, both are scaled down proportionally.
      - Finally, both legs are forced to be at least MIN_PER_LEG_USDT.

    Returns dict:
      {
        "leverage", "exposure_asset1", "exposure_asset2",
        "amount_asset1", "amount_asset2",
        "price_asset1", "price_asset2",
        "total_exposure", "beta_norm_used", "controller" (1 or 2)
      }
    """
    # --- local hard minimum per leg (USDT) ---
    MIN_PER_LEG_USDT = 500.0

    try:
        # --- Daily PnL check (for regime) ---
        daily_pnl_percent = await asyncio.to_thread(get_today_closed_pnl_percent, True)
        print(f"{datetime.now()} - Daily closed PnL% (net) = {daily_pnl_percent:.2f}%")

        # --- Prices ---
        t1 = await asyncio.to_thread(bybit.fetch_ticker, asset1_symbol)
        t2 = await asyncio.to_thread(bybit.fetch_ticker, asset2_symbol)
        p1 = float(t1.get('last') or 0.0)
        p2 = float(t2.get('last') or 0.0)
        if p1 <= 0.0 or p2 <= 0.0:
            raise ValueError("❌ ERROR: Failed to fetch valid asset prices.")

        # --- Balance ---
        balance = await asyncio.to_thread(bybit.fetch_balance)
        total_balance = float(balance.get('total', {}).get('USDT', 0.0) or 0.0)
        free_balance  = balance.get('free', {}).get('USDT')
        if free_balance is None:
            free_balance = float(balance['info']['result']['list'][0]['totalAvailableBalance'])
        free_balance = float(free_balance)
        if total_balance <= 0:
            raise ValueError("❌ ERROR: Total balance is zero or missing.")

        # --- Base exposure regime (your original gating) ---
        if daily_pnl_percent <= -3.0:
            exposure_per_asset_base = exp_per_asset1 * total_balance
            balance_to_use = bal_to_use1 * free_balance
        else:
            if free_balance > 0.2 * total_balance:
                exposure_per_asset_base = exp_per_asset * total_balance
                balance_to_use = bal_to_use * free_balance
            else:
                exposure_per_asset_base = exp_per_asset1 * total_balance
                balance_to_use = bal_to_use1 * free_balance

        if balance_to_use <= 0:
            raise ValueError("❌ ERROR: Calculated balance to use is zero.")

        # --- Throttle (halve exposure after TP stage) ---
        if throttle_mode:
            exposure_per_asset_base *= 0.5

        # --- Raw 5m volumes ---
        v1_raw = float(asset1_5m_vol) if asset1_5m_vol is not None else 0.0
        v2_raw = float(asset2_5m_vol) if asset2_5m_vol is not None else 0.0

        # --- Effective caps (at least MIN_PER_LEG_USDT) ---
        v1_cap = max(MIN_PER_LEG_USDT, v1_raw)
        v2_cap = max(MIN_PER_LEG_USDT, v2_raw)

        # --- Beta normalization (clamp 0.8..1.2) ---
        try:
            beta_adj = float(beta_norm) if (beta_norm is not None and float(beta_norm) > 0) else 1.0
        except Exception:
            beta_adj = 1.0
        beta_adj = max(0.8, min(1.2, beta_adj))

        # --- Controller = lower raw volume ---
        ctrl_is_1 = (v1_raw <= v2_raw)

        # Controller exposure capped by base & its cap
        E_ctrl = min(exposure_per_asset_base, v1_cap if ctrl_is_1 else v2_cap)

        # Proposed other exposure via beta
        E_other_prop = E_ctrl * beta_adj
        other_cap_eff = v2_cap if ctrl_is_1 else v1_cap

        # If other exceeds its cap, scale both
        if E_other_prop > other_cap_eff:
            scale = (other_cap_eff / E_other_prop) if E_other_prop > 0 else 0.0
            E_ctrl       *= scale
            E_other_prop  = other_cap_eff

        # Map to legs
        E1 = E_ctrl if ctrl_is_1 else E_other_prop
        E2 = E_other_prop if ctrl_is_1 else E_ctrl

        # Enforce hard minimum again (safety)
        E1 = max(E1, MIN_PER_LEG_USDT)
        E2 = max(E2, MIN_PER_LEG_USDT)

        if E1 <= 0 or E2 <= 0:
            raise ValueError("❌ ERROR: Computed exposures are non-positive.")

        # Leverage from usable balance
        total_exposure_usdt = E1 + E2
        leverage = min(total_exposure_usdt / balance_to_use, max_leverage)

        # Amounts
        amt1 = E1 / p1
        amt2 = E2 / p2
        if amt1 <= 0 or amt2 <= 0:
            raise ValueError("❌ ERROR: Calculated trade amount is zero or negative.")

        print(f"✅ {datetime.now()} - Leverage: {leverage}x")
        print(f"✅ {datetime.now()} - Exposure1: {E1:.2f} USDT, Exposure2: {E2:.2f} USDT (beta_norm={beta_adj})")
        print(f"✅ {datetime.now()} - {asset1_symbol} Amount: {amt1}, {asset2_symbol} Amount: {amt2}")

        return {
            "leverage": leverage,
            "exposure_asset1": E1,
            "exposure_asset2": E2,
            "amount_asset1": amt1,
            "amount_asset2": amt2,
            "price_asset1": p1,
            "price_asset2": p2,
            "total_exposure": total_exposure_usdt,
            "beta_norm_used": beta_adj,
            "controller": 1 if ctrl_is_1 else 2,
        }

    except Exception as e:
        logging.error(f"❌ ERROR in leverage calculation: {e}")
        print(f"{datetime.now()} - ❌ ERROR: {e}")
        return None




async def set_leverage_with_retry(bybit, symbol, leverage, retries=3, delay=2):
    for attempt in range(retries):
        try:
            curr_lev = None
            leverage_info = await asyncio.to_thread(bybit.fetch_leverage, symbol=symbol, params={})
            if isinstance(leverage_info, dict) and 'leverage' in leverage_info:
                curr_lev = float(leverage_info['leverage'])
            elif isinstance(leverage_info, list) and 'leverage' in leverage_info[0]:
                curr_lev = float(leverage_info[0]['leverage'])
            print(f"Current leverage for {symbol} is {curr_lev}, requested: {leverage}")
            if curr_lev is not None and abs(curr_lev - leverage) < 1e-5:
                print(f"Leverage for {symbol} already correct ({curr_lev}).")
                return True
            await asyncio.to_thread(bybit.set_leverage, symbol=symbol, leverage=leverage, params={})
            print(f"Leverage set to {leverage} for {symbol} - {datetime.now()}")
            # Verify again
            leverage_info = await asyncio.to_thread(bybit.fetch_leverage, symbol=symbol, params={})
            if isinstance(leverage_info, dict) and 'leverage' in leverage_info:
                new_lev = float(leverage_info['leverage'])
            elif isinstance(leverage_info, list) and 'leverage' in leverage_info[0]:
                new_lev = float(leverage_info[0]['leverage'])
            else:
                print(f"Attempt {attempt+1}/{retries}: Could not verify leverage set: {leverage_info}")
                new_lev = None
            if new_lev is not None and abs(new_lev - leverage) < 1e-5:
                print(f"Leverage for {symbol} successfully set to {new_lev}.")
                return True
            else:
                print(f"Attempt {attempt+1}/{retries}: Leverage verify failed, got {new_lev}, expected {leverage}.")
        except Exception as e:
            print(f"Attempt {attempt + 1}/{retries} failed while setting leverage for {symbol}: {e}")
        if attempt < retries - 1:
            print(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
        else:
            print(f"Max retries reached. Failed to set leverage for {symbol}.")
            return False


async def trade_pair(asset1_symbol, asset2_symbol):
    asset1_name = asset1_symbol.split('/')[0].replace('/', '_').replace(':', '_')
    asset2_name = asset2_symbol.split('/')[0].replace('/', '_').replace(':', '_')
    time_now = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"trade_log_{asset1_name}_{asset2_name}_{time_now}.log"
    logger = logging.getLogger(f"{asset1_symbol}_{asset2_symbol}")
    handler = logging.FileHandler(log_filename)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.info(f"Started trading for {asset1_symbol}/{asset2_symbol}")
    await execute_trade_logic()


async def fetch_unrealized_pnl(bybit, asset1_symbol, asset2_symbol, max_retries=5, delay=3):
    """
    Fetches unrealized PnL for two symbols, with retries on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            position1 = await asyncio.to_thread(bybit.fetch_position, symbol=asset1_symbol, params={})
            position2 = await asyncio.to_thread(bybit.fetch_position, symbol=asset2_symbol, params={})
            pnl1 = float(position1['info']['unrealisedPnl']) if 'unrealisedPnl' in position1['info'] else 0.0
            pnl2 = float(position2['info']['unrealisedPnl']) if 'unrealisedPnl' in position2['info'] else 0.0
            total_pnl = pnl1 + pnl2
            print(f"🔎 {datetime.now()} - {asset1_symbol} Unrealized PnL: {pnl1} USDT")
            print(f"🔎 {datetime.now()} - {asset2_symbol} Unrealized PnL: {pnl2} USDT")
            print(f"✅ {datetime.now()} - Total Unrealized PnL: {total_pnl} USDT")
            return total_pnl
        except Exception as e:
            print(f"❌ ERROR: Attempt {attempt}/{max_retries} - Failed to fetch unrealized PnL: {e}")
            if attempt < max_retries:
                print(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
            else:
                print("❌ ERROR: Max retries reached. Returning None.")
                return None


def reset_bot_flag_in_db(uuid_val, bot_field=BOT_FIELD, max_retries=5, delay=3):
    for attempt in range(max_retries):
        connection = connect_to_db()
        if connection is None:
            print("Error: Could not establish MySQL connection in reset_bot_flag_in_db.")
            return False
        try:
            cursor = connection.cursor()
            query = f"UPDATE `Z-Scores` SET {bot_field} = 0 WHERE uuid = %s"
            cursor.execute(query, (uuid_val,))
            connection.commit()
            cursor.execute(f"SELECT {bot_field} FROM `Z-Scores` WHERE uuid = %s", (uuid_val,))
            row = cursor.fetchone()
            if row is not None and row[0] == 0:
                print(f"{bot_field} flag reset to 0 for uuid {uuid_val}.")
                return True
            else:
                print(f"Attempt {attempt+1}: {bot_field} flag not reset, retrying...")
        except Error as e:
            print(f"Error resetting {bot_field} flag for uuid {uuid_val}: {e}")
        finally:
            if connection.is_connected():
                connection.close()
        time.sleep(delay)
    return False


def update_trade_result(uuid_val, total_closed_pnl, max_retries=5, delay=3):
    for attempt in range(max_retries):
        connection = connect_to_db()
        if connection is None:
            print("Error: Could not establish MySQL connection in update_trade_result.")
            return False
        try:
            cursor = connection.cursor()
            if total_closed_pnl > 0:
                cursor.execute(f"SELECT COALESCE({BOT_FIELD_TP}, 0) FROM `Z-Scores` WHERE uuid = %s", (uuid_val,))
                current_value = cursor.fetchone()[0]
                print(f"Current {BOT_FIELD_TP} for {uuid_val} is {current_value}")
                query = f"UPDATE `Z-Scores` SET {BOT_FIELD_TP} = COALESCE({BOT_FIELD_TP}, 0) + 1 WHERE uuid = %s"
            elif total_closed_pnl < 0:
                cursor.execute(f"SELECT COALESCE({BOT_FIELD_SL}, 0) FROM `Z-Scores` WHERE uuid = %s", (uuid_val,))
                current_value = cursor.fetchone()[0]
                print(f"Current {BOT_FIELD_SL} for {uuid_val} is {current_value}")
                query = f"UPDATE `Z-Scores` SET {BOT_FIELD_SL} = COALESCE({BOT_FIELD_SL}, 0) + 1 WHERE uuid = %s"
            else:
                print("Total Closed PnL is zero; no update to trade results.")
                return True
            cursor.execute(query, (uuid_val,))
            connection.commit()
            if total_closed_pnl > 0:
                cursor.execute(f"SELECT COALESCE({BOT_FIELD_TP}, 0) FROM `Z-Scores` WHERE uuid = %s", (uuid_val,))
                new_value = cursor.fetchone()[0]
            else:
                cursor.execute(f"SELECT COALESCE({BOT_FIELD_SL}, 0) FROM `Z-Scores` WHERE uuid = %s", (uuid_val,))
                new_value = cursor.fetchone()[0]
            if new_value == current_value + 1:
                print(f"Trade result update verified for uuid {uuid_val}: {current_value} -> {new_value}")
                return True
            else:
                print(f"Attempt {attempt+1}: Update not verified (current: {current_value}, new: {new_value}).")
        except Error as e:
            print(f"Error updating trade result for uuid {uuid_val}: {e}")
        finally:
            if connection.is_connected():
                connection.close()
        print(f"Retrying in {delay} seconds...")
        time.sleep(delay)
    return False


def build_open_close_cond(uuid_val):
    cond = ""
    for attempt in range(1, DB_MAX_RETRIES + 1):
        connection = connect_to_db()
        if connection is None:
            print("Error: Could not establish MySQL connection in build_open_close_cond.")
            return cond
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT ADF, `p-value`, HL, Hurst, last_z_score, TP, SL, hl_spread_med, last_spread
                FROM `Z-Scores`
                WHERE uuid = %s
            """, (uuid_val,))
            row = cursor.fetchone()
            if row:
                cond = (
                    f"ADF:{row[0]},p-value:{row[1]},HL:{row[2]},Hurst:{row[3]},"
                    f"z-score:{row[4]},TP:{row[5]},SL:{row[6]},"
                    f"hl_spread_med:{row[7]},last_spread:{row[8]}"
                )
            return cond
        except Error as e:
            if getattr(e, "errno", None) in DEADLOCK_ERROR_CODES:
                print(f"[DEADLOCK] build_open_close_cond attempt {attempt}/{DB_MAX_RETRIES}: {e}")
                if attempt < DB_MAX_RETRIES:
                    time.sleep(DB_BASE_WAIT * attempt)
                    continue
            print(f"Error building open/close cond for uuid={uuid_val}: {e}")
            return cond
        finally:
            if connection and connection.is_connected():
                connection.close()
    print("[CRITICAL] build_open_close_cond(): exceeded max deadlock retries")
    return cond


def insert_trade_open(uuid_val, asset1, asset2, open_cond, bot_num, pos_val=0):
    """
    Inserts an 'open' row and returns its auto-increment ID (trade_id).
    Deadlock-safe with retries.
    """
    for attempt in range(1, DB_MAX_RETRIES + 1):
        connection = None
        try:
            connection = connect_to_db()
            if connection is None:
                print("Error: Could not establish MySQL connection in insert_trade_open.")
                return None
            cursor = connection.cursor()

            open_date = datetime.utcnow().date()
            open_time = datetime.utcnow().time().replace(microsecond=0)

            sql = """
                INSERT INTO `Bot_4_res`
                    (uuid, asset_1, asset_2, open_date, open_time,
                     pnl, closed_by, Bot_num, pos_val, open_cond)
                VALUES
                    (%s,   %s,      %s,      %s,        %s,
                     %s,   %s,       %s,      %s,       %s)
            """
            vals = (
                uuid_val, asset1, asset2, open_date, open_time,
                0.0, None, bot_num, pos_val, open_cond
            )
            cursor.execute(sql, vals)
            connection.commit()

            trade_id = cursor.lastrowid
            return trade_id

        except Error as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            if getattr(e, "errno", None) in DEADLOCK_ERROR_CODES:
                print(f"[DEADLOCK] insert_trade_open attempt {attempt}/{DB_MAX_RETRIES}: {e}")
                if attempt < DB_MAX_RETRIES:
                    time.sleep(DB_BASE_WAIT * attempt)
                    continue
            print(f"[ERROR] insert_trade_open(): {e}")
            return None
        finally:
            if connection and connection.is_connected():
                connection.close()
    print("[CRITICAL] insert_trade_open(): exceeded max deadlock retries")
    return None


def update_trade_close(trade_id, close_reason, pnl_value, pnl_percent,
                       pos_val=None, close_cond=None):
    """
    Updates the exact row opened earlier, using the unique ID.
    Deadlock-safe with retries.
    """
    if not trade_id:
        print("[WARN] update_trade_close(): missing trade_id")
        return False

    for attempt in range(1, DB_MAX_RETRIES + 1):
        connection = None
        try:
            connection = connect_to_db()
            if connection is None:
                print("Error: Could not establish MySQL connection in update_trade_close.")
                return False
            cursor = connection.cursor()

            close_date = datetime.utcnow().date()
            close_time = datetime.utcnow().time().replace(microsecond=0)

            set_cols = [
                "close_date = %s",
                "close_time = %s",
                "closed_by  = %s",
                "pnl        = %s",
                "pnl_pers    = %s",
            ]
            params = [close_date, close_time, close_reason, pnl_value, pnl_percent]

            if pos_val is not None:
                set_cols.append("pos_val = %s")
                params.append(pos_val)
            if close_cond is not None:
                set_cols.append("close_cond = %s")
                params.append(close_cond)

            params.append(trade_id)

            sql = f"""
                UPDATE `Bot_4_res`
                SET {", ".join(set_cols)}
                WHERE id = %s
            """
            cursor.execute(sql, params)
            connection.commit()
            return True

        except Error as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            if getattr(e, "errno", None) in DEADLOCK_ERROR_CODES:
                print(f"[DEADLOCK] update_trade_close attempt {attempt}/{DB_MAX_RETRIES}: {e}")
                if attempt < DB_MAX_RETRIES:
                    time.sleep(DB_BASE_WAIT * attempt)
                    continue
            print(f"[ERROR] update_trade_close(): {e}")
            return False

        finally:
            if connection and connection.is_connected():
                connection.close()

    print("[CRITICAL] update_trade_close(): exceeded max deadlock retries")
    return False


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _mysql_open_to_utc_ms(open_date: str, open_time: str, tz_offset_hours: int = TZ_OFFSET_HOURS) -> int:
    """
    Convert MySQL DATE+TIME (strings) to UTC ms.
    If your DB datetime is local-time, shift by tz_offset_hours to UTC.
    """
    naive = datetime.strptime(f"{open_date} {open_time}", "%Y-%m-%d %H:%M:%S")
    # Treat naive as UTC + offset (i.e., convert to true UTC by applying offset)
    aware_utc = naive.replace(tzinfo=timezone.utc) + timedelta(hours=tz_offset_hours)
    return _to_ms(aware_utc)


def get_trade_open_ts_ms(trade_id: int, tz_offset_hours: int = TZ_OFFSET_HOURS) -> int:
    """
    Read open_date/open_time from Result table for a given ID and return UTC ms.
    """
    conn = connect_to_db()
    if conn is None:
        raise RuntimeError("DB connection failed in get_trade_open_ts_ms()")
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT open_date, open_time
            FROM {RESULT_TABLE}
            WHERE ID = %s
        """, (trade_id,))
        row = cur.fetchone()
        if not row or not row[0] or not row[1]:
            raise RuntimeError(f"No open_date/open_time found for trade_id={trade_id}")
        open_date, open_time = str(row[0]), str(row[1])
        return _mysql_open_to_utc_ms(open_date, open_time, tz_offset_hours)
    finally:
        try:
            if conn.is_connected(): conn.close()
        except: pass


def _sum_closed_pnl_from_positions(positions: list) -> float:
    """
    Sum parsed PnL from CCXT positions history.
    Prefer parsed 'pnl'; fallback to raw 'info.closedPnl'.
    """
    total = 0.0
    for p in positions or []:
        pnl = p.get("pnl")
        if pnl is None:
            try:
                pnl = float(p.get("info", {}).get("closedPnl") or 0.0)
            except Exception:
                pnl = 0.0
        total += float(pnl or 0.0)
    return total


async def _fetch_positions_history_page(bybit, symbol: str, since_ms: int, until_ms: int, limit: int = 100) -> list:
    """
    Single page fetch via CCXT (run in thread). Returns parsed list.
    """
    return await asyncio.to_thread(
        bybit.fetch_positions_history,
        [symbol],               # CCXT expects a single-symbol list to set request['symbol']
        since_ms,
        limit,
        {"until": until_ms, "subType": "linear"}  # USDT-perps
    )


async def sum_closed_pnl_for_window(bybit, symbol: str, since_ms: int, until_ms: int) -> float:
    """
    Sum closed PnL for a symbol over [since_ms, until_ms].
    If you expect >100 rows often, loop over sub-windows (7-day max span per Bybit).
    For most intraday trades, a single page is enough.
    """
    positions = await _fetch_positions_history_page(bybit, symbol, since_ms, until_ms, limit=100)
    return _sum_closed_pnl_from_positions(positions)


async def get_total_closed_pnl_for_trade(bybit, trade_id: int, asset1_symbol: str, asset2_symbol: str) -> float:
    """
    End-to-end: compute trade window and sum closed PnL across both legs.
    Uses open timestamp from DB and 'now' as end boundary (right after closing).
    """
    since_ms = await asyncio.to_thread(get_trade_open_ts_ms, int(trade_id), TZ_OFFSET_HOURS)
    until_ms = int(time.time() * 1000)  # now UTC
    pnl1 = await sum_closed_pnl_for_window(bybit, asset1_symbol, since_ms, until_ms)
    pnl2 = await sum_closed_pnl_for_window(bybit, asset2_symbol, since_ms, until_ms)
    return float(pnl1 + pnl2)


async def place_pair_in_chunks(
    asset1_symbol: str, side1: str, target_usdt_1: float,
    asset2_symbol: str, side2: str, target_usdt_2: float,
    leverage: float,
    chunk_usdt: float = chunk,
    pause_sec: float = pause_s,
) -> bool:
    """
    Alternates orders between both legs in fixed $-notional chunks:
      cycle:
        - market order asset1 for min(chunk_usdt, remaining1)
        - market order asset2 for min(chunk_usdt, remaining2)
        - sleep pause_sec
    Stops when both target notionals are filled (± last price rounding) or if a leg fails;
    on failure, attempts to unwind any already-filled notional on the other leg.

    Returns True if both legs reached target_usdt; False otherwise.
    """
    try:
        # Track remaining notional per leg
        rem1 = float(max(0.0, target_usdt_1))
        rem2 = float(max(0.0, target_usdt_2))

        filled1_amt = 0.0  # base-asset amounts (for unwind if needed)
        filled2_amt = 0.0

        while rem1 > 1e-8 or rem2 > 1e-8:
            # Refresh prices for conversion
            t1 = await with_backoff(bybit.fetch_ticker, asset1_symbol, retries=3, base_delay=1.2)
            t2 = await with_backoff(bybit.fetch_ticker, asset2_symbol, retries=3, base_delay=1.2)

            p1 = float(t1.get("last") or 0.0)
            p2 = float(t2.get("last") or 0.0)
            if p1 <= 0.0 or p2 <= 0.0:
                print(f"{datetime.now()} - ❌ ERROR: Invalid price(s): {asset1_symbol}={p1}, {asset2_symbol}={p2}")
                # attempt unwind if anything filled
                if filled1_amt > 0:
                    await place_order_and_check(asset1_symbol, 'buy' if side1 == 'sell' else 'sell', filled1_amt, leverage)
                if filled2_amt > 0:
                    await place_order_and_check(asset2_symbol, 'buy' if side2 == 'sell' else 'sell', filled2_amt, leverage)
                return False

            # ---- Leg 1 chunk (if remaining) ----
            if rem1 > 1e-8:
                notional1 = min(chunk_usdt, rem1)
                amt1 = max(notional1 / p1, 0.0)
                if amt1 > 0.0:
                    o1 = await place_order_and_check(asset1_symbol, side1, amt1, leverage)
                    if not o1:
                        print(f"{datetime.now()} - ❌ Failed chunk for {asset1_symbol}. Unwinding counterpart if needed.")
                        # unwind any filled on leg 2
                        if filled2_amt > 0:
                            await place_order_and_check(asset2_symbol, 'buy' if side2 == 'sell' else 'sell', filled2_amt, leverage)
                        return False
                    filled1_amt += amt1
                    rem1 -= notional1

            # ---- Leg 2 chunk (if remaining) ----
            if rem2 > 1e-8:
                notional2 = min(chunk_usdt, rem2)
                amt2 = max(notional2 / p2, 0.0)
                if amt2 > 0.0:
                    o2 = await place_order_and_check(asset2_symbol, side2, amt2, leverage)
                    if not o2:
                        print(f"{datetime.now()} - ❌ Failed chunk for {asset2_symbol}. Unwinding counterpart if needed.")
                        # unwind any filled on leg 1
                        if filled1_amt > 0:
                            await place_order_and_check(asset1_symbol, 'buy' if side1 == 'sell' else 'sell', filled1_amt, leverage)
                        return False
                    filled2_amt += amt2
                    rem2 -= notional2

            # Pause between cycles to reduce market impact
            await asyncio.sleep(pause_sec)

        print(f"{datetime.now()} - ✅ Chunked pair fully executed.")
        return True

    except Exception as e:
        print(f"{datetime.now()} - ❌ ERROR in place_pair_in_chunks: {e}")
        return False



async def execute_trade_logic():
    while True:
        # Balance check
        try:
            balance = await get_cached_balance(bybit, ttl=30)
        except Exception as e:
            print(f"{datetime.now()} - NETFAIL balance (cached): {e}")
            await asyncio.sleep(10)
            continue
        total_balance = float(balance.get('total', {}).get('USDT', 0))
        free_balance = balance.get('free', {}).get('USDT', None)
        if free_balance is None:
            free_balance = float(balance['info']['result']['list'][0]['totalAvailableBalance'])
        if free_balance < total_balance * balance_req:
            print(f"{datetime.now()} - Not enough balance for trading")
            await asyncio.sleep(SPL)
            continue

        guard = await asyncio.to_thread(
            daily_guard_eval_and_act, bybit, connect_to_db, BOT_NUM, RESULT_TABLE, DAILY_TP, DAILY_SL
        )
        if guard.get("blocked"):
            logging.warning(f"[Daily] Blocked today: {guard.get('action')} | pnl={guard['pnl_pct_now']:.2f}%")
            await asyncio.sleep(30)
            continue

        if is_in_open_blackout():
            logging.info("[Schedule] Blackout window active (Fri 14:00 → Sun 14:00). Not opening new trades.")
            await asyncio.sleep(SPL1)
            continue

        throttled_today = bool(guard.get("throttled", False))
        logging.info(
            f"[Daily] throttle={throttled_today}, tp_stage={guard.get('tp_stage')}, tp_next={guard.get('tp_next')}")


        (uuid_val,
         asset1_symbol,
         asset2_symbol,
         last_z_score,
         pair_tp,
         pair_sl,
         _level,
         _adf,
         _pval,
         pair_hl,
         _hl_spread_med,
         _last_spread,
         asset1_5m_vol, asset2_5m_vol, beta_norm) = await get_first_asset_pair()

        if not asset1_symbol or not asset2_symbol:
            print(f"{datetime.now()} - INFO: No more pairs available. Sleeping for {SPL} seconds...")
            await asyncio.sleep(SPL)
            continue

        if pair_tp is None or pair_sl is None:
            print(f"TP or SL not set for {asset1_symbol}/{asset2_symbol}. Skipping pair.")
            await asyncio.sleep(SPL)
            continue

        print(f"{datetime.now()} - INFO: Proceeding with asset pair: {asset1_symbol}/{asset2_symbol} and Z-Score: {last_z_score}")
        logger = get_pair_logger(asset1_symbol, asset2_symbol)
        logger.info(f"Started trading for {asset1_symbol}/{asset2_symbol} with initial Z-score: {last_z_score}")

        try:
            z_score_upper_threshold = 2
            z_score_lower_threshold = -2
            if last_z_score >= z_score_upper_threshold or last_z_score <= z_score_lower_threshold:
                side1, side2 = ('sell', 'buy') if last_z_score > 0 else ('buy', 'sell')

                leverage_data = await calculate_dynamic_leverage(bybit, asset1_symbol, asset2_symbol, asset1_5m_vol,
                                                                 asset2_5m_vol, beta_norm,
                                                                 throttle_mode=throttled_today)
                if not leverage_data:
                    print(f"{datetime.now()} - ERROR: Failed to calculate leverage. Exiting trade execution.")
                    logger.error("Failed to calculate leverage. Exiting trade execution.")
                    continue

                exposure_asset1 = leverage_data["exposure_asset1"]
                exposure_asset2 = leverage_data["exposure_asset2"]
                amount_asset1 = leverage_data["amount_asset1"]
                amount_asset2 = leverage_data["amount_asset2"]
                dynamic_leverage = leverage_data["leverage"]
                total_exposure = leverage_data["total_exposure"]

                print(f"Leverage: {dynamic_leverage}x, "
            f"Exposures: {exposure_asset1:.2f} / {exposure_asset2:.2f} USDT "
            f"(total {total_exposure:.2f})")
                logger.info(f"Leverage: {dynamic_leverage}x, "
            f"Exposures: {exposure_asset1:.2f} / {exposure_asset2:.2f} USDT "
            f"(total {total_exposure:.2f})")

                await set_leverage_with_retry(bybit, asset1_symbol, dynamic_leverage)
                await set_leverage_with_retry(bybit, asset2_symbol, dynamic_leverage)

                # Determine which leg is long/short based on sides
                if side1 == 'buy' and side2 == 'sell':
                    long_symbol, long_amount = asset1_symbol, exposure_asset1
                    short_symbol, short_amount = asset2_symbol, exposure_asset2
                else:
                    long_symbol, long_amount = asset2_symbol, exposure_asset2
                    short_symbol, short_amount = asset1_symbol, exposure_asset1

                # Send Telegram "OPEN" signal (off-thread)
                try:
                    ok = await asyncio.to_thread(
                        send_open_signal,
                        uuid=str(uuid_val),
                        long_symbol=long_symbol, long_amount=float(long_amount),
                        short_symbol=short_symbol, short_amount=float(short_amount),
                        z_score=float(last_z_score),
                        level=f"{_level if _level is not None else LEVEL} | {BOT_NUM}"
                    )
                    logger.info(f"[TG] OPEN signal sent: {ok}")
                except Exception as e:
                    logger.error(f"[TG] OPEN signal error: {e}")


                print(f"{datetime.now()} - INFO: Placing orders...")
                logger.info("Placing orders...")

                price_asset1 = await with_backoff(bybit.fetch_ticker, asset1_symbol, retries=3, base_delay=1.2)
                price_asset2 = await with_backoff(bybit.fetch_ticker, asset2_symbol, retries=3, base_delay=1.2)
                price_asset1 = price_asset1.get('last', 0)
                price_asset2 = price_asset2.get('last', 0)

                print(f"Checking which asset is more expensive to prioritize opening that leg first...")
                if price_asset1 >= price_asset2:
                    first_symbol, first_side, first_amount, first_price = asset1_symbol, side1, amount_asset1, price_asset1
                    second_symbol, second_side, second_amount, second_price = asset2_symbol, side2, amount_asset2, price_asset2
                else:
                    first_symbol, first_side, first_amount, first_price = asset2_symbol, side2, amount_asset2, price_asset2
                    second_symbol, second_side, second_amount, second_price = asset1_symbol, side1, amount_asset1, price_asset1

                print(f"First to open: {first_symbol} ({first_side}, {first_amount} @ {first_price})")
                print(f"Second to open: {second_symbol} ({second_side}, {second_amount} @ {second_price})")

                # ---- Open both legs in $100 chunks
                # Convert target amounts to target notionals (USDT) using latest prices
                t1 = await asyncio.to_thread(bybit.fetch_ticker, asset1_symbol)
                t2 = await asyncio.to_thread(bybit.fetch_ticker, asset2_symbol)
                p1 = float(t1.get("last") or 0.0)
                p2 = float(t2.get("last") or 0.0)
                if p1 <= 0.0 or p2 <= 0.0:
                    print(f"{datetime.now()} - ❌ ERROR: Invalid prices for chunked open.")
                    continue

                target_usdt_1 = amount_asset1 * p1
                target_usdt_2 = amount_asset2 * p2

                ok_pair = await place_pair_in_chunks(
                    asset1_symbol, side1, target_usdt_1,
                    asset2_symbol, side2, target_usdt_2,
                    dynamic_leverage,
                    chunk_usdt=chunk,
                    pause_sec=pause_s,
                )
                if not ok_pair:
                    print(f"{datetime.now()} - ❌ Pair open (chunked) failed/unwound. Skipping this trade.")
                    continue

                print("Both legs opened successfully (chunked). Proceeding to monitoring...")

                # After: insert_trade_open(...)
                open_dt = datetime.now()  # keep consistent with the rest of your codebase
                hl_bars = int(pair_hl) if pair_hl is not None else 0
                hl_minutes = hl_bars * TIMEFRAME_MIN_PER_BAR
                close_deadline = open_dt + timedelta(minutes=hl_minutes) if hl_minutes > 0 else None

                # === COLLECT & SIGN OPEN CONDITIONS ===
                open_cond = await asyncio.to_thread(build_open_close_cond, uuid_val)
                # compute open_cond above
                trade_id = await asyncio.to_thread(
                    insert_trade_open,
                    uuid_val,
                    asset1_symbol,
                    asset2_symbol,
                    open_cond,  # 4th = open_cond (ok)
                    BOT_NUM,  # 5th = bot_num    ✅
                    total_exposure  # 6th = pos_val    ✅
                )
                if not trade_id:
                    return

                print(f"{datetime.now()} - INFO: Monitoring positions.")
                logger.info(f"Monitoring positions for {asset1_symbol}/{asset2_symbol}.")
                closed_by_str = 'Some extra'
                previous_z_score = last_z_score

                while True:  # Monitoring part
                    # ---- Daily guard check inside monitoring ----
                    guard = await asyncio.to_thread(
                        daily_guard_eval_and_act, bybit, connect_to_db, BOT_NUM, RESULT_TABLE, DAILY_TP, DAILY_SL
                    )
                    act = guard.get("action")
                    if act in ("loss_stop_signal", "loss_blocked"):
                        closed_by_str = "daily SL reached"
                        logger.warning(f"[Daily] Guard hit: {closed_by_str} | pnl={guard['pnl_pct_now']:.2f}%")
                        break
                    elif act == "profit_lock_throttle":
                        # don't close; just acknowledge that throttle is in effect
                        logger.info(f"[Daily] Throttle in effect. Next TP target: {guard.get('tp_next'):.2f}%")

                    new_z, coint_flag = await refresh_z_score_for_pair(uuid_val)
                    if new_z is None:
                        print(f"{datetime.now()} - ERROR: Could not refresh Z-score for {asset1_symbol}/{asset2_symbol}. Exiting monitoring.")
                        logger.error(f"Could not refresh Z-score for {asset1_symbol}/{asset2_symbol}. Exiting monitoring.")

                        closed_by_str = 'Z-Score refresh break'
                        break

                    last_z_score = new_z
                    pos_value_1 = await fetch_position_value(asset1_symbol)
                    pos_value_2 = await fetch_position_value(asset2_symbol)
                    print(f"{datetime.now()} - INFO: Position Value: {pos_value_1} for {asset1_symbol}")
                    print(f"{datetime.now()} - INFO: Position Value: {pos_value_2} for {asset2_symbol}")

                    total_pnl = await fetch_unrealized_pnl(bybit, asset1_symbol, asset2_symbol)
                    if total_pnl is None:
                        print(f"{datetime.now()} - ERROR: Failed to fetch unrealized PnL. Skipping check.")
                        logger.error("Failed to fetch unrealized PnL. Exiting monitoring.")

                        closed_by_str = 'Failed to fetch pnl'
                        break

                    pnl_percentage = (total_pnl / total_exposure) * 100
                    print(f"{datetime.now()} - INFO: Unrealized PnL: {total_pnl} USDT ({pnl_percentage:.2f}%)")
                    logger.info(f"Unrealized PnL: {total_pnl} USDT ({pnl_percentage:.2f}%)")

                    if pnl_percentage >= float(pair_tp):
                        print(f"{datetime.now()} - WARNING: Unrealized PnL hit tp% threshold. Closing positions...")
                        logger.warning(
                            f"Unrealized PnL hit tp% threshold. Closing positions for {asset1_symbol}/{asset2_symbol}")

                        closed_by_str = 'Pnl % take-profit'
                        break

                    if pnl_percentage <= -float(pair_sl):
                        print(f"{datetime.now()} - WARNING: Unrealized PnL hit sl% threshold. Closing positions...")
                        logger.warning(
                            f"Unrealized PnL hit sl% threshold. Closing positions for {asset1_symbol}/{asset2_symbol}")

                        closed_by_str = 'Pnl % stop-loss'
                        break

                    # Time-limit exit: if open longer than HL bars (converted to minutes) AND cointegration lost
                    if close_deadline is not None and (datetime.now() >= close_deadline):
                        if coint_flag is not None and int(coint_flag) == 0:
                            print(
                                f"{datetime.now()} - WARNING: HL time exceeded and cointegration lost. Closing positions...")
                            logger.warning(
                                f"HL time exceeded and cointegration lost for {asset1_symbol}/{asset2_symbol}.")
                            closed_by_str = 'HL timeout & lost cointegration'
                            break

                    stop_loss_threshold = z_exit
                    if abs(last_z_score) >= stop_loss_threshold:
                        if coint_flag is not None and int(coint_flag) == 0:
                            print(f"{datetime.now()} - WARNING: Z-score stop-loss triggered. Closing positions...")
                            logger.warning(f"Z-score stop-loss triggered for {asset1_symbol}/{asset2_symbol}.")

                            closed_by_str = 'Stop_loss_threshold & lost cointegration'
                            break

                    if (previous_z_score > 0 and last_z_score <= 0) or (previous_z_score < 0 and last_z_score >= 0):
                        print(f"{datetime.now()} - WARNING: Z-score crossed zero! Trend change detected, closing positions...")
                        logger.warning(f"Z-score crossed zero for {asset1_symbol}/{asset2_symbol}. Trend change detected.")

                        closed_by_str = 'Z-score changed trend. Cross 0'
                        break

                    previous_z_score = last_z_score
                    print(f"{datetime.now()} - INFO: Sleeping for {SPL1} seconds...")
                    logger.info(f"Sleeping for {SPL1} seconds...")
                    await asyncio.sleep(SPL1)

                print(f"{datetime.now()} - INFO: Closing positions...")
                logger.info(f"Closing positions for {asset1_symbol}/{asset2_symbol}.")

                # ---- Fetch live position sizes to close EXACT amount ----
                pos_amt1 = await fetch_position_amount(asset1_symbol)
                pos_amt2 = await fetch_position_amount(asset2_symbol)

                print(f"{datetime.now()} - INFO: Live position amount {asset1_symbol} = {pos_amt1}")
                print(f"{datetime.now()} - INFO: Live position amount {asset2_symbol} = {pos_amt2}")

                if pos_amt1 <= 0 and pos_amt2 <= 0:
                    print(f"{datetime.now()} - WARNING: No open positions detected for "
                          f"{asset1_symbol}/{asset2_symbol} at close time.")
                else:
                    # Close trades (flip sides)
                    close_side1 = 'buy' if side1 == 'sell' else 'sell'
                    close_side2 = 'buy' if side2 == 'sell' else 'sell'

                    # Compute current notionals to close (live amount * current price)
                    t1c = await asyncio.to_thread(bybit.fetch_ticker, asset1_symbol)
                    t2c = await asyncio.to_thread(bybit.fetch_ticker, asset2_symbol)
                    p1c = float(t1c.get('last') or 0.0)
                    p2c = float(t2c.get('last') or 0.0)

                    if p1c <= 0.0 or p2c <= 0.0:
                        print(f"{datetime.now()} - ❌ ERROR: Invalid prices for chunked close.")
                        # fall back to single-shot closes with live sizes
                        if pos_amt1 > 0:
                            await place_order_and_check(
                                asset1_symbol, close_side1, pos_amt1, dynamic_leverage
                            )
                        if pos_amt2 > 0:
                            await place_order_and_check(
                                asset2_symbol, close_side2, pos_amt2, dynamic_leverage
                            )
                    else:
                        target_close_usdt_1 = pos_amt1 * p1c
                        target_close_usdt_2 = pos_amt2 * p2c

                        close_ok = await place_pair_in_chunks(
                            asset1_symbol, close_side1, target_close_usdt_1,
                            asset2_symbol, close_side2, target_close_usdt_2,
                            dynamic_leverage,
                            chunk_usdt=chunk,
                            pause_sec=pause_s,
                        )
                        if not close_ok:
                            print(f"{datetime.now()} - ⚠️ Chunked close incomplete; consider manual review.")


                # Use the same mapping for short/long as at open
                if side1 == 'buy' and side2 == 'sell':
                    long_symbol, long_amount = asset1_symbol, amount_asset1
                    short_symbol, short_amount = asset2_symbol, amount_asset2
                else:
                    long_symbol, long_amount = asset2_symbol, amount_asset2
                    short_symbol, short_amount = asset1_symbol, amount_asset1

                # Send Telegram "CLOSE" signal (off-thread)
                try:
                    ok = await asyncio.to_thread(
                        send_close_signal,
                        uuid=str(uuid_val),
                        short_symbol=short_symbol, short_amount=float(short_amount),
                        long_symbol=long_symbol, long_amount=float(long_amount),
                        reason=f"{closed_by_str} | size={total_exposure:.2f} USDT | bot={BOT_NUM}",
                        level=f"{_level if _level is not None else LEVEL} | {BOT_NUM}"
                    )
                    logger.info(f"[TG] CLOSE signal sent: {ok}")
                except Exception as e:
                    logger.error(f"[TG] CLOSE signal error: {e}")

                await asyncio.sleep(5)

                # Sum all closed-PnL rows for both legs within the trade window (open_ts → now)
                try:
                    total_closed_pnl = await get_total_closed_pnl_for_trade(bybit, int(trade_id), asset1_symbol,
                                                                            asset2_symbol)
                except Exception as e:
                    print(f"{datetime.now()} - ❌ ERROR computing total_closed_pnl via history: {e}")
                    total_closed_pnl = 0.0

                pnl_percent = (total_closed_pnl / total_exposure) if total_exposure else None
                pnl_percent = 0.0 if pnl_percent is None else float(pnl_percent)

                # === COLLECT & SIGN CLOSE CONDITIONS ===
                close_cond = await asyncio.to_thread(build_open_close_cond, uuid_val)
                await asyncio.to_thread(
                    update_trade_close,
                    trade_id,
                    closed_by_str,
                    total_closed_pnl,
                    pnl_percent,
                    None,  # pos_val
                    close_cond
                )
                
                if not ok_close_db:
                	msg = f"DB CLOSE UPDATE FAILED trade_id={trade_id} uuid={uuid_val} pair={asset1_symbol}/{asset2_symbol}"
                logger.error(msg)
                await tg_alert(msg)

                update_success = await asyncio.to_thread(update_trade_result, uuid_val, total_closed_pnl)
                if update_success:
                    logger.info(f"Trade result updated for pair {asset1_symbol}/{asset2_symbol}.")
                else:
                    logger.error(f"Failed to update trade result for pair {asset1_symbol}/{asset2_symbol}.")

                try:
                    reset_success = await asyncio.to_thread(reset_bot_flag_in_db, uuid_val, BOT_FIELD)
                    if reset_success:
                        logger.info(f"{BOT_FIELD} flag reset for pair {asset1_symbol}/{asset2_symbol}.")
                    else:
                        logger.error(f"Failed to reset {BOT_FIELD} flag for pair {asset1_symbol}/{asset2_symbol}.")
                except Exception as e:
                    logger.error(f"Exception while resetting {BOT_FIELD} for uuid={uuid_val}: {e}")
                continue


        except Exception as e:
            err_msg = (
                f"ERROR in execute_trade_logic for {asset1_symbol}/{asset2_symbol} "
                f"({BOT_NUM}): {type(e).__name__}: {e}")
            print(f"{datetime.now()} - {err_msg}")
            logger.error(err_msg)
            # send Telegram alert about workflow-breaking error
            try:
                await tg_alert(err_msg)
            except Exception as tg_err:
                print(f"[TG] Failed to send workflow-error alert: {tg_err}")
            await asyncio.sleep(SPL)


def trade_pair_thread(thread_id: int = 1):  # default id
    while True:
        loop = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(execute_trade_logic())
        except Exception as e:
            err_msg = f"THREAD-{thread_id} crashed:\n{type(e).__name__}: {e}"
            print(f"{datetime.now()} - {err_msg}")
            try:
                asyncio.run(tg_alert(err_msg))
            except Exception as tg_err:
                print(f"[TG] Failed to send crash alert: {tg_err}")
            time.sleep(10)
        finally:
            if loop is not None:
                try:
                    loop.close()
                except Exception:
                    pass


if __name__ == "__main__":
    trading_threads = []
    for _ in range(MAX_TRADING_THREADS):
        thread = threading.Thread(target=trade_pair_thread, daemon=True)
        thread.start()
        trading_threads.append(thread)
        time.sleep(10)
    print("🚀 Multi-threaded trading bot started.")
    while True:
        time.sleep(SPL1)
