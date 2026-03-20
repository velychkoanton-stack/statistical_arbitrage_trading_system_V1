import pandas as pd
import numpy as np
from datetime import datetime, timezone
import time
import ccxt
import asyncio
import os
from mysql.connector import connect, Error
import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS
from statsmodels.tsa.stattools import adfuller
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta, timezone
import asyncio

# === Utility/stat functions ===

def calculate_half_life(data_set):
    try:
        z_array = data_set.values
        if len(z_array) < 10:
            print("Half-life calculation skipped: not enough data.")
            return np.nan
        z_lag = np.roll(z_array, 1)
        z_lag[0] = 0
        z_ret = z_array - z_lag
        z_ret[0] = 0
        z_lag = sm.add_constant(z_lag)
        model = sm.OLS(z_ret, z_lag)
        res = model.fit()
        beta = res.params[1]
        if beta == 0:
            print("Warning: Beta=0, half-life undefined.")
            return np.nan
        halflife = -np.log(2) / beta
        print(f"Half-life of mean reversion: {halflife:.2f}")
        return halflife
    except Exception as e:
        print(f"Error calculating half-life: {e}")
        return np.nan

# === Database connection ===

def connect_to_db():
    try:
        connection = connect(
            host="xxxx",
            user="xxxx",
            password="xxxx",
            database="xxxx"
        )
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# === Bybit API key loading ===

def read_api_credentials():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "Linux_Demo_1_1.txt")
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

api_credentials = read_api_credentials()
bybit = ccxt.bybit({
    'apiKey': api_credentials.get('api_key'),
    'secret': api_credentials.get('api_secret'),
    'enableRateLimit': True
})

print(f"Bybit initialized with API key: {api_credentials.get('api_key')[:4]}****")

# === Asset pairs CSV reader ===

async def fetch_cached(cache: dict, symbol: str, timeframe: str, limit: int):
    """
    Fetch OHLCV with caching per cycle. Always returns a cleaned DataFrame.
    """
    if symbol in cache:
        return cache[symbol]
    data = await asyncio.to_thread(bybit.fetch_ohlcv, symbol, timeframe, None, limit, {})
    df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df.dropna(inplace=True)
    cache[symbol] = df
    return df



def get_asset_pairs():
    """
    Load pairs from MySQL `Z-Scores` instead of CSV.
    Returns a list of tuples: (asset1, asset2, HL, TP, SL)
    """
    cn = connect_to_db()
    if cn is None:
        print("ERROR: DB connection failed in get_asset_pairs().")
        return []

    rows = []
    try:
        cur = cn.cursor()
        # Minimal filter: need assets + HL/TP/SL present.
        # (If you want extra filters like Coint=1 later, we can add them.)
        sql = """
            SELECT asset_1, asset_2, HL, TP, SL
            FROM `Z-Scores`
            WHERE asset_1 IS NOT NULL AND asset_2 IS NOT NULL
              AND HL IS NOT NULL AND TP IS NOT NULL AND SL IS NOT NULL
        """
        cur.execute(sql)
        for a1, a2, hl, tp, sl in cur.fetchall():
            try:
                rows.append((
                    str(a1).strip(),
                    str(a2).strip(),
                    int(hl),
                    int(tp),
                    int(sl)
                ))
            except Exception:
                # Skip malformed rows but keep the loop going
                continue
        cur.close()
    except Exception as e:
        print(f"Error reading pairs from DB: {e}")
    finally:
        try:
            if cn.is_connected():
                cn.close()
        except Exception:
            pass

    if not rows:
        print(f"{datetime.now()} - INFO: No trading pairs found in Z-Scores.")
    else:
        print(f"Asset pairs from DB (count={len(rows)}): {rows[:5]}{'...' if len(rows)>5 else ''}")
    return rows


# === Main analytics functions ===

def hurst_exponent(spread):
    try:
        spread = spread.dropna()
        if len(spread) < 100:
            print(f"Skipping Hurst calculation - insufficient data ({len(spread)} rows).")
            return None
        ts = spread.values
        lags = range(2, 100)
        tau = [np.sqrt(np.std(np.subtract(ts[lag:], ts[:-lag]))) for lag in lags]
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
        hurst_value = poly[0] * 2.0
        print(f"Hurst Exponent: {hurst_value:.4f}")
        return hurst_value
    except Exception as e:
        print(f"Error calculating Hurst exponent: {e}")
        return None


def compute_usdt_volumes_from_5m(df: pd.DataFrame):
    """
    Compute USDT-denominated volumes from 5m OHLCV with strict candle handling:
      - Drop the last row because it may be open/incomplete.
      - 5m volume (USDT): AVERAGE over the last 12 CLOSED 5m candles [-13..-2] of:
            per-candle USDT vol = Volume * ((Open + Close) / 2)
      - 1h volume (USDT): last 12 CLOSED candles [-13..-2]
            vol1h_usdt = (sum Volume[-13..-2]) * ((max High[-13..-2] + min Low[-13..-2]) / 2)

    Returns (vol5m_usdt_avg, vol1h_usdt) as floats or None if not enough data.
    """
    try:
        # Need at least 14 rows so that after dropping the last (open) candle,
        # we still have 13+ rows and can take [-13..-2] = 12 candles.
        if df is None or len(df) < 14:
            return None, None

        # Drop the last (possibly open) candle
        closed = df.iloc[:-1]
        if len(closed) < 13:
            return None, None

        # Last 12 CLOSED 5m candles ending at pre-last: [-13..-2] in original df
        last12 = closed.iloc[-12:]

        # --- 5m (AVG over last hour, candle-by-candle OC-mid) ---
        oc_mid = (last12["Open"].astype(float) + last12["Close"].astype(float)) / 2.0
        vol_usdt_each = last12["Volume"].astype(float) * oc_mid
        vol5m_usdt_avg = float(vol_usdt_each.mean())

        # --- 1h (sum of volumes × midpoint of (global High, global Low)) ---
        v1h_sum = float(last12["Volume"].sum())
        high_max = float(last12["High"].max())
        low_min = float(last12["Low"].min())
        mid1h_avg = (high_max + low_min) / 2.0
        vol1h_usdt = v1h_sum * mid1h_avg

        return vol5m_usdt_avg, vol1h_usdt
    except Exception:
        return None, None



def calculate_z_score(asset1_symbol, asset2_symbol, HL, df1, df2, TP, SL):
    try:
        # --- Hedge ratio (simple OLS via sklearn) ---
        hedge_ratio = LinearRegression().fit(df2[['Close']], df1['Close']).coef_[0]
        if not np.isfinite(hedge_ratio) or hedge_ratio == 0:
            print(f"Bad hedge_ratio for {asset1_symbol}-{asset2_symbol}: {hedge_ratio}")
            return

        # --- Spread and variance guard ---
        spread = df1['Close'] - hedge_ratio * df2['Close']
        spread_std = spread.std()
        if not np.isfinite(spread_std) or spread_std == 0:
            print(f"Spread variance zero/NaN for {asset1_symbol}-{asset2_symbol}; skipping.")
            return

        model = LinearRegression().fit(df2[['Close']], df1['Close'])
        beta_raw = model.coef_[0]
        price1 = df1['Close'].iloc[-1]
        price2 = df2['Close'].iloc[-1]
        beta_norm = beta_raw * (price2 / price1)

        # --- USDT-denominated volumes for each leg from 5m OHLCV ---
        asset1_5m_vol_usdt, asset1_1h_vol_usdt = compute_usdt_volumes_from_5m(df1)
        asset2_5m_vol_usdt, asset2_1h_vol_usdt = compute_usdt_volumes_from_5m(df2)

        # --- Half-life calculation & selection ---
        hl_calc = calculate_half_life(spread)
        if np.isfinite(hl_calc) and hl_calc >= 60:
            hl_used = int(round(hl_calc))
        else:
            hl_used = int(HL)

        # --- Spread ratio metrics (sign-agnostic later usage) ---
        ratio_series = df1['Close'] / (hedge_ratio * df2['Close'])
        win_ratio = min(hl_used, len(ratio_series))
        hl_spread_med = float(ratio_series.iloc[-win_ratio:].median()) if win_ratio > 0 else float('nan')
        hl_spread_med = abs(hl_spread_med)
        last_spread = float(ratio_series.iloc[-1]) if len(ratio_series) else float('nan')
        last_spread = abs(last_spread)

        # --- Z-scores across full series; extremes over last hl_used bars ---
        z_scores = (spread - spread.mean()) / spread_std
        win = min(hl_used, len(z_scores))
        last_z_score = float(z_scores.iloc[-1])
        max_z_score = float(z_scores.iloc[-win:].max())
        min_z_score = float(z_scores.iloc[-win:].min())

        # --- Record identity & timestamps ---
        record_uuid = f"{asset1_symbol.split('/')[0]}_{asset2_symbol.split('/')[0]}"
        last_dt = datetime.now(timezone.utc)
        last_date = last_dt.date()
        last_time = last_dt.time()

        # --- DB connection ---
        connection = connect_to_db()
        if connection is None or not connection.is_connected():
            print("ERROR: Could not establish MySQL connection. Skipping upsert.")
            return

        cursor = connection.cursor()

        # --- UPSERT with new volume columns ---
        sql = """
        INSERT INTO `Z-Scores`
          (uuid, asset_1, asset_2, last_date, last_time,
           last_z_score, max_z_score, min_z_score,
           HL, hl_spread_med, `last_spread`,
           TP, SL, bot_1, bot_2, bot_3,
           Asset1_5m_vol, Asset1_1h_vol, Asset2_5m_vol, Asset2_1h_vol, beta, beta_norm)
        VALUES
          (%s, %s, %s, %s, %s,
           %s, %s, %s,
           %s, %s, %s,
           %s, %s, 0, 0, 0,
           %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          asset_1 = VALUES(asset_1),
          asset_2 = VALUES(asset_2),
          last_date = VALUES(last_date),
          last_time = VALUES(last_time),
          last_z_score = VALUES(last_z_score),
          max_z_score = VALUES(max_z_score),
          min_z_score = VALUES(min_z_score),
          HL = VALUES(HL),
          hl_spread_med = VALUES(hl_spread_med),
          `last_spread` = VALUES(`last_spread`),
          TP = VALUES(TP),
          SL = VALUES(SL),
          Asset1_5m_vol = VALUES(Asset1_5m_vol),
          Asset1_1h_vol = VALUES(Asset1_1h_vol),
          Asset2_5m_vol = VALUES(Asset2_5m_vol),
          Asset2_1h_vol = VALUES(Asset2_1h_vol),
          beta = VALUES(beta),
          beta_norm = VALUES(beta_norm);
        """

        # Safer handling for TP/SL possibly being None
        tp_val = int(TP) if TP is not None else None
        sl_val = int(SL) if SL is not None else None

        params = (
            record_uuid, asset1_symbol, asset2_symbol, last_date, last_time,
            last_z_score, max_z_score, min_z_score,
            hl_used, hl_spread_med, last_spread,
            tp_val, sl_val,
            asset1_5m_vol_usdt, asset1_1h_vol_usdt, asset2_5m_vol_usdt, asset2_1h_vol_usdt, float(hedge_ratio),
            float(beta_norm)
        )

        cursor.execute(sql, params)
        connection.commit()
        cursor.close()

    except Exception as e:
        print(f"Error in calculate_z_score for {asset1_symbol}-{asset2_symbol}: {e}")


def coint_test(asset1_symbol, asset2_symbol, HL, df1, df2):
    adf_threshold = -2.5
    pvalue_threshold = 0.1
    try:
        X = sm.add_constant(df2['Close'])
        Y = df1['Close']
        model = OLS(Y, X).fit()
        beta_hr = model.params.iloc[1]
        spread = Y - beta_hr * df2['Close']
        adf_stat, pvalue, *_ = adfuller(spread)
        hurst_val = hurst_exponent(spread)
        record_uuid = f"{asset1_symbol.split('/')[0]}_{asset2_symbol.split('/')[0]}"

        connection = connect_to_db()
        if connection is not None and connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE `Z-Scores` SET ADF=%s, `p-value`=%s, Hurst=%s WHERE uuid=%s",
                (adf_stat, pvalue, hurst_val, record_uuid)
            )
            coint = int((adf_stat < adf_threshold) and (pvalue < pvalue_threshold))
            print(f"{asset1_symbol}-{asset2_symbol}: ADF={adf_stat:.3f}, p-value={pvalue:.3f}, Hurst={hurst_val}, Coint={coint}")
            cursor.execute("UPDATE `Z-Scores` SET Coint=%s WHERE uuid=%s", (coint, record_uuid))
            connection.commit()
            cursor.close()
    except Exception as e:
        print(f"Error in coint_test for {asset1_symbol}-{asset2_symbol}: {e}")


def cleanup_dead_pair(asset1, asset2):
    # 1. Remove from CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "list1.csv")
    try:
        df = pd.read_csv(file_path, header=None)
        initial_len = len(df)
        # Each row is "asset1,asset2,HL,TP,SL" as a string
        def is_this_pair(row):
            parts = row[0].split(',')
            a, b = parts[0].strip(), parts[1].strip()
            return (a == asset1 and b == asset2) or (a == asset2 and b == asset1)
        df = df[~df.apply(is_this_pair, axis=1)]
        if len(df) < initial_len:
            df.to_csv(file_path, header=False, index=False)
            print(f"Removed {asset1}-{asset2} from CSV.")
    except Exception as e:
        print(f"Error cleaning up CSV for pair {asset1}-{asset2}: {e}")

    # 2. Remove from Z-Scores table
    try:
        uuid = f"{asset1.split('/')[0]}_{asset2.split('/')[0]}"
        connection = connect_to_db()
        if connection is not None and connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("DELETE FROM `Z-Scores` WHERE uuid=%s", (uuid,))
            connection.commit()
            cursor.close()
            print(f"Removed {uuid} from Z-Scores DB.")
    except Exception as e:
        print(f"Error cleaning up DB for pair {asset1}-{asset2}: {e}")

# === Main update loop ===

async def update_z_scores_periodically():
    timeframe = '5m'
    limit = 1000
    cycle_time = 300  # seconds (5 minutes)

    while True:
        start_time = time.time()
        asset_pairs = get_asset_pairs()
        if not asset_pairs:
            print(f"{datetime.now()} - INFO: No more asset pairs to process.")
            break

        # NEW: cache for this cycle
        ohlcv_cache = {}

        for asset1, asset2, HL, TP, SL in asset_pairs:
            try:
                # Fetch both symbols concurrently, cached per cycle
                df1_task = fetch_cached(ohlcv_cache, asset1, timeframe, limit)
                df2_task = fetch_cached(ohlcv_cache, asset2, timeframe, limit)
                df1, df2 = await asyncio.gather(df1_task, df2_task)

                # Stats writes off the event loop
                await asyncio.to_thread(calculate_z_score, asset1, asset2, HL, df1, df2, TP, SL)
                await asyncio.to_thread(coint_test, asset1, asset2, HL, df1, df2)

            except Exception as e:
                if 'does not have market symbol' in str(e):
                    cleanup_dead_pair(asset1, asset2)
                print(f"Error processing pair {asset1}-{asset2}: {e}")

        elapsed = time.time() - start_time
        elapsed = round(elapsed)
        sleep_time = max(0, cycle_time - elapsed)
        print(f"Cycle finished in {elapsed:.1f} seconds. Sleeping for {sleep_time:.1f} seconds.")
        print(f"Current time: {datetime.now()}")
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    async def _starter():
        # Use UTC (your server is UTC); switch to datetime.now() if you prefer local time.
        now = datetime.now(timezone.utc)
        # Minutes to add to reach the next multiple of 5
        add_min = (5 - (now.minute % 5)) % 5
        # If we're mid-minute (seconds > 0) exactly on a 5-min mark, push to the next one
        if add_min == 0 and now.second > 0:
            add_min = 5
        next_run = (now + timedelta(minutes=add_min)).replace(second=0, microsecond=0)
        sleep_sec = (next_run - now).total_seconds()
        print(f"Aligning to next 5-min boundary at {next_run.isoformat()} (sleep {int(sleep_sec)}s)")
        await asyncio.sleep(sleep_sec)

        # Start your periodic job after alignment
        await update_z_scores_periodically()

    asyncio.run(_starter())
