import pandas as pd
from datetime import datetime
import time
from mysql.connector import connect, Error
import os
from datetime import datetime, timedelta, timezone
import asyncio

# === Database connection ===

def connect_to_db():
    try:
        connection = connect(
            host="localhost",
            user="root",
            password="s0406001_A11",
            database="MyDB"
        )
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# === Trade statistics updater ===

def update_trade_statistics():
    connection = connect_to_db()
    if connection is None:
        print("DB connection failed.")
        return

    # === MAIN CHANGE: Use pnl_pers for stats ===
    query = """
        SELECT uuid, pnl_pers FROM Bot_1_res WHERE pnl_pers IS NOT NULL
        UNION ALL
        SELECT uuid, pnl_pers FROM Bot_2_res WHERE pnl_pers IS NOT NULL
        UNION ALL
        SELECT uuid, pnl_pers FROM Bot_3_res WHERE pnl_pers IS NOT NULL
        UNION ALL
        SELECT uuid, pnl_pers FROM Bot_4_res WHERE pnl_pers IS NOT NULL
    """
    trades_df = pd.read_sql(query, connection)

    def calc_stats(group):
        num_trades = group['pnl_pers'].count()
        wins = (group['pnl_pers'] > 0).sum()
        losses = (group['pnl_pers'] < 0).sum()
        win_rate = wins / num_trades if num_trades > 0 else 0
        avg_win = group[group['pnl_pers'] > 0]['pnl_pers'].mean() if wins > 0 else 0
        avg_loss = -group[group['pnl_pers'] < 0]['pnl_pers'].mean() if losses > 0 else 0
        rew_risk = (avg_win / avg_loss) if avg_loss > 0 else 0
        total_pnl = group['pnl_pers'].sum()
        return pd.Series({
            'Win Rate': win_rate,
            'Rew_Risk': rew_risk,
            'Num trades': num_trades,
            'Total pnl': total_pnl
        })

    stats_df = trades_df.groupby('uuid').apply(calc_stats).reset_index()

    cursor = connection.cursor()
    for _, row in stats_df.iterrows():
        cursor.execute("""
            UPDATE `Z-Scores`
            SET `Win Rate`=%s, `Rew_Risk`=%s, `Num trades`=%s, `Total pnl`=%s
            WHERE uuid=%s
        """, (row['Win Rate'], row['Rew_Risk'], int(row['Num trades']), row['Total pnl'], row['uuid']))
    connection.commit()
    cursor.close()
    connection.close()
    print("Trade statistics (percent-based) updated in Z-Scores.")

def update_expectations():
    connection = connect_to_db()
    if connection is None or not connection.is_connected():
        print("DB connection failed.")
        return

    cursor = connection.cursor()
    cursor.execute("SELECT uuid, `Win Rate`, `Rew_Risk` FROM `Z-Scores`")
    rows = cursor.fetchall()

    for row in rows:
        uuid, win_rate_raw, rew_risk_raw = row
        try:
            win_rate = float(str(win_rate_raw).replace('%', '').strip()) / 100 if '%' in str(win_rate_raw) else float(win_rate_raw)
            rew_risk = float(rew_risk_raw) if rew_risk_raw is not None else 0.0
            expect_val = (win_rate * rew_risk) - (1 - win_rate)
            expect = int(expect_val > 0)
            cursor.execute("UPDATE `Z-Scores` SET `Expect`=%s WHERE uuid=%s", (expect, uuid))
        except Exception as e:
            print(f"Error updating Expect for {uuid}: {e}")
    connection.commit()
    cursor.close()
    connection.close()
    print("Expectations updated in Z-Scores.")

# === Level Assignment ===


def update_levels():
    from datetime import datetime
    import pandas as pd

    today = datetime.utcnow().date()

    cn = None
    cur = None
    try:
        cn = connect_to_db()
        if cn is None:
            print("DB connect failed in update_levels()")
            return
        cur = cn.cursor()

        # Pull everything we need in one go (including current level & quarantine)
        df = pd.read_sql(
            """
            SELECT
                uuid,
                `Expect`,
                `Coint`,
                `Num trades`        AS num_trades,
                `Level_30`          AS level_30,
                `Level`             AS current_level,
                `Quarantine_Until`  AS quarantine_until
            FROM `Z-Scores`
            """,
            cn
        )

        def decide_level(row):
            # Respect quarantine: if still active, keep current level unchanged.
            q_until = row.get("quarantine_until")
            if pd.notnull(q_until):
                try:
                    q_date = pd.to_datetime(q_until).date()
                    if q_date > today:
                        return row.get("current_level")  # no change while quarantined
                except Exception:
                    # If parsing fails, fall through to normal rules
                    pass

            expect     = int(row['Expect'])      if pd.notnull(row['Expect'])      else 0
            coint      = int(row['Coint'])       if pd.notnull(row['Coint'])       else 0
            num_trades = int(row['num_trades'])  if pd.notnull(row['num_trades'])  else 0
            level30    = (row['level_30'] or "").strip().upper() if pd.notnull(row['level_30']) else ""

            if (expect == 1) and (coint == 1) and (num_trades > 20) and (level30 in ("L1", "L2")):
                return "Level_2"
            elif (expect == 0) and (coint == 0):
                return "Level_0"
            else:
                return "Level_1"

        df["new_level"] = df.apply(decide_level, axis=1)

        # Update only where it actually changed
        changes = df[df["new_level"] != df["current_level"]][["uuid", "new_level"]]
        if changes.empty:
            print("update_levels(): nothing to update.")
            return

        sql = "UPDATE `Z-Scores` SET `Level` = %s WHERE uuid = %s"
        params = [(row.new_level, row.uuid) for row in changes.itertuples(index=False)]

        cur.executemany(sql, params)
        cn.commit()
        print(f"update_levels(): updated {cur.rowcount} rows.")

    except Exception as e:
        print(f"Error in update_levels(): {e}")
        try:
            if cn:
                cn.rollback()
        except Exception:
            pass
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if cn and cn.is_connected():
                cn.close()
        except Exception:
            pass


def update_trade_statistics_last30(result_table: str = "Bot_4_res"):
    """
    Update Z-Scores with 'last 60 days, max 30 trades' stats from `result_table`.

    Uses pnl_pers (percentage PnL):
      - Win Rate_30   = wins / n
      - Rew_Risk_30   = avg_win / |avg_loss|   (0 if no losses)
      - Total pnl_30  = sum(pnl_pers)
      - Expect_30     = int( (win_rate * rew_risk) - (1 - win_rate) > 0 )
      - Level_30:
            if n < 5 -> 'NA'
            elif win_rate >= 0.55 and rew_risk >= 1.1 and Expect_30 == 1 and n >= 15 -> 'L2'
            elif win_rate >= 0.50 and Expect_30 == 1 and n >= 5 -> 'L1'
            else -> 'L0'
      - Trades_30     = n (trades used within 60 days, up to 30)
    """
    cn = None
    cur = None
    try:
        cn = connect_to_db()
        if cn is None:
            print("DB connect failed in update_trade_statistics_last30()")
            return
        cur = cn.cursor(dictionary=True)

        # universe of uuids to update
        cur.execute("SELECT uuid FROM `Z-Scores`")
        uuids = [r["uuid"] for r in cur.fetchall() if r["uuid"]]

        # 60-day window (server is UTC; DATE is fine)
        today = datetime.utcnow().date()
        start_date = today - timedelta(days=59)

        fetch_last30 = f"""
            SELECT pnl_pers
            FROM `{result_table}`
            WHERE uuid = %s
              AND close_date IS NOT NULL
              AND close_date BETWEEN %s AND %s
              AND Bot_num != 'Bot_V_2'
              AND Bot_num != 'Bot_D_1'
              AND Bot_num != 'Bot_CFT_2'
            ORDER BY close_date DESC, close_time DESC
            LIMIT 30
        """
        upd_sql = """
            UPDATE `Z-Scores`
            SET `Win Rate_30` = %s,
                `Rew_Risk_30` = %s,
                `Total pnl_30` = %s,
                `Expect_30`    = %s,
                `Level_30`     = %s,
                `Trades_30`    = %s
            WHERE uuid = %s
        """

        for u in uuids:
            cur.execute(fetch_last30, (u, start_date, today))
            rows = cur.fetchall()
            pnls = [float(r["pnl_pers"]) for r in rows if r["pnl_pers"] is not None]
            n = len(pnls)

            if n == 0:
                win_rate = 0.0
                rew_risk = 0.0
                total_pnl = 0.0
                expect = 0
                lvl = "NA"
            else:
                wins = [p for p in pnls if p > 0]
                losses = [p for p in pnls if p < 0]
                win_rate = len(wins) / n
                avg_win = (sum(wins) / len(wins)) if wins else 0.0
                avg_loss = (sum(losses) / len(losses)) if losses else 0.0  # negative or 0
                total_pnl = sum(pnls)
                rew_risk = (avg_win / abs(avg_loss)) if (losses and abs(avg_loss) > 1e-12) else 0.0

                expect_val = (win_rate * rew_risk) - (1.0 - win_rate)
                expect = 1 if expect_val > 0 else 0

                # level rules using n (count over the 60-day/30-trade window)
                if n < 5:
                    lvl = "NA"
                elif (win_rate >= 0.55) and (rew_risk >= 1.1) and (expect == 1) and (n >= 15):
                    lvl = "L2"
                elif (win_rate >= 0.50) and (expect == 1) and (n >= 5):
                    lvl = "L1"
                else:
                    lvl = "L0"

            cur.execute(
                upd_sql,
                (float(win_rate), float(rew_risk), float(total_pnl), int(expect), lvl, int(n), u)
            )

        cn.commit()
        print("update_trade_statistics_last30(): DONE (60-day window, max 30 trades)")

    except Exception as e:
        print(f"Error in update_trade_statistics_last30(): {e}")
        try:
            if cn:
                cn.rollback()
        except Exception:
            pass
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if cn and cn.is_connected():
                cn.close()
        except Exception:
            pass


def update_pnl_pers_all_tables():
    connection = connect_to_db()
    if connection is None or not connection.is_connected():
        print("DB connection failed.")
        return

    bot_tables = ["Bot_1_res", "Bot_2_res", "Bot_3_res", "Bot_4_res"]
    cursor = connection.cursor()
    for table in bot_tables:
        # Select rows where pnl_pers is NULL and pos_val is not null and not zero
        cursor.execute(f"SELECT ID, pnl, pos_val FROM {table} WHERE pnl_pers IS NULL AND pos_val IS NOT NULL AND pos_val != 0")
        rows = cursor.fetchall()
        count = 0
        for row in rows:
            trade_id, pnl, pos_val = row
            if pnl is None or pos_val is None or pos_val == 0:
                continue  # Skip problematic rows!
            pnl_pers = pnl / pos_val
            cursor.execute(f"UPDATE {table} SET pnl_pers=%s WHERE ID=%s", (pnl_pers, trade_id))
            count += 1
        if count > 0:
            print(f"Updated {count} rows in {table} with pnl_pers.")
    connection.commit()
    cursor.close()
    connection.close()



def remove_losing_pairs_from_db_and_csv(trade_limit=25):
    connection = connect_to_db()
    if connection is None or not connection.is_connected():
        print("DB connection failed.")
        return

    # Use percent-based statistics for filtering
    df = pd.read_sql(
        f"SELECT uuid, asset_1, asset_2 FROM `Z-Scores` WHERE `Total pnl`<0 AND `Expect`=0 AND `Num trades`>{trade_limit}",
        connection
    )
    if df.empty:
        print("No losing pairs to remove.")
        connection.close()
        return

    # Remove from Z-Scores
    cursor = connection.cursor()
    for _, row in df.iterrows():
        uuid = row['uuid']
        cursor.execute("DELETE FROM `Z-Scores` WHERE uuid=%s", (uuid,))
        print(f"Removed {uuid} from Z-Scores DB.")
    connection.commit()
    cursor.close()
    connection.close()

    # Remove from CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "list1.csv")
    try:
        csv_df = pd.read_csv(file_path, header=None)
        def is_pair(row, asset1, asset2):
            parts = row[0].split(',')
            a, b = parts[0].strip(), parts[1].strip()
            return (a == asset1 and b == asset2) or (a == asset2 and b == asset1)
        for _, row in df.iterrows():
            asset1, asset2 = row['asset_1'], row['asset_2']
            csv_df = csv_df[~csv_df.apply(is_pair, axis=1, asset1=asset1, asset2=asset2)]
            print(f"Removed {asset1}-{asset2} from CSV.")
        csv_df.to_csv(file_path, header=False, index=False)
    except Exception as e:
        print(f"Error cleaning up CSV: {e}")

def apply_quarantine_level(result_tables=("Bot_1_res","Bot_2_res","Bot_3_res","Bot_4_res"),
    last_n_days=5,
    required_losing_days=5,
    quarantine_label="Level_Q",
    quarantine_days=14):
    cn = None; cur = None
    try:
        cn = connect_to_db()
        if cn is None:
            print("DB connect failed in apply_quarantine_level()"); return
        cur = cn.cursor()

        unions = []
        for t in result_tables:
            unions.append(
                f"SELECT uuid, close_date, pnl_pers FROM `{t}` "
                f"WHERE close_date IS NOT NULL AND pnl_pers IS NOT NULL"
            )
        all_trades_sql = " \nUNION ALL\n".join(unions)

        quarantine_sql = f"""
            WITH all_trades AS (
                {all_trades_sql}
            ),
            daily AS (
                SELECT uuid, close_date, SUM(pnl_pers) AS day_pnl
                FROM all_trades
                GROUP BY uuid, close_date
            ),
            ranked AS (
                SELECT
                    uuid,
                    close_date,
                    day_pnl,
                    ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY close_date DESC) AS rn
                FROM daily
            ),
            lastN AS (
                SELECT * FROM ranked WHERE rn <= %s
            ),
            agg AS (
                SELECT
                    uuid,
                    COUNT(*) AS days,
                    SUM(CASE WHEN day_pnl < 0 THEN 1 ELSE 0 END) AS losing_days
                FROM lastN
                GROUP BY uuid
            )
            SELECT uuid
            FROM agg
            WHERE days >= %s
              AND losing_days >= %s
        """
        cur.execute(quarantine_sql, (last_n_days, last_n_days, required_losing_days))
        uuids = [row[0] for row in cur.fetchall() if row and row[0]]
        if not uuids:
            print("apply_quarantine_level(): no uuids to quarantine."); return

        # Set Level='Level_Q' and an expiry if not already active
        # (keep existing future Quarantine_Until; set only if NULL or expired)
        upd = f"""
            UPDATE `Z-Scores`
            SET `Level` = %s,
                `Quarantine_Reason` = %s,
                `Quarantine_Until` = CASE
                    WHEN `Quarantine_Until` IS NULL OR `Quarantine_Until` <= CURDATE()
                    THEN CURDATE() + INTERVAL {int(quarantine_days)} DAY
                    ELSE `Quarantine_Until`
                END
            WHERE uuid = %s
        """
        data = [(quarantine_label, f"Last {last_n_days} days losing", u) for u in uuids]
        cur.executemany(upd, data)
        cn.commit()
        print(f"apply_quarantine_level(): quarantined {cur.rowcount} pairs as {quarantine_label}.")
    except Exception as e:
        print(f"Error in apply_quarantine_level(): {e}")
        try:
            if cn: cn.rollback()
        except: pass
    finally:
        try:
            if cur: cur.close()
        except: pass
        try:
            if cn and cn.is_connected(): cn.close()
        except: pass



def release_expired_quarantine():
    """
    When Quarantine_Until <= today, remove quarantine.
    Map back to Level_30: 'L2' -> 'Level_2', 'L1' -> 'Level_1', else 'Level_0'.
    """
    cn = None; cur = None
    try:
        cn = connect_to_db()
        if cn is None:
            print("DB connect failed in release_expired_quarantine()"); return
        cur = cn.cursor()
        sql = """
            UPDATE `Z-Scores`
            SET
              `Level` = CASE
                  WHEN `Level_30`='L2' THEN 'Level_2'
                  WHEN `Level_30`='L1' THEN 'Level_1'
                  ELSE 'Level_0'
              END,
              `Quarantine_Until` = NULL,
              `Quarantine_Reason` = NULL
            WHERE `Level` = 'Level_Q'
              AND `Quarantine_Until` IS NOT NULL
              AND `Quarantine_Until` <= CURDATE()
        """
        cur.execute(sql)
        cn.commit()
        print(f"release_expired_quarantine(): released {cur.rowcount} pairs.")
    except Exception as e:
        print(f"Error in release_expired_quarantine(): {e}")
        try:
            if cn: cn.rollback()
        except: pass
    finally:
        try:
            if cur: cur.close()
        except: pass
        try:
            if cn and cn.is_connected(): cn.close()
        except: pass


# === Main update loop ===

def statistical_update_loop():
    cycle_time = 300  # 5 minutes

    while True:
        start_time = time.time()
        try:
            update_pnl_pers_all_tables()
            update_trade_statistics()
            update_expectations()
            update_trade_statistics_last30()
            update_levels()
            update_pnl_pers_all_tables()
            update_trade_statistics()
            update_expectations()
            update_trade_statistics_last30()
            update_levels()  # classic
            apply_quarantine_level(  # override + set Quarantine_Until
                result_tables=("Bot_1_res", "Bot_2_res", "Bot_3_res", "Bot_4_res"),
                last_n_days=5,
                required_losing_days=5,
                quarantine_label="Level_Q",
                quarantine_days=14
            )
            release_expired_quarantine()  # free any that reached the date

            remove_losing_pairs_from_db_and_csv(trade_limit=30)
        except Exception as e:
            print(f"{datetime.now()} - ERROR in statistical update: {e}")
        elapsed = time.time() - start_time
        elapsed = round(elapsed)
        sleep_time = max(0, cycle_time - elapsed)
        print(f"Stat update cycle finished in {elapsed:.1f} seconds. Sleeping for {sleep_time:.1f} seconds.")
        print(f"Current time: {datetime.now()}")
        if sleep_time > 0:
            time.sleep(sleep_time)

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
        await statistical_update_loop()

    asyncio.run(_starter())

