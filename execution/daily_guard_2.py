# daily_guard_2.py
import logging
from datetime import datetime
from typing import Optional, TypedDict



# ----- Provided by your bot at runtime -----
# connect_to_db, BOT_NUM, RESULT_TABLE are passed in as call args

class DayState(TypedDict, total=False):
    date: Optional[str]
    equity_start: Optional[float]
    cum_real_start: Optional[float]
    # ↓ add the throttle-related keys so type checkers are happy
    tp_stage: int
    tp_throttled: bool
    profit_lock_current: Optional[float]

# Initialize all fields here (no later “DAY[...]= ...” adds needed)
DAY: DayState = {
    "date": None,
    "equity_start": None,
    "cum_real_start": None,
    "tp_stage": 0,
    "tp_throttled": False,
    "profit_lock_current": None,
}


# ---------------- DB helpers (global lock flag) ----------------
def is_bot_blocked_today(connect_to_db, BOT_NUM):
    conn = connect_to_db()
    if conn is None:
        return False, None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT is_blocked, reason
            FROM Bot_Locks
            WHERE bot=%s AND lock_date=CURDATE()
        """, (BOT_NUM,))
        row = cur.fetchone()
        return (bool(row[0]), row[1]) if row else (False, None)
    finally:
        try:
            if conn.is_connected(): conn.close()
        except:  # noqa
            pass

def set_bot_blocked_today(connect_to_db, BOT_NUM, is_blocked: bool, reason: Optional[str]):
    conn = connect_to_db()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO Bot_Locks (bot, lock_date, is_blocked, reason)
            VALUES (%s, CURDATE(), %s, %s)
            ON DUPLICATE KEY UPDATE is_blocked=VALUES(is_blocked), reason=VALUES(reason)
        """, (BOT_NUM, int(is_blocked), reason))
        conn.commit()
    finally:
        try:
            if conn.is_connected(): conn.close()
        except:  # noqa
            pass

# --------------- Balance field extractors ----------------
def _get_balance(bybit):
    return bybit.fetch_balance()

def _get_usdt_coin(balance_info):
    try:
        return next(c for c in balance_info["info"]["result"]["list"][0]["coin"] if c.get("coin") == "USDT")
    except Exception:
        return {}

def _extract_total_perp_upl(balance_info) -> float:
    try:
        return float(balance_info["info"]["result"]["list"][0].get("totalPerpUPL") or 0.0)
    except Exception:
        return 0.0

def _extract_total_equity_usdt(balance_info) -> float:
    try:
        return float(balance_info.get("total", {}).get("USDT") or 0.0)
    except Exception:
        return 0.0

# --------------- Day snapshot & live PnL ----------------
def ensure_day_snapshot(bybit, connect_to_db, BOT_NUM):
    today = datetime.now().strftime("%Y-%m-%d")
    if DAY.get("date") == today and DAY.get("equity_start") is not None:
        return
    bal = _get_balance(bybit)
    DAY["date"] = today
    DAY["equity_start"] = _extract_total_equity_usdt(bal)
    usdt_coin = _get_usdt_coin(bal)
    DAY["cum_real_start"] = float(usdt_coin.get("cumRealisedPnl", 0.0) or 0.0)
    # new day → reset throttle & stages
    DAY["tp_stage"] = 0
    DAY["tp_throttled"] = False
    DAY["profit_lock_current"] = None
    set_bot_blocked_today(connect_to_db, BOT_NUM, False, None)
    logging.info(
        f"[Daily] Snapshot: equity_start={DAY['equity_start']:.4f}, "
        f"cum_real_start={DAY['cum_real_start']:.4f}, stage={DAY['tp_stage']}, throttled={DAY['tp_throttled']}"
    )

def pnl_now(bybit):
    """
    Returns dict with:
      equity_start_usdt, realized_today_usdt, unreal_now_usdt, pnl_total_usdt, pnl_pct_now
    realized_today = USDT cumRealisedPnl delta (now - day_start)
    unreal_now = totalPerpUPL
    """
    bal = _get_balance(bybit)
    eq_start = float(DAY.get("equity_start") or 0.0)
    cum_real_start = float(DAY.get("cum_real_start") or 0.0)

    usdt_coin = _get_usdt_coin(bal)
    cum_real_now = float(usdt_coin.get("cumRealisedPnl", 0.0) or 0.0)
    realized_today = cum_real_now - cum_real_start

    unreal_now = _extract_total_perp_upl(bal)

    pnl_total = realized_today + unreal_now
    pnl_pct_now = (pnl_total / eq_start * 100.0) if eq_start > 0 else 0.0

    return {
        "equity_start_usdt": eq_start,
        "realized_today_usdt": realized_today,
        "unreal_now_usdt": unreal_now,
        "pnl_total_usdt": pnl_total,
        "pnl_pct_now": pnl_pct_now,
    }

# --------------- The guard (signal-only + throttle) ----------------
def daily_guard_eval_and_act(bybit, connect_to_db, BOT_NUM, RESULT_TABLE,
                             profit_lock_pct: float = 1.0,
                             loss_stop_pct: float = 3.0):
    """
    Signal-only guard with TP throttle stages:
      - If PnL% >= current TP target → set throttled=True and advance TP target (×2). Action='profit_lock_throttle'.
      - If PnL% <= -loss_stop_pct     → action='loss_stop_signal' + set blocked for today.
      - If already blocked today      → action='loss_blocked'.
      - Else                          → action='continue'.
    Returns state fields: throttled (bool), tp_stage (int), tp_next (float).
    """
    ensure_day_snapshot(bybit, connect_to_db, BOT_NUM)

    blocked, _ = is_bot_blocked_today(connect_to_db, BOT_NUM)
    snap = pnl_now(bybit)
    p = snap["pnl_pct_now"]

    # compute current and next TP thresholds
    tp_stage = int(DAY.get("tp_stage", 0))
    tp_current = DAY.get("profit_lock_current")
    if tp_current is None:
        tp_current = profit_lock_pct * (2 ** tp_stage)
        DAY["profit_lock_current"] = tp_current
    tp_next = profit_lock_pct * (2 ** (tp_stage + 1))

    if blocked:
        return {**snap, "ok": False, "blocked": True, "action": "loss_blocked",
                "throttled": bool(DAY.get("tp_throttled", False)),
                "tp_stage": tp_stage, "tp_next": tp_current}  # while blocked, current target is what matters

    # Loss stop → block today
    if p <= -loss_stop_pct:
        reason = f"Daily SL reached -{loss_stop_pct:.2f}%"
        logging.warning(f"[Daily] {reason} → set block for today.")
        set_bot_blocked_today(connect_to_db, BOT_NUM, True, reason)
        return {**snap, "ok": False, "blocked": True, "action": "loss_stop_signal",
                "throttled": bool(DAY.get("tp_throttled", False)),
                "tp_stage": tp_stage, "tp_next": tp_current}

    # TP throttle: if we reach the current TP target, enable throttle and move target ×2
    if p >= tp_current:
        if not DAY.get("tp_throttled", False):
            logging.warning(f"[Daily] TP {tp_current:.2f}% reached → enable throttle, next target {tp_next:.2f}%.")
        else:
            logging.info(f"[Daily] TP stage {tp_stage} reaffirmed at {p:.2f}% (already throttled). Next {tp_next:.2f}%.")

        DAY["tp_throttled"] = True
        DAY["tp_stage"] = tp_stage + 1
        DAY["profit_lock_current"] = tp_next  # advance target

        return {**snap, "ok": True, "blocked": False, "action": "profit_lock_throttle",
                "throttled": True, "tp_stage": DAY["tp_stage"], "tp_next": DAY["profit_lock_current"]}

    # otherwise continue, report current throttle state and next target
    return {**snap, "ok": True, "blocked": False, "action": "continue",
            "throttled": bool(DAY.get("tp_throttled", False)),
            "tp_stage": tp_stage, "tp_next": tp_current}
