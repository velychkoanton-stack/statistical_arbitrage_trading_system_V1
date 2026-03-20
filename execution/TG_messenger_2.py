# TG_messenger.py
import os
import time
import requests
from typing import Optional

# ---- Config ----
# Prefer environment variables; falls back to hardcoded defaults if present.
TELEGRAM_BOT_TOKEN = "8401495687:AAEjWWFsFpZ8ZAxULWs1lInKhoPSGGBqGIE"
TELEGRAM_CHAT_ID   = "8495254471"

API_URL = "https://api.telegram.org/bot{token}/sendMessage"

def init_tg(token: Optional[str] = None, chat_id: Optional[str] = None):
    """
    Optionally set/override token & chat_id at runtime.
    """
    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    if token:
        TELEGRAM_BOT_TOKEN = token
    if chat_id:
        TELEGRAM_CHAT_ID = chat_id

def send_message(text: str, parse_mode: Optional[str] = None, disable_web_page_preview: bool = True) -> bool:
    """
    Send a plain Telegram message. Returns True on success.
    Minimal retry to be resilient.
    """
    url = API_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.ok:
                return True
            # retry on 5xx or rate limits
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 * attempt, 5))
                continue
            # non-retriable
            return False
        except Exception:
            time.sleep(min(2 * attempt, 5))
    return False

# ---------- Convenience helpers for your bot ----------

def send_open_signal(uuid: str,
                     long_symbol: str, long_amount: float,
                     short_symbol: str, short_amount: float,
                     z_score: float, level: str) -> bool:
    """
    Sends an 'open' signal with both legs and sizing.
    """
    msg = (
        "📣 *OPEN* (pairs trade)\n"
        f"• UUID: `{uuid}`\n"
        f"• Level: `{level}`\n"
        f"• Z-score: {z_score:.3f}\n"
        f"• Long: `{long_symbol}` — amount: {long_amount:.6f}\n"
        f"• Short: `{short_symbol}` — amount: {short_amount:.6f}\n"
    )
    # MarkdownV2 requires heavy escaping; HTML is simpler here.
    return send_message(msg, parse_mode="Markdown")

def send_close_signal(uuid: str,
                      short_symbol: str, short_amount: float,
                      long_symbol: str, long_amount: float,
                      reason: str, level: str) -> bool:
    """
    Sends a 'close' signal with both legs and reason.
    """
    msg = (
        "✅ *CLOSE* (pairs trade)\n"
        f"• UUID: `{uuid}`\n"
        f"• Level: `{level}`\n"
        f"• Reason: {reason}\n"
        f"• Short: `{short_symbol}` — amount: {short_amount:.6f}\n"
        f"• Long: `{long_symbol}` — amount: {long_amount:.6f}\n"
    )
    return send_message(msg, parse_mode="Markdown")
