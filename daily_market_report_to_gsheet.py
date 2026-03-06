# -*- coding: utf-8 -*-
"""
Daily Market Report -> Google Sheets
- Update fixed cells in a Google Sheet tab (keeps layout & formulas).
- TWII turnover: TWSE FMTQIK JSON (成交金額(元)) -> 億元
- HK turnover: HKEX Day Quotations (market turnover) -> 億港幣 (fallback AASTOCKS)
- TW stocks OHLCV: Prefer TWSE MI_INDEX (ALLBUT0999) for stability on GitHub Actions.
  If not found (e.g., TPEx/ESB), fallback to TPEx st43_result.php, then yfinance last resort.
- HK stocks/indices: yfinance.
- NEW: Revenue tab "營收" -> fill current month / YoY base / MoM base revenues from MOPSFIN CSV.

Env required (GitHub Actions secrets/env):
  GSHEET_ID, GSHEET_TAB, GCP_SA_JSON
Optional:
  GSHEET_TAB_REVENUE=營收
  DEBUG_HKEX=1 (saves debug html)
"""

import os
import json
import math
import re
from pathlib import Path
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

from market_report.hk_market import hk_hands_from_aastocks, hk_turnover_scan_prev, hk_turnover_two_days
from market_report.quote_updates import build_hk_stock_updates, build_tw_stock_updates, fetch_hk_stock_map
from market_report.revenue import update_revenue_tab
from market_report.sheet_exports import get_sheet_properties, hide_column_a
from market_report.sheet_layout import find_stock_rows_from_sheet
from market_report.tw_market import tw_price_pack_for_codes, twse_turnover_yi


# ========= Basic =========
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
DEBUG_HKEX = os.getenv("DEBUG_HKEX", "") == "1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEBUG_DIR = os.path.join(BASE_DIR, "debug")

def _debug_save(name: str, text: str):
    if not DEBUG_HKEX:
        return
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        Path(os.path.join(DEBUG_DIR, name)).write_text(text or "", encoding="utf-8", errors="ignore")
    except Exception:
        pass


# ========= Google Sheets =========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def gsheet_service():
    """
    Build Google Sheets API service using Application Default Credentials (ADC).
    Works with GitHub Actions WIF (google-github-actions/auth@v2) without JSON key.
    """
    import os
    import google.auth
    from googleapiclient.discovery import build

    sheet_id = os.getenv("GSHEET_ID")
    tab = os.getenv("GSHEET_TAB")

    if not sheet_id or not tab:
        raise RuntimeError("缺少 GSHEET_ID / GSHEET_TAB（請在 GitHub Secrets/Env 設定）")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds, _ = google.auth.default(scopes=scopes)

    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return svc, sheet_id, tab


def batch_update_values(svc, sheet_id: str, updates: list[tuple[str, list[list]]], value_input="USER_ENTERED"):
    body = {
        "valueInputOption": value_input,
        "data": [{"range": rng, "values": vals} for rng, vals in updates],
    }
    svc.spreadsheets().values().batchUpdate(spreadsheetId=sheet_id, body=body).execute()


def get_values(svc, sheet_id: str, rng: str) -> list[list]:
    res = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=rng).execute()
    return res.get("values", [])


# ========= Utils =========
def _to_float(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("\u00a0", " ")
    if s in ("", "--", "—", "-"):
        return None
    m = re.search(r"-?([0-9]+(?:\.[0-9]+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _round2(x):
    if x is None:
        return None
    try:
        return round(float(x), 2)
    except Exception:
        return None



def _round3(x):
    try:
        return round(float(x), 3)
    except Exception:
        return None
def _today_taipei() -> datetime:
    # GitHub Actions default is UTC; if you need strict Asia/Taipei, convert with zoneinfo.
    return datetime.now()

def _parse_sheet_datetime(date_value, time_value=None) -> datetime | None:
    """解析 L3/M3 的更新時間；相容舊格式 L3 單格日期時間。"""
    if date_value is None:
        return None
    date_text = str(date_value).strip()
    time_text = str(time_value).strip() if time_value is not None else ""
    if not date_text:
        return None

    candidates = []
    if time_text:
        candidates.extend(
            [
                f"{date_text} {time_text}",
                f"{date_text} {time_text}:00" if len(time_text) == 5 else f"{date_text} {time_text}",
            ]
        )
    candidates.append(date_text)

    for text in candidates:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                pass
    return None

def should_skip_today_by_l3(svc, sheet_id: str, tab_name: str) -> bool:
    """
    True: 今天已更新過 -> 直接跳過
    可用 env FORCE_RUN=1 強制不跳過（留給手動排查用）
    """
    if os.getenv("FORCE_RUN", "0").strip() == "1":
        print("[DEDUP] FORCE_RUN=1 -> do not skip")
        return False

    tab_q = f"'{tab_name}'" if re.search(r"[^A-Za-z0-9_]", tab_name) else tab_name
    values = get_values(svc, sheet_id, f"{tab_q}!L3:M3")
    row = values[0] if values else []
    l3_value = row[0] if len(row) > 0 else None
    m3_value = row[1] if len(row) > 1 else None
    last_dt = _parse_sheet_datetime(l3_value, m3_value)

    if last_dt is None:
        print("[DEDUP] L3/M3 empty or unparseable -> do not skip")
        return False

    today = datetime.now().date()  # 你 workflow 已設 TZ=Asia/Taipei
    if last_dt.date() == today:
        print(f"[DEDUP] Already updated today at {last_dt} -> skip")
        return True

    print(f"[DEDUP] Last update {last_dt} not today -> do not skip")
    return False


def _is_blank_cell(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s in ("", "--", "—", "-")

def _is_first_run_from_range(vals: list[list]) -> bool:
    # If user cleared D/E/H/I/J/K, this block should be mostly empty.
    if not vals:
        return True
    for row in vals:
        for v in row:
            if not _is_blank_cell(v):
                return False
    return True


# ========= yfinance helpers =========
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def hist_one(ticker: str) -> pd.DataFrame:
    return yf.Ticker(ticker).history(period="1mo", interval="1d", auto_adjust=False)

def last_two(series: pd.Series):
    s = series.dropna()
    if len(s) < 2:
        return (pd.NaT, math.nan, pd.NaT, math.nan)
    return s.index[-1], float(s.iloc[-1]), s.index[-2], float(s.iloc[-2])


# ========= Sessions =========
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})


# ========= Main =========
TICKER_TWII = "^TWII"
TICKER_HSI  = "^HSI"

def main():
    svc, sheet_id, tab = gsheet_service()
  
    # ---- Dedup: skip if already updated today ----
    if should_skip_today_by_l3(svc, sheet_id, tab):
        # 告訴 workflow 這次是 skip（避免寄信）
        Path("skipped.txt").write_text("1", encoding="utf-8")
        print("[DEDUP] skipped.txt written. Exit 0.")
        return
    tab_q = f"'{tab}'" if re.search(r"[^A-Za-z0-9_]", tab) else tab
    sheet_props = get_sheet_properties(svc, sheet_id, tab)

    # Detect "first run" (user cleared D/E/H/I/J/K)
    first_block = get_values(svc, sheet_id, f"{tab_q}!D3:K60")
    first_run = _is_first_run_from_range(first_block)

    # Keep old HK turnover for right-shift (only if NOT first run)
    old_hk_today = None
    if not first_run:
        v = get_values(svc, sheet_id, f"{tab_q}!H8:H8")
        if v and v[0]:
            old_hk_today = _to_float(v[0][0])

    ab = get_values(svc, sheet_id, f"{tab_q}!A1:B260")
    col_a = [row[0] if len(row) > 0 else "" for row in ab]
    col_b = [row[1] if len(row) > 1 else "" for row in ab]

    tw_rows, hk_rows = find_stock_rows_from_sheet(col_a, col_b)

    idx_map = {}
    for tkr in (TICKER_TWII, TICKER_HSI):
        try:
            h = hist_one(tkr)
        except Exception:
            h = pd.DataFrame()
        if h is None or h.empty or "Close" not in h.columns or h["Close"].dropna().shape[0] < 2:
            idx_map[tkr] = {}
            continue
        t_date, t_close, p_date, p_close = last_two(h["Close"])
        idx_map[tkr] = {"t_date": t_date, "p_date": p_date, "close": t_close, "prev_close": p_close}

    twii = idx_map.get(TICKER_TWII, {})
    hsi  = idx_map.get(TICKER_HSI, {})

    tw_t = twii.get("t_date")
    tw_p = twii.get("p_date")
    if not isinstance(tw_t, pd.Timestamp) or not isinstance(tw_p, pd.Timestamp):
        now = _today_taipei()
        tw_t = pd.Timestamp(now.date())
        tw_p = pd.Timestamp((now - timedelta(days=1)).date())
    tw_t_dt = tw_t.to_pydatetime()
    tw_p_dt = tw_p.to_pydatetime()

    # turnovers
    tw_today_yi = twse_turnover_yi(_SESSION, tw_t_dt)
    tw_prev_yi  = twse_turnover_yi(_SESSION, tw_p_dt)

    hk_today_yi, hk_prev_yi = hk_turnover_two_days(
        hsi.get("t_date"),
        hsi.get("p_date"),
        session=_SESSION,
        user_agent=UA,
        debug_save=_debug_save,
        to_float=_to_float,
    )

    # If HK prev is missing:
    if hk_prev_yi is None:
        if (not first_run) and (old_hk_today is not None):
            # right-shift from last run
            hk_prev_yi = _round2(old_hk_today)
        else:
            # first run => scan back to find a valid previous trading day
            base_dt = (hsi.get("t_date").to_pydatetime() if isinstance(hsi.get("t_date"), pd.Timestamp) else _today_taipei())
            hk_prev_yi = hk_turnover_scan_prev(
                base_dt,
                max_back_days=10,
                session=_SESSION,
                user_agent=UA,
                debug_save=_debug_save,
                to_float=_to_float,
            )

    # TW stocks
    tw_codes = [code for _, code in tw_rows]
    tw_today_map, tw_prev_map = tw_price_pack_for_codes(
        tw_codes,
        tw_t_dt,
        tw_p_dt,
        session=_SESSION,
        hist_one=hist_one,
        last_two=last_two,
        to_float=_to_float,
    )

    # HK stocks via yfinance
    hk_codes = [code for _, code in hk_rows]
    hk_tickers = [f"{int(c):04d}.HK" for c in hk_codes]
    hk_stock_map = fetch_hk_stock_map(hk_tickers, hist_one=hist_one, last_two=last_two)

    # build updates
    updates = []
    now = _today_taipei()
    updates.append((f"{tab_q}!L3", [[now.strftime("%Y-%m-%d")]]))
    updates.append((f"{tab_q}!M3", [[now.strftime("%H:%M:%S")]]))

    updates.append((f"{tab_q}!D6", [[_round2(twii.get("close"))]]))
    updates.append((f"{tab_q}!E6", [[_round2(twii.get("prev_close"))]]))
    updates.append((f"{tab_q}!H6", [[tw_today_yi]]))
    updates.append((f"{tab_q}!I6", [[tw_prev_yi]]))

    updates.append((f"{tab_q}!D8", [[_round2(hsi.get("close"))]]))
    updates.append((f"{tab_q}!E8", [[_round2(hsi.get("prev_close"))]]))
    updates.append((f"{tab_q}!H8", [[hk_today_yi]]))
    updates.append((f"{tab_q}!I8", [[hk_prev_yi]]))

    updates.extend(
        build_tw_stock_updates(
            tab_q,
            tw_rows,
            tw_today_map,
            tw_prev_map,
            round_price=_round2,
        )
    )
    updates.extend(
        build_hk_stock_updates(
            tab_q,
            hk_rows,
            hk_tickers,
            hk_stock_map,
            round_price=_round3,
            hk_hands_from_aastocks=lambda code: hk_hands_from_aastocks(code, user_agent=UA),
        )
    )


    batch_update_values(svc, sheet_id, updates, value_input="USER_ENTERED")
    hide_column_a(svc, sheet_id, sheet_props["sheetId"])

    # NEW: update Revenue tab
    update_revenue_tab(
        svc,
        sheet_id,
        get_values=get_values,
        batch_update_values=batch_update_values,
        today_taipei=_today_taipei,
        session=_SESSION,
        user_agent=UA,
        to_float=_to_float,
    )

    print("DONE: updated Google Sheet")
    print(f"TW rows: {len(tw_rows)} | HK rows: {len(hk_rows)}")
    print(f"TW dates: {tw_t_dt.date()} / {tw_p_dt.date()}")
    print(f"TWII turnover (today/prev, 億元): {tw_today_yi} / {tw_prev_yi}")
    print(f"HK turnover (today/prev, 億港幣): {hk_today_yi} / {hk_prev_yi}")
    print(f"first_run={first_run}")


if __name__ == "__main__":
    main()
