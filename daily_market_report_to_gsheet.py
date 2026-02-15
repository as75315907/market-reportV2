# -*- coding: utf-8 -*-
"""
Daily Market Report -> Google Sheets (template-layout, no Excel output)

What it does
- Pull prices via yfinance (stocks + indices)
- TWII turnover via TWSE FMTQIK JSON -> 億元
- HSI "market turnover" via HKEX Day Quotations (primary) + AASTOCKS fallback -> 億港幣
- Writes values into fixed cells / fixed columns on your Google Sheet template.

Required GitHub Secrets (or env vars):
- GSHEET_ID: Google Sheet ID
- GSHEET_TAB: worksheet name, e.g. "IR_updated (PC HOME)"
- GCP_SA_JSON: service account JSON (full JSON string)

Optional env:
- TZ=Asia/Taipei
- DEBUG_HKEX=1 : saves raw HTML into ./debug on runner
"""

import os
import json
import math
import time
import re
import subprocess
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import yfinance as yf
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ====== Basic settings ======
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

TICKER_TWII = "^TWII"
TICKER_HSI = "^HSI"

TWSE_FMTQIK = "https://www.twse.com.tw/exchangeReport/FMTQIK"

# HKEX Daily Quotations (eng)
HKEX_DAYQUOT = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{yymmdd}e.htm"
HKEX_DAYQUOT_REFERER = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/qtn.asp"

# AASTOCKS fallback
AASTOCKS_HSI_URL = "https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI&o=0&p=&s=8&t=6"

CACHE_FILE = os.path.join(BASE_DIR, "tw_suffix_cache.json")

DEBUG_HKEX = os.getenv("DEBUG_HKEX", "") == "1"
DEBUG_DIR = os.path.join(BASE_DIR, "debug")


def _debug_save(name: str, text: str):
    if not DEBUG_HKEX:
        return
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        Path(os.path.join(DEBUG_DIR, name)).write_text(text or "", encoding="utf-8", errors="ignore")
    except Exception:
        pass


# ====== small utils ======
def _isnan(x) -> bool:
    try:
        return x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))
    except Exception:
        return True


def _round2(x):
    if _isnan(x):
        return None
    try:
        return round(float(x), 2)
    except Exception:
        return None


def _norm_text(s: str) -> str:
    """Normalize for matching: remove spaces/newlines and unify parentheses."""
    if s is None:
        return ""
    t = str(s)
    t = t.replace("\u3000", " ")
    t = t.replace("（", "(").replace("）", ")")
    t = re.sub(r"\s+", "", t)
    return t


# ====== yfinance helpers ======
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def hist_one(ticker: str) -> pd.DataFrame:
    return yf.Ticker(ticker).history(period="1mo", interval="1d", auto_adjust=False)


def has_enough_prices(hist: pd.DataFrame) -> bool:
    if hist is None or hist.empty:
        return False
    if "Close" not in hist.columns:
        return False
    return hist["Close"].dropna().shape[0] >= 2


def last_two(series: pd.Series):
    s = series.dropna()
    if len(s) < 2:
        return (pd.NaT, math.nan, pd.NaT, math.nan)
    return s.index[-1], float(s.iloc[-1]), s.index[-2], float(s.iloc[-2])


def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache: dict):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def resolve_tw_ticker(code: str, cache: dict) -> str:
    """
    Taiwan stock tickers:
    - XXXX.TW (listed) / XXXX.TWO (OTC)
    We try BOTH and cache the working suffix.
    """
    code = str(code).strip()
    if code in cache:
        return f"{code}.{cache[code]}"

    for suf in ["TW", "TWO"]:
        t = f"{code}.{suf}"
        try:
            h = hist_one(t)
            if has_enough_prices(h):
                cache[code] = suf
                return t
        except Exception:
            continue

    # fallback
    cache[code] = "TW"
    return f"{code}.TW"


def hk_ticker(code: str) -> str:
    return f"{int(str(code)):04d}.HK"


def build_ohlcv_map(ticker_list: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for t in ticker_list:
        try:
            h = hist_one(t)
        except Exception:
            h = pd.DataFrame()

        if h is None or h.empty or "Close" not in h.columns or h["Close"].dropna().shape[0] < 2:
            out[t] = {}
            continue

        t_date, t_close, p_date, p_close = last_two(h["Close"])

        def _last(col):
            if col not in h.columns:
                return math.nan
            s = h[col].dropna()
            return float(s.iloc[-1]) if len(s) else math.nan

        out[t] = {
            "t_date": t_date,
            "p_date": p_date,
            "close": float(t_close),
            "prev_close": float(p_close),
            "open": _last("Open"),
            "high": _last("High"),
            "low": _last("Low"),
            "volume": _last("Volume"),
        }
        time.sleep(0.2)
    return out


# ====== TWSE FMTQIK turnover ======
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})


def _ad_to_twse_date_str(dt: datetime) -> str:
    roc_y = dt.year - 1911
    return f"{roc_y:03d}/{dt.month:02d}/{dt.day:02d}"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_fmtqik_month_json(dt: datetime) -> dict:
    month_key = dt.strftime("%Y%m") + "01"
    params = {"response": "json", "date": month_key}
    r = _SESSION.get(TWSE_FMTQIK, params=params, timeout=30)
    r.raise_for_status()
    obj = r.json()
    stat = str(obj.get("stat", ""))
    if "沒有" in stat or "No data" in stat:
        raise RuntimeError(f"FMTQIK no data for {month_key}")
    return obj


def extract_turnover_from_fmtqik(obj: dict, dt: datetime) -> Optional[int]:
    target = _ad_to_twse_date_str(dt)
    data = obj.get("data", [])
    if not isinstance(data, list):
        return None

    fields = obj.get("fields", [])
    col_idx = None
    if isinstance(fields, list):
        for i, f in enumerate(fields):
            if "成交金額" in str(f):
                col_idx = i
                break

    for row in data:
        if not isinstance(row, list) or not row:
            continue
        if str(row[0]).strip() != target:
            continue

        if col_idx is not None and col_idx < len(row):
            s = str(row[col_idx]).replace(",", "").strip()
            if s.isdigit():
                return int(s)

        for cell in reversed(row):
            s = str(cell).replace(",", "").strip()
            if s.isdigit():
                return int(s)
    return None


def twse_turnover_yi(dt: datetime) -> Optional[float]:
    """Return turnover in 億元 (TWD 1e8)."""
    try:
        obj = fetch_fmtqik_month_json(dt)
        val = extract_turnover_from_fmtqik(obj, dt)
        if val is None:
            return None
        return round(val / 1e8, 2)
    except Exception:
        return None


# ====== HKEX Turnover parsing (ported from your fixhk6 logic) ======
def _curl_get_text(url: str, timeout: int = 30, insecure: bool = False, http1: bool = False,
                   extra_headers: Optional[List[str]] = None) -> str:
    cmd = ["curl", "-L", "-s", "--max-time", str(timeout), "-A", UA]
    if http1:
        cmd.append("--http1.1")
    if extra_headers:
        for h in extra_headers:
            cmd += ["-H", h]
    if insecure:
        cmd.insert(1, "-k")
    cmd.append(url)
    return subprocess.check_output(cmd, text=True, encoding="utf-8", errors="ignore")


def _hkex_yymmdd(dt: datetime) -> str:
    return dt.strftime("%y%m%d")


def fetch_hkex_dayquot_html(trade_dt: datetime) -> str:
    yymmdd = _hkex_yymmdd(trade_dt)
    url = HKEX_DAYQUOT.format(yymmdd=yymmdd)
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
        "Referer": HKEX_DAYQUOT_REFERER,
    }

    # Try requests first
    try:
        r = _SESSION.get(url, timeout=30, headers=headers)
        r.raise_for_status()
        try:
            r.encoding = r.apparent_encoding or r.encoding
        except Exception:
            pass
        html = r.text or ""
        _debug_save(f"hkex_dayquot_{yymmdd}_requests.html", html)
        low = html.lower()
        if ("turnover" not in low) and ("成交" not in html) and ("daily quotations" not in low):
            raise RuntimeError("HKEX content not expected")
        return html
    except Exception as e_req:
        # curl fallback
        curl_headers = [
            f"Referer: {HKEX_DAYQUOT_REFERER}",
            "Accept-Language: en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
        ]
        try:
            html = _curl_get_text(url, timeout=30, insecure=False, http1=True, extra_headers=curl_headers)
            _debug_save(f"hkex_dayquot_{yymmdd}_curl.html", html)
            return html
        except Exception:
            html = _curl_get_text(url, timeout=30, insecure=True, http1=True, extra_headers=curl_headers)
            _debug_save(f"hkex_dayquot_{yymmdd}_curl_insecure.html", html)
            return html


def parse_hkex_turnover_hkd(html: str) -> float:
    """
    Parse HKEX Day Quotations turnover and return HKD amount (NOT yi).
    HKEX pages typically provide values in HK$ Million.
    """
    if not html:
        raise RuntimeError("HKEX HTML empty")

    # Regex fast path
    rx_candidates = [
        r"Total\s+Market\s+Turnover\s*\(\s*HK\$\s*Million\s*\)[^0-9]{0,300}([0-9][0-9,]*\.?[0-9]*)",
        r"Total\s+Market\s+Turnover[^0-9]{0,300}([0-9][0-9,]*\.?[0-9]*)",
        r"市場\s*成交額[^0-9]{0,300}([0-9][0-9,]*\.?[0-9]*)",
        r"成交\s*(?:額|金額)[^0-9]{0,300}([0-9][0-9,]*\.?[0-9]*)",
    ]
    for rx in rx_candidates:
        m = re.search(rx, html, flags=re.I)
        if m:
            v = float(m.group(1).replace(",", ""))
            # Treat as HK$ million if it looks like a normal HKEX magnitude
            # (HKEX "million" values are usually 100,000 ~ 500,000)
            if v >= 10_000:
                return v * 1_000_000.0
            return v

    # Table parse path
    tables = pd.read_html(StringIO(html))
    def _to_float(x):
        s = str(x).strip().replace(",", "").replace("\u00a0", " ")
        m = re.search(r"([0-9][0-9]*\.?[0-9]*)", s)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _unit_multiplier(col_text: str, sample_val: Optional[float]):
        t = (col_text or "").lower()
        if "million" in t or "mn" in t:
            return 1_000_000.0
        if "billion" in t or "bn" in t:
            return 1_000_000_000.0
        if "百萬" in col_text or "百万" in col_text:
            return 1_000_000.0
        if "十億" in col_text or "十亿" in col_text:
            return 1_000_000_000.0
        # Default assumption for HKEX: million
        if sample_val is not None and 10_000 <= sample_val <= 1_000_000:
            return 1_000_000.0
        return 1.0

    for df in tables:
        try:
            df = df.fillna("")
        except Exception:
            pass
        cols = [str(c).strip() for c in getattr(df, "columns", [])]
        if not cols:
            continue

        turnover_cols = []
        for i, c in enumerate(cols):
            cl = c.lower()
            if ("turnover" in cl) or ("成交額" in c) or ("成交金額" in c) or ("成交金额" in c):
                turnover_cols.append(i)
        if not turnover_cols:
            continue

        idx = turnover_cols[0]
        col_name = cols[idx]
        series = df.iloc[:, idx]

        # Prefer Total row if exists
        pick_val = None
        if df.shape[1] >= 2:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            mask = first_col.str.contains(r"^(total|總計|合計)$", case=False, regex=True)
            if mask.any():
                v = _to_float(series[mask].iloc[-1])
                if v is not None:
                    pick_val = v
        if pick_val is None:
            vals = [v for v in (_to_float(x) for x in series.tolist()) if v is not None]
            if vals:
                pick_val = vals[-1]
        if pick_val is None:
            continue

        mult = _unit_multiplier(col_name, pick_val)
        return pick_val * mult

    raise RuntimeError("HKEX turnover not found")


def fetch_aastocks_hsi_html() -> str:
    r = _SESSION.get(
        AASTOCKS_HSI_URL,
        timeout=30,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.aastocks.com/",
        },
    )
    r.raise_for_status()
    try:
        r.encoding = r.apparent_encoding or r.encoding
    except Exception:
        pass
    html = r.text or ""
    _debug_save("aastocks_hsi.html", html)
    return html


def parse_aastocks_turnover_yi(html: str) -> float:
    """
    Return turnover in 億港幣.
    Examples:
      成交額◎ 2,575.78億
      Turnover◎ 257.58B (HKD billions)
    """
    if not html:
        raise RuntimeError("AASTOCKS HTML empty")

    m = re.search(r'(?:成交額|Turnover)[^0-9]{0,50}([0-9][0-9,]*\.?[0-9]*)\s*([BbMm億])', html, flags=re.I)
    if not m:
        raise RuntimeError("AASTOCKS turnover not found")
    num = float(m.group(1).replace(",", ""))
    unit = m.group(2)

    if unit == "億":
        return round(num, 2)
    if unit in ("B", "b"):
        return round(num * 10.0, 2)   # HKD billions -> 億
    if unit in ("M", "m"):
        return round(num / 100.0, 2)  # HKD millions -> 億
    return round(num, 2)


def normalize_hk_turnover_to_yi(val: Optional[float]) -> Optional[float]:
    """
    Ensure value is 億港幣.
    - If already in a reasonable range (0 ~ 50,000), keep.
    - If too large, scale down by 1e6 or 1e3 heuristics.
    """
    if val is None:
        return None
    try:
        x = float(val)
    except Exception:
        return None
    x = abs(x)
    if 0 <= x <= 50000:
        return x
    # too large: likely HKD or HK$ million multiplied
    for _ in range(8):
        if x <= 50000:
            break
        if x >= 1e7:
            x /= 1e6
        else:
            x /= 1e3
    return x


def fetch_hkex_turnover_yi(date_dt: datetime) -> float:
    """Fetch HKEX turnover and return 億港幣."""
    html = fetch_hkex_dayquot_html(date_dt)
    hkd = parse_hkex_turnover_hkd(html)
    return round(hkd / 1e8, 2)


def get_two_hkex_turnover_by_hsi_dates(hsi_today_dt, hsi_prev_dt) -> Tuple[Optional[float], Optional[float]]:
    """
    Align HKEX turnover dates with yfinance ^HSI trade dates.
    HKEX failure -> today fallback to AASTOCKS latest.
    """
    out_today = None
    out_prev = None

    try:
        if hsi_today_dt is not None and not pd.isna(hsi_today_dt):
            d = hsi_today_dt.to_pydatetime() if isinstance(hsi_today_dt, pd.Timestamp) else hsi_today_dt
            out_today = fetch_hkex_turnover_yi(d)
    except Exception:
        out_today = None

    if out_today is None:
        try:
            html = fetch_aastocks_hsi_html()
            out_today = parse_aastocks_turnover_yi(html)
        except Exception:
            out_today = None

    try:
        if hsi_prev_dt is not None and not pd.isna(hsi_prev_dt):
            d = hsi_prev_dt.to_pydatetime() if isinstance(hsi_prev_dt, pd.Timestamp) else hsi_prev_dt
            out_prev = fetch_hkex_turnover_yi(d)
    except Exception:
        out_prev = None

    return out_today, out_prev


# ====== Google Sheets helpers ======
def gsheet_service():
    gsheet_id = os.getenv("GSHEET_ID", "").strip()
    sa_json = os.getenv("GCP_SA_JSON", "").strip()
    if not gsheet_id or not sa_json:
        raise RuntimeError("缺少 GSHEET_ID 或 GCP_SA_JSON（請在 GitHub Secrets/Env 設定）")

    try:
        sa_info = json.loads(sa_json)
    except Exception as e:
        raise RuntimeError(f"GCP_SA_JSON 不是合法 JSON：{e}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def sheet_values_get(svc, spreadsheet_id: str, rng: str) -> List[List[Any]]:
    return svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute().get("values", [])


def sheet_values_batch_update(svc, spreadsheet_id: str, updates: List[Dict[str, Any]]):
    body = {"valueInputOption": "USER_ENTERED", "data": updates}
    svc.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def _a1(col: int, row: int) -> str:
    """1-based col/row to A1, col 1 -> A."""
    col_letters = ""
    x = col
    while x > 0:
        x, r = divmod(x - 1, 26)
        col_letters = chr(65 + r) + col_letters
    return f"{col_letters}{row}"


def find_stock_rows_from_sheet(grid: List[List[Any]]) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
    """
    Scan a 2D grid (rows x cols) and find:
    - tw_header row contains "台股" and "台幣"
    - hk_header row contains "港股" and "港幣"
    Then parse codes from column A under each section.

    Supports header order reversed.
    """
    tw_header = None
    hk_header = None

    for i, row in enumerate(grid, start=1):
        joined = _norm_text("".join(str(c) for c in row if c is not None))
        if tw_header is None and ("台股" in joined and "台幣" in joined):
            tw_header = i
        if hk_header is None and ("港股" in joined and "港幣" in joined):
            hk_header = i

    if tw_header is None or hk_header is None:
        # provide a quick preview for debugging
        preview_lines = []
        for r in range(1, min(41, len(grid) + 1)):
            row = grid[r - 1] if r - 1 < len(grid) else []
            preview_lines.append(f"{r:03d}: " + " | ".join(str(x) for x in (row[:6] if row else [])))
        raise RuntimeError(
            "找不到『台股（台幣）』或『港股（港幣）』標題列，請確認分頁版面。\n"
            + "\n".join(preview_lines)
        )

    def _read_codes_down(start_row: int, stop_row: Optional[int]) -> List[Tuple[int, str]]:
        out: List[Tuple[int, str]] = []
        r = start_row
        while True:
            if stop_row is not None and r >= stop_row:
                break
            if r > len(grid):
                break
            a_val = ""
            try:
                a_val = str(grid[r - 1][0]).strip() if len(grid[r - 1]) > 0 else ""
            except Exception:
                a_val = ""
            if a_val == "" or a_val.lower() == "none":
                break
            code = a_val.replace(".HK", "").replace("HK", "").strip()
            # accept digits only for TW, but for safety allow numeric-looking
            out.append((r, code))
            r += 1
        return out

    tw_rows: List[Tuple[int, str]]
    hk_rows: List[Tuple[int, str]]

    if tw_header < hk_header:
        tw_rows = _read_codes_down(tw_header + 1, hk_header)
        hk_rows = _read_codes_down(hk_header + 1, None)
    else:
        hk_rows = _read_codes_down(hk_header + 1, tw_header)
        tw_rows = _read_codes_down(tw_header + 1, None)

    # TW: keep only pure digits (4-digit codes etc)
    tw_rows = [(r, c) for (r, c) in tw_rows if str(c).strip().isdigit()]
    # HK: normalize to 4 digits
    hk_rows = [(r, str(c).strip().replace(".HK", "").zfill(4)) for (r, c) in hk_rows if str(c).strip()]

    return tw_rows, hk_rows


# ====== Main ======
def main():
    gsheet_id = os.getenv("GSHEET_ID", "").strip()
    tab = os.getenv("GSHEET_TAB", "").strip() or "IR_updated (PC HOME)"
    if not gsheet_id:
        raise RuntimeError("缺少 GSHEET_ID")

    svc = gsheet_service()

    # Read a block for scanning and codes from column A
    # Use A1:Z300 to be safe with headers not strictly in B.
    rng_grid = f"{tab}!A1:Z300"
    grid = sheet_values_get(svc, gsheet_id, rng_grid)

    tw_rows, hk_rows = find_stock_rows_from_sheet(grid)

    cache = load_cache()
    tw_codes = [code for _, code in tw_rows]
    hk_codes = [code for _, code in hk_rows]  # already 4-digit

    tw_tickers = [resolve_tw_ticker(c, cache) for c in tw_codes]
    hk_tickers = [hk_ticker(c) for c in hk_codes]
    save_cache(cache)

    stock_map = build_ohlcv_map(tw_tickers + hk_tickers)
    idx_map = build_ohlcv_map([TICKER_TWII, TICKER_HSI])

    now = datetime.now()

    # Indices
    twii = idx_map.get(TICKER_TWII, {})
    hsi = idx_map.get(TICKER_HSI, {})

    # TWII turnover aligned with ^TWII trade dates
    tw_t_date = twii.get("t_date")
    tw_p_date = twii.get("p_date")
    tw_today_yi = None
    tw_prev_yi = None
    if isinstance(tw_t_date, pd.Timestamp):
        tw_today_yi = twse_turnover_yi(tw_t_date.to_pydatetime())
    if isinstance(tw_p_date, pd.Timestamp):
        tw_prev_yi = twse_turnover_yi(tw_p_date.to_pydatetime())

    # HKEX turnover aligned with ^HSI dates
    hk_today_yi, hk_prev_yi = get_two_hkex_turnover_by_hsi_dates(hsi.get("t_date"), hsi.get("p_date"))
    hk_today_yi = normalize_hk_turnover_to_yi(hk_today_yi)
    hk_prev_yi = normalize_hk_turnover_to_yi(hk_prev_yi)

    updates: List[Dict[str, Any]] = []

    def put(a1: str, value):
        updates.append({"range": f"{tab}!{a1}", "values": [[value]]})

    # timestamp
    put("L3", now.strftime("%Y-%m-%d %H:%M:%S"))

    # Index close/prev
    put("D6", _round2(twii.get("close")))
    put("E6", _round2(twii.get("prev_close")))
    put("H6", tw_today_yi)
    put("I6", tw_prev_yi)

    put("D8", _round2(hsi.get("close")))
    put("E8", _round2(hsi.get("prev_close")))
    put("H8", hk_today_yi)
    put("I8", hk_prev_yi)

    # Stocks: write to fixed columns per row:
    # D close, E prev_close, H open, I low, J high, K volume-lots/hands
    # TW lots = volume/1000, HK hands = volume/lot_size (default 1000)
    hk_lot_size_default = 1000

    # TW section
    for (r, _code), ticker in zip(tw_rows, tw_tickers):
        d = stock_map.get(ticker, {})
        close = _round2(d.get("close"))
        prev = _round2(d.get("prev_close"))
        opn = _round2(d.get("open"))
        low = _round2(d.get("low"))
        high = _round2(d.get("high"))
        vol = d.get("volume")

        lots = None
        if not _isnan(vol):
            try:
                lots = int(round(float(vol) / 1000))
            except Exception:
                lots = None

        put(f"D{r}", close)
        put(f"E{r}", prev)
        put(f"H{r}", opn)
        put(f"I{r}", low)
        put(f"J{r}", high)
        put(f"K{r}", lots)

    # HK section
    for (r, code), ticker in zip(hk_rows, hk_tickers):
        d = stock_map.get(ticker, {})
        close = _round2(d.get("close"))
        prev = _round2(d.get("prev_close"))
        opn = _round2(d.get("open"))
        low = _round2(d.get("low"))
        high = _round2(d.get("high"))
        vol = d.get("volume")

        hands = None
        if not _isnan(vol):
            try:
                hands = int(round(float(vol) / hk_lot_size_default))
            except Exception:
                hands = None

        put(f"D{r}", close)
        put(f"E{r}", prev)
        put(f"H{r}", opn)
        put(f"I{r}", low)
        put(f"J{r}", high)
        put(f"K{r}", hands)

    # batch update
    sheet_values_batch_update(svc, gsheet_id, updates)

    print("DONE: updated Google Sheet")
    print(f"TW rows: {len(tw_rows)} | HK rows: {len(hk_rows)}")
    print(f"TWII turnover (today/prev, 億元): {tw_today_yi} / {tw_prev_yi}")
    print(f"HK turnover (today/prev, 億港幣): {hk_today_yi} / {hk_prev_yi}")


if __name__ == "__main__":
    main()
