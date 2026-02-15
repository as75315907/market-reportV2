# -*- coding: utf-8 -*-
"""
Daily Market Report -> Google Sheets (GitHub Actions friendly)

What this script does
- Reads your Google Sheet tab (e.g. "IR_updated (PC HOME)")
- Locates TW/HK stock blocks by **Column A patterns** (no need to find "台股（台幣）/港股（港幣）" titles):
    - TW block: A cell is a 4-digit code (e.g. 2926)
    - HK block: A cell is like 03368.HK / 825.HK
- Updates fixed cells:
    - L3: update timestamp
    - D6/E6: TWII close / prev close (best-effort, uses yfinance; if blocked, leaves blank)
    - H6/I6: TWII turnover today/prev (億元) from TWSE FMTQIK
    - D8/E8: HSI close / prev close (best-effort, uses yfinance; if blocked, leaves blank)
    - H8/I8: HK market turnover today/prev (億港幣) from HKEX Day Quotations (with AASTOCKS fallback)
- Updates each stock row (keeps formulas intact by writing only these columns):
    - D: 今日收盤
    - E: 前一交易日收盤
    - H: 開盤
    - I: 最低
    - J: 最高
    - K: 成交張數 (台股=成交股數/1000；港股=成交股數/lot_size, default 1000)

Required GitHub Secrets (or env)
- GSHEET_ID: Google Sheet ID (the long id in URL)
- GSHEET_TAB: sheet tab name, e.g. IR_updated (PC HOME)
- GCP_SA_JSON: Google Service Account JSON (string)
    Tip: if private_key has \\n, script will convert to real newlines.

Optional
- DEBUG_HKEX=1 to save HKEX/AASTOCKS HTML in debug/
"""

import os
import re
import json
import math
import time
import subprocess
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf

from googleapiclient.discovery import build
from google.oauth2 import service_account


# ========= Config =========
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "").strip()
GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()

DEBUG_HKEX = os.getenv("DEBUG_HKEX", "") == "1"
DEBUG_DIR = os.path.join(BASE_DIR, "debug")

# Indices (Yahoo, best-effort; may be blocked on GitHub IP sometimes)
TICKER_TWII = "^TWII"
TICKER_HSI = "^HSI"

# TWSE month JSON (turnover)
TWSE_FMTQIK = "https://www.twse.com.tw/exchangeReport/FMTQIK"

# HKEX turnover (day quotations)
HKEX_DAYQUOT = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{yymmdd}e.htm"
HKEX_DAYQUOT_REFERER = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/qtn.asp"
AASTOCKS_HSI_URL = "https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI&o=0&p=&s=8&t=6"

# TWSE daily quotes (all listed stocks)
TWSE_MI_INDEX_URLS = [
    "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
    "https://www.twse.com.tw/exchangeReport/MI_INDEX",
]


# ========= Small utils =========
def _debug_save(name: str, text: str):
    if not DEBUG_HKEX:
        return
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        Path(os.path.join(DEBUG_DIR, name)).write_text(text or "", encoding="utf-8", errors="ignore")
    except Exception:
        pass

def _isnan(x) -> bool:
    try:
        return x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))
    except Exception:
        return True

def _to_float(s):
    if s is None:
        return None
    if isinstance(s, (int, float)) and not (isinstance(s, float) and math.isnan(s)):
        return float(s)
    st = str(s).strip().replace(",", "")
    if st in ("", "--", "—", "NA", "N/A"):
        return None
    m = re.search(r"[-+]?\d+(?:\.\d+)?", st)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _to_int(s):
    f = _to_float(s)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None

def _round2(x):
    if x is None:
        return None
    try:
        return round(float(x), 2)
    except Exception:
        return None

def _hkex_yymmdd(dt: datetime) -> str:
    return dt.strftime("%y%m%d")


# ========= Google Sheets helpers =========
def gsheet_service():
    if not GSHEET_ID or not GCP_SA_JSON or not GSHEET_TAB:
        raise RuntimeError("缺少 GSHEET_ID / GSHEET_TAB / GCP_SA_JSON（請在 GitHub Secrets/Env 設定）")

    try:
        info = json.loads(GCP_SA_JSON)
    except Exception as e:
        raise RuntimeError(f"GCP_SA_JSON 不是合法 JSON：{e}")

    # Fix private_key newlines if stored as one-line secret
    pk = info.get("private_key")
    if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
        info["private_key"] = pk.replace("\\n", "\n")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return svc

def sheet_values_get(svc, a1_range: str):
    return svc.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID, range=a1_range
    ).execute()

def sheet_values_batch_update(svc, data: list[dict], value_input_option: str = "USER_ENTERED"):
    body = {"valueInputOption": value_input_option, "data": data}
    return svc.spreadsheets().values().batchUpdate(
        spreadsheetId=GSHEET_ID, body=body
    ).execute()


# ========= Find TW/HK stock rows by Column A patterns =========
_TW_RE = re.compile(r"^\s*\d{4}\s*$")
_HK_RE = re.compile(r"^\s*\d{1,5}\s*\.HK\s*$", re.I)

def _norm_cell_str(v) -> str:
    if v is None:
        return ""
    # Google Sheets API may return numbers as "2926" (string) or "2926.0"
    if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
        return str(int(v))
    s = str(v).strip()
    # handle "2926.0"
    m = re.fullmatch(r"(\d{4})\.0", s)
    if m:
        return m.group(1)
    return s

def find_stock_rows_from_col_a(col_a_vals: list):
    """
    Returns:
      tw_rows: list[(row_index, code4)]
      hk_rows: list[(row_index, code4)]  # code4 is zero-filled 4-digit for hk
    """
    # Normalize all A values
    A = [_norm_cell_str(x) for x in col_a_vals]
    tw_start = None
    hk_start = None

    # First TW code row
    for i, s in enumerate(A, start=1):
        if _TW_RE.match(s):
            tw_start = i
            break

    # First HK code row (search after tw_start if possible)
    start_idx = (tw_start or 1) - 1
    for i in range(start_idx, len(A)):
        if _HK_RE.match(A[i]):
            hk_start = i + 1
            break

    if tw_start is None or hk_start is None:
        raise RuntimeError("無法在 A 欄自動定位台股/港股區塊：請確認 A 欄是否有台股4碼與港股xxxx.HK 代碼。")

    # TW rows: from tw_start until hk_start-1, stop on first non-4digit/blank
    tw_rows = []
    r = tw_start
    while r < hk_start:
        s = A[r - 1] if r - 1 < len(A) else ""
        if not _TW_RE.match(s):
            break
        tw_rows.append((r, s))
        r += 1

    # HK rows: from hk_start downward, stop on first non-HK/blank
    hk_rows = []
    r = hk_start
    while r <= len(A):
        s = A[r - 1] if r - 1 < len(A) else ""
        if not _HK_RE.match(s):
            break
        code = re.sub(r"\s*\.HK\s*$", "", s, flags=re.I).strip()
        hk_rows.append((r, code.zfill(4)))
        r += 1

    return tw_rows, hk_rows


# ========= TWSE turnover (FMTQIK) =========
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})

def _ad_to_twse_date_str(dt: datetime) -> str:
    # ROC year like "115/02/11"
    roc_y = dt.year - 1911
    return f"{roc_y:03d}/{dt.month:02d}/{dt.day:02d}"

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

def extract_turnover_from_fmtqik(obj: dict, dt: datetime) -> int | None:
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

def twse_turnover_yi(dt: datetime) -> float | None:
    """Return turnover in 億元 (TWD 1e8)."""
    try:
        obj = fetch_fmtqik_month_json(dt)
        val = extract_turnover_from_fmtqik(obj, dt)
        if val is None:
            return None
        return round(val / 1e8, 2)
    except Exception:
        return None

def latest_two_tw_trade_dates(max_back_days: int = 20) -> tuple[datetime, datetime]:
    """Find latest 2 trading dates by probing FMTQIK backward."""
    d = datetime.now().date()
    found = []
    for _ in range(max_back_days):
        dt = datetime(d.year, d.month, d.day)
        yi = twse_turnover_yi(dt)
        if yi is not None:
            found.append(dt)
            if len(found) >= 2:
                return found[0], found[1]
        d = d - timedelta(days=1)
    raise RuntimeError("找不到最近兩個台股交易日（FMTQIK 回傳皆為 None），請檢查 TWSE 是否可連線。")


# ========= TWSE daily stock quotes (MI_INDEX) =========
def fetch_twse_mi_index(date_dt: datetime) -> dict:
    ymd = date_dt.strftime("%Y%m%d")
    params = {"response": "json", "date": ymd, "type": "ALLBUT0999"}
    last_err = None
    for url in TWSE_MI_INDEX_URLS:
        try:
            r = _SESSION.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"TWSE MI_INDEX 取得失敗：{last_err}")

def parse_twse_quotes(obj: dict) -> dict:
    """
    Return: {code: {"open","high","low","close","volume_shares"}}
    This parses the table that contains fields including:
      證券代號, 開盤價, 最高價, 最低價, 收盤價, 成交股數
    """
    tables = obj.get("tables", [])
    if not isinstance(tables, list):
        return {}

    for tb in tables:
        fields = tb.get("fields", [])
        data = tb.get("data", [])
        if not isinstance(fields, list) or not isinstance(data, list):
            continue
        f = [str(x).strip() for x in fields]
        # Must include key fields
        if "證券代號" not in f or "收盤價" not in f:
            continue

        # Identify column indexes (some tables are not stock quotes; filter by having these)
        def idx(name: str):
            try:
                return f.index(name)
            except Exception:
                return None

        i_code = idx("證券代號")
        i_open = idx("開盤價")
        i_high = idx("最高價")
        i_low  = idx("最低價")
        i_close = idx("收盤價")
        i_vol  = idx("成交股數")

        if i_code is None or i_close is None or i_open is None or i_high is None or i_low is None or i_vol is None:
            continue

        out = {}
        for row in data:
            if not isinstance(row, list) or len(row) <= max(i_code, i_open, i_high, i_low, i_close, i_vol):
                continue
            code = str(row[i_code]).strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            out[code] = {
                "open": _to_float(row[i_open]),
                "high": _to_float(row[i_high]),
                "low": _to_float(row[i_low]),
                "close": _to_float(row[i_close]),
                "volume_shares": _to_int(row[i_vol]),
            }
        if out:
            return out
    return {}


# ========= HKEX turnover parsing =========
def _curl_get_text(url: str, timeout: int = 30, insecure: bool = False, http1: bool = True, extra_headers: list[str] | None = None) -> str:
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

def fetch_hkex_dayquot_html(trade_dt: datetime) -> str:
    yymmdd = _hkex_yymmdd(trade_dt)
    url = HKEX_DAYQUOT.format(yymmdd=yymmdd)
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
        "Referer": HKEX_DAYQUOT_REFERER,
    }
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
            raise RuntimeError("HKEX 回傳內容疑似非日報頁")
        return html
    except Exception as e_req:
        _debug_save(f"hkex_dayquot_{yymmdd}_requests_error.log", repr(e_req))
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
    """Return HK market turnover in HKD (not in Yi)."""
    if not html:
        raise RuntimeError("HKEX HTML 空白")

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
            # HKEX dayquot commonly uses HK$ Million
            if v >= 10_000:
                return v * 1_000_000.0
            return v

    # fallback: read_html
    try:
        tables = pd.read_html(StringIO(html))
    except Exception as e:
        raise RuntimeError(f"HKEX read_html 失敗：{e}")

    def _unit_multiplier(col_text: str, sample_val: float | None):
        t = (col_text or "").lower()
        if "million" in t or "mn" in t or "百萬" in col_text or "百万" in col_text:
            return 1_000_000.0
        if "billion" in t or "bn" in t or "十億" in col_text or "十亿" in col_text:
            return 1_000_000_000.0
        # default assume million
        if sample_val is not None and 10_000 <= sample_val <= 1_000_000:
            return 1_000_000.0
        return 1.0

    def _to_float_any(x):
        s = str(x).strip().replace(",", "")
        m = re.search(r"([0-9][0-9]*\.?[0-9]*)", s)
        return float(m.group(1)) if m else None

    for df in tables:
        try:
            df = df.fillna("")
        except Exception:
            pass
        cols = [str(c).strip() for c in getattr(df, "columns", [])]
        if not cols:
            continue
        # find turnover column
        idxs = [i for i, c in enumerate(cols) if ("turnover" in c.lower()) or ("成交額" in c) or ("成交金額" in c) or ("成交金额" in c)]
        if not idxs:
            continue
        idx = idxs[0]
        series = df.iloc[:, idx]
        # find total row if any
        pick = None
        if df.shape[1] >= 2:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            mask = first_col.str.contains(r"^(total|總計|合計)$", case=False, regex=True)
            if mask.any():
                v = _to_float_any(series[mask].iloc[-1])
                if v is not None:
                    pick = v
        if pick is None:
            vals = [v for v in (_to_float_any(x) for x in series.tolist()) if v is not None]
            if vals:
                pick = vals[-1]
        if pick is None:
            continue
        mult = _unit_multiplier(cols[idx], pick)
        return pick * mult

    raise RuntimeError("HKEX 找不到 Turnover")

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
    """Return turnover in 億港幣 (HKD 1e8)."""
    if not html:
        raise RuntimeError("AASTOCKS HTML 空白")
    m = re.search(r'(?:成交額|Turnover)[^0-9]{0,80}([0-9][0-9,]*\.?[0-9]*)\s*([BbMm億])', html, flags=re.I)
    if not m:
        m = re.search(r'class=["\']turnover["\'][^>]*>\s*([0-9][0-9,]*\.?[0-9]*)\s*([BbMm億])', html, flags=re.I)
    if not m:
        raise RuntimeError("AASTOCKS 找不到成交額/Turnover")
    num = float(m.group(1).replace(",", ""))
    unit = m.group(2)
    if unit == "億":
        return round(num, 2)
    if unit in ("B", "b"):  # HKD billions
        return round(num * 10.0, 2)
    if unit in ("M", "m"):  # HKD millions
        return round(num / 100.0, 2)
    return round(num, 2)

def fetch_hkex_turnover_yi(date_dt: datetime) -> float:
    html = fetch_hkex_dayquot_html(date_dt)
    hkd = parse_hkex_turnover_hkd(html)
    return round(hkd / 1e8, 2)

def get_two_hk_turnover_by_dates(hk_today_dt: datetime, hk_prev_dt: datetime) -> tuple[float | None, float | None]:
    out_today = None
    out_prev = None
    try:
        out_today = fetch_hkex_turnover_yi(hk_today_dt)
    except Exception:
        out_today = None
    if out_today is None:
        try:
            out_today = parse_aastocks_turnover_yi(fetch_aastocks_hsi_html())
        except Exception:
            out_today = None

    try:
        out_prev = fetch_hkex_turnover_yi(hk_prev_dt)
    except Exception:
        out_prev = None
    return out_today, out_prev


# ========= Yahoo helpers (HK stocks and indices) =========
def hist_one(ticker: str) -> pd.DataFrame:
    return yf.Ticker(ticker).history(period="1mo", interval="1d", auto_adjust=False)

def last_two(series: pd.Series):
    s = series.dropna()
    if len(s) < 2:
        return (pd.NaT, math.nan, pd.NaT, math.nan)
    return s.index[-1], float(s.iloc[-1]), s.index[-2], float(s.iloc[-2])

def build_yahoo_ohlcv_map(tickers: list[str]) -> dict:
    out = {}
    for t in tickers:
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
                return None
            s = h[col].dropna()
            return float(s.iloc[-1]) if len(s) else None

        out[t] = {
            "t_date": t_date,
            "p_date": p_date,
            "close": t_close,
            "prev_close": p_close,
            "open": _last("Open"),
            "high": _last("High"),
            "low": _last("Low"),
            "volume": _last("Volume"),
        }
        time.sleep(0.15)
    return out

def hk_ticker(code4: str) -> str:
    return f"{int(str(code4)):04d}.HK"


# ========= Main =========
def main():
    svc = gsheet_service()

    # Read A column (enough rows to cover blocks)
    a1 = f"'{GSHEET_TAB}'!A1:A200"
    resp = sheet_values_get(svc, a1)
    col_a = [r[0] if r else "" for r in resp.get("values", [])]

    tw_rows, hk_rows = find_stock_rows_from_col_a(col_a)

    # Determine latest 2 TW trade dates (for turnover and TWSE daily quotes)
    tw_today_dt, tw_prev_dt = latest_two_tw_trade_dates()
    tw_today_yi = twse_turnover_yi(tw_today_dt)
    tw_prev_yi = twse_turnover_yi(tw_prev_dt)

    # TWSE daily quotes for the 2 dates
    tw_q_today = parse_twse_quotes(fetch_twse_mi_index(tw_today_dt))
    tw_q_prev = parse_twse_quotes(fetch_twse_mi_index(tw_prev_dt))

    # HK stocks via Yahoo
    hk_codes = [code4 for _, code4 in hk_rows]
    hk_tickers = [hk_ticker(c) for c in hk_codes]
    hk_map = build_yahoo_ohlcv_map(hk_tickers)

    # Indices via Yahoo (best-effort)
    idx_map = build_yahoo_ohlcv_map([TICKER_TWII, TICKER_HSI])
    twii = idx_map.get(TICKER_TWII, {})
    hsi = idx_map.get(TICKER_HSI, {})

    # HK turnover by HKEX (align by HSI trade dates if we have them; else use today/prev by TW calendar as fallback)
    hk_today_dt = None
    hk_prev_dt = None
    if isinstance(hsi.get("t_date"), pd.Timestamp) and not pd.isna(hsi.get("t_date")):
        hk_today_dt = hsi["t_date"].to_pydatetime()
    if isinstance(hsi.get("p_date"), pd.Timestamp) and not pd.isna(hsi.get("p_date")):
        hk_prev_dt = hsi["p_date"].to_pydatetime()
    if hk_today_dt is None:
        hk_today_dt = datetime.now()
    if hk_prev_dt is None:
        hk_prev_dt = hk_today_dt - timedelta(days=1)

    hk_today_yi, hk_prev_yi = get_two_hk_turnover_by_dates(hk_today_dt, hk_prev_dt)

    # Build batch updates (write only non-formula cells)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updates = []

    def _put(cell: str, value):
        updates.append({"range": f"'{GSHEET_TAB}'!{cell}", "values": [[value]]})

    # Timestamp
    _put("L3", now)

    # Indices
    _put("D6", _round2(twii.get("close")) if twii else None)
    _put("E6", _round2(twii.get("prev_close")) if twii else None)
    _put("H6", tw_today_yi)
    _put("I6", tw_prev_yi)

    _put("D8", _round2(hsi.get("close")) if hsi else None)
    _put("E8", _round2(hsi.get("prev_close")) if hsi else None)
    _put("H8", hk_today_yi)
    _put("I8", hk_prev_yi)

    # TW stocks -> from TWSE quotes
    for r, code in tw_rows:
        today = tw_q_today.get(code, {})
        prev = tw_q_prev.get(code, {})
        close = _round2(today.get("close"))
        prev_close = _round2(prev.get("close"))
        opn = _round2(today.get("open"))
        low = _round2(today.get("low"))
        high = _round2(today.get("high"))
        vol_shares = today.get("volume_shares")
        lots = None
        if vol_shares is not None:
            try:
                lots = int(round(int(vol_shares) / 1000))
            except Exception:
                lots = None

        # Write D:E and H:K (keep F/G/L formulas untouched)
        updates.append({"range": f"'{GSHEET_TAB}'!D{r}:E{r}", "values": [[close, prev_close]]})
        updates.append({"range": f"'{GSHEET_TAB}'!H{r}:K{r}", "values": [[opn, low, high, lots]]})

    # HK stocks -> from Yahoo
    default_lot = 1000
    for (r, code4), ticker in zip(hk_rows, hk_tickers):
        d = hk_map.get(ticker, {})
        close = _round2(d.get("close"))
        prev_close = _round2(d.get("prev_close"))
        opn = _round2(d.get("open"))
        low = _round2(d.get("low"))
        high = _round2(d.get("high"))
        vol = d.get("volume")
        hands = None
        if vol is not None and not _isnan(vol):
            try:
                hands = int(round(float(vol) / default_lot))
            except Exception:
                hands = None

        updates.append({"range": f"'{GSHEET_TAB}'!D{r}:E{r}", "values": [[close, prev_close]]})
        updates.append({"range": f"'{GSHEET_TAB}'!H{r}:K{r}", "values": [[opn, low, high, hands]]})

    # Execute update
    sheet_values_batch_update(svc, updates, value_input_option="USER_ENTERED")

    print("DONE: updated Google Sheet")
    print(f"TW rows: {len(tw_rows)} | HK rows: {len(hk_rows)}")
    print(f"TW dates: {tw_today_dt.date()} / {tw_prev_dt.date()}")
    print(f"TWII turnover (today/prev, 億元): {tw_today_yi} / {tw_prev_yi}")
    print(f"HK turnover (today/prev, 億港幣): {hk_today_yi} / {hk_prev_yi}")


if __name__ == "__main__":
    main()
