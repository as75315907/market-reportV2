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


def sheet_a1(tab_name: str) -> str:
    """Return a sheet name safe for A1 notation (quotes when needed)."""
    t = str(tab_name or "").strip()
    if t == "":
        return t
    # quote if contains spaces or special chars
    if re.search(r"[\s\(\)\[\]\!\:\'\"]", t):
        t = t.replace("'", "''")
        return f"'{t}'"
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
    從 Google Sheet 的 A1:Z? 2D grid 解析「台股/港股」個股區塊的列號與代碼。

    你這張版面上方還有「大盤收盤資料」(台股/香港恆生 + (台幣)/(港幣))，
    會讓單純用包含關鍵字的方式誤判 header 列，導致 TW rows=0。
    這裡改成：
    1) 先定位「個股資料」區塊起始列（避免抓到上方大盤表）。
    2) 再在其下方找「台股（台幣）」與「港股（港幣）」標題列（允許括號/空白/換行差異）。
    3) 台股：抓 4~6 位數代碼；港股：抓 xxxx.HK / xxxxx.HK 形式。
    """
    # ----- helpers -----
    def _norm(x: Any) -> str:
        s = "" if x is None else str(x)
        return s.replace("\u00a0", " ").strip()

    def _row_text(r: int) -> str:
        return " ".join(_norm(c) for c in grid[r] if _norm(c))

    def _cell_has(s: str, *keys: str) -> bool:
        s2 = s.replace(" ", "")
        return all(k.replace(" ", "") in s2 for k in keys)

    def _is_good_section_header(s: str, must1: str, must2: str) -> bool:
        # 避免抓到「208.01億（台幣）」這種含數字/億的 cell
        if not _cell_has(s, must1, must2):
            return False
        if re.search(r"\d", s):
            return False
        if "億" in s:
            return False
        # 太長通常不是 section header
        return len(s) <= 30

    def _find_anchor_row() -> int:
        # 先找「個股資料」，找不到就回傳 0（仍可 fallback）
        for r in range(min(len(grid), 200)):
            txt = _row_text(r)
            if "個股資料" in txt:
                return r
        return 0

    def _find_header_row(after_r: int, kind: str) -> int | None:
        # kind: "tw" or "hk"
        must = ("台股", "台幣") if kind == "tw" else ("港股", "港幣")
        for r in range(after_r, min(len(grid), after_r + 200)):
            for c in range(min(len(grid[r]), 26)):
                cell = _norm(grid[r][c])
                if not cell:
                    continue
                # 容許括號/全半形/換行，例如 台股（台幣）、台股 (台幣)、台股\n(台幣)
                cell2 = cell.replace("\n", "").replace("（", "(").replace("）", ")")
                if _is_good_section_header(cell2, must[0], must[1]):
                    return r
        return None

    def _tw_code_from_a(val: Any) -> str | None:
        s = _norm(val)
        m = re.match(r"^(\d{4,6})", s)
        return m.group(1) if m else None

    def _hk_code_from_a(val: Any) -> str | None:
        s = _norm(val).upper()
        # 03368.HK / 0825.HK / 825.HK
        m = re.match(r"^0*([0-9]{1,5})\.HK$", s)
        if m:
            return m.group(1).zfill(4) if len(m.group(1)) <= 4 else m.group(1).zfill(5)
        # 也容許寫成 03368HK / 0825HK
        m = re.match(r"^0*([0-9]{1,5})HK$", s)
        if m:
            return m.group(1).zfill(4) if len(m.group(1)) <= 4 else m.group(1).zfill(5)
        return None

    def _read_codes_down(start_row_1based: int, end_row_1based: int | None, kind: str) -> List[Tuple[int, str]]:
        out: List[Tuple[int, str]] = []
        blank_streak = 0
        r1 = start_row_1based
        r2 = end_row_1based if end_row_1based is not None else len(grid)
        for rr in range(r1, min(r2, len(grid) + 1)):
            a = grid[rr - 1][0] if len(grid[rr - 1]) >= 1 else ""
            code = _tw_code_from_a(a) if kind == "tw" else _hk_code_from_a(a)
            if code:
                out.append((rr, code))
                blank_streak = 0
                continue
            # 遇到空白代碼列，累積 3 次就結束（避免掃到很下面）
            if _norm(a) == "":
                blank_streak += 1
                if blank_streak >= 3 and out:
                    break
            else:
                blank_streak = 0
        return out

    # ----- main logic -----
    anchor = _find_anchor_row()

    tw_header = _find_header_row(anchor, "tw")
    hk_header = _find_header_row(anchor, "hk")

    # fallback：若找不到 header，就以 A 欄格式推斷區塊（保底）
    if tw_header is None:
        # 找第一個 4 位數台股代碼列
        for r in range(anchor, min(len(grid), anchor + 200)):
            code = _tw_code_from_a(grid[r][0] if grid[r] else "")
            if code:
                tw_header = r - 1  # 讓 start=tw_header+1 落在 code 列
                break

    if hk_header is None:
        for r in range(anchor, min(len(grid), anchor + 300)):
            code = _hk_code_from_a(grid[r][0] if grid[r] else "")
            if code:
                hk_header = r - 1
                break

    if tw_header is None or hk_header is None:
        raise RuntimeError("找不到『台股（台幣）』或『港股（港幣）』區塊，請確認分頁版面 / 欄位是否變更。")

    # 確保 hk_header 在 tw_header 之後（避免顛倒）
    if hk_header <= tw_header:
        # 嘗試在 tw_header 之後重新找 hk header
        hk_header2 = _find_header_row(tw_header + 1, "hk")
        if hk_header2 is not None:
            hk_header = hk_header2
        else:
            # 最後保底：找第一個 .HK 代碼列
            for r in range(tw_header + 1, min(len(grid), tw_header + 400)):
                code = _hk_code_from_a(grid[r][0] if grid[r] else "")
                if code:
                    hk_header = r - 1
                    break

    # 台股代碼列：從 tw_header 下一列開始，到 hk_header（不含）為止
    tw_rows = _read_codes_down(tw_header + 2, hk_header + 1, "tw")
    # 港股代碼列：從 hk_header 下一列開始到底
    hk_rows = _read_codes_down(hk_header + 2, None, "hk")

    return tw_rows, hk_rows


def main():
    gsheet_id = os.getenv("GSHEET_ID", "").strip()
    tab = os.getenv("GSHEET_TAB", "").strip() or "IR_updated (PC HOME)"
    if not gsheet_id:
        raise RuntimeError("缺少 GSHEET_ID")

    svc = gsheet_service()

    # Read a block for scanning and codes from column A
    # Use A1:Z300 to be safe with headers not strictly in B.
    tab_a1 = sheet_a1(tab)
    rng_grid = f"{tab_a1}!A1:Z300"
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
        updates.append({"range": f"{tab_a1}!{a1}", "values": [[value]]})

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
