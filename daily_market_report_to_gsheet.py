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
import time
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

from google.oauth2 import service_account
from googleapiclient.discovery import build


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
    sheet_id = os.getenv("GSHEET_ID", "").strip()
    tab = os.getenv("GSHEET_TAB", "").strip()
    sa_json = os.getenv("GCP_SA_JSON", "").strip()
    if not sheet_id or not tab or not sa_json:
        raise RuntimeError("缺少 GSHEET_ID / GSHEET_TAB / GCP_SA_JSON（請在 GitHub Secrets/Env 設定）")

    # GitHub Secrets multiline JSON often comes with escaped newlines; normalize
    try:
        info = json.loads(sa_json)
    except json.JSONDecodeError:
        info = json.loads(sa_json.replace("\\n", "\n"))

    if isinstance(info, dict) and "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
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

def _today_taipei() -> datetime:
    # GitHub Actions default is UTC; if you need strict Asia/Taipei, convert with zoneinfo.
    return datetime.now()


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


# ========= TWSE: FMTQIK turnover =========
TWSE_FMTQIK = "https://www.twse.com.tw/exchangeReport/FMTQIK"

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
    try:
        obj = fetch_fmtqik_month_json(dt)
        val = extract_turnover_from_fmtqik(obj, dt)
        if val is None:
            return None
        return round(val / 1e8, 2)
    except Exception:
        return None


# ========= TW stocks: prefer TWSE MI_INDEX =========
TWSE_MI_INDEX = "https://www.twse.com.tw/exchangeReport/MI_INDEX"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_mi_index(date_dt: datetime) -> dict:
    params = {
        "response": "json",
        "date": date_dt.strftime("%Y%m%d"),
        "type": "ALLBUT0999",
    }
    r = _SESSION.get(TWSE_MI_INDEX, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _pick_idx(fields: list[str], key_words: list[str]) -> int | None:
    for i, f in enumerate(fields):
        fs = str(f)
        if any(k in fs for k in key_words):
            return i
    return None

def parse_mi_index_map(obj: dict) -> dict:
    """
    Return map: code -> dict(open, high, low, close, volume_shares)
    """
    out = {}
    tables = obj.get("tables", [])
    if not isinstance(tables, list):
        return out
    for t in tables:
        fields = t.get("fields", [])
        data = t.get("data", [])
        if not isinstance(fields, list) or not isinstance(data, list):
            continue
        fields_s = [str(x) for x in fields]
        i_code = 0
        i_open = _pick_idx(fields_s, ["開盤"])
        i_high = _pick_idx(fields_s, ["最高"])
        i_low  = _pick_idx(fields_s, ["最低"])
        i_close= _pick_idx(fields_s, ["收盤"])
        i_vol  = _pick_idx(fields_s, ["成交股數"])
        if i_open is None or i_high is None or i_low is None or i_close is None or i_vol is None:
            continue

        for row in data:
            if not isinstance(row, list) or len(row) <= max(i_vol, i_close, i_open, i_high, i_low, i_code):
                continue
            code = str(row[i_code]).strip()
            if not code.isdigit():
                continue
            close=_to_float(row[i_close])
            if close is None:
                continue
            out[code] = {
                "open": _to_float(row[i_open]),
                "high": _to_float(row[i_high]),
                "low":  _to_float(row[i_low]),
                "close": close,
                "volume": _to_float(row[i_vol]),  # shares
            }
    return out


# ========= TPEx fallback =========
TPEX_ST43 = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_tpex_st43(code: str, date_dt: datetime) -> dict | None:
    roc = _ad_to_twse_date_str(date_dt)
    params = {"l": "zh-tw", "o": "json", "d": roc, "stkno": str(code).strip()}
    r = _SESSION.get(TPEX_ST43, params=params, timeout=30)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None

def parse_tpex_st43(obj: dict) -> dict | None:
    data = obj.get("aaData") or obj.get("data") or None
    if not isinstance(data, list) or not data:
        return None
    row = data[0]
    if not isinstance(row, list) or len(row) < 8:
        return None
    # Common layout: close idx2, open4, high5, low6, volume7
    close = _to_float(row[2])
    if close is None:
        return None
    return {
        "open": _to_float(row[4]),
        "high": _to_float(row[5]),
        "low":  _to_float(row[6]),
        "close": close,
        "volume": _to_float(row[7]),
    }

def tw_price_pack_for_codes(codes: list[str], t_date: datetime, p_date: datetime):
    today_map, prev_map = {}, {}

    try:
        today_map = parse_mi_index_map(fetch_mi_index(t_date))
    except Exception:
        today_map = {}

    try:
        prev_map = parse_mi_index_map(fetch_mi_index(p_date))
    except Exception:
        prev_map = {}

    # TPEx補洞
    for c in codes:
        if c not in today_map:
            try:
                obj = fetch_tpex_st43(c, t_date)
                parsed = parse_tpex_st43(obj or {})
                if parsed:
                    today_map[c] = parsed
            except Exception:
                pass
        if c not in prev_map:
            try:
                obj = fetch_tpex_st43(c, p_date)
                parsed = parse_tpex_st43(obj or {})
                if parsed:
                    prev_map[c] = parsed
            except Exception:
                pass

    # 最後才 yfinance
    for c in codes:
        if c in today_map and c in prev_map:
            continue
        for suf in ("TW", "TWO"):
            tkr = f"{c}.{suf}"
            try:
                h = hist_one(tkr)
                if h is None or h.empty or h["Close"].dropna().shape[0] < 2:
                    continue
                t_dt, t_close, p_dt, p_close = last_two(h["Close"])
                if c not in today_map:
                    today_map[c] = {
                        "open": float(h["Open"].dropna().iloc[-1]) if "Open" in h else None,
                        "high": float(h["High"].dropna().iloc[-1]) if "High" in h else None,
                        "low":  float(h["Low"].dropna().iloc[-1])  if "Low"  in h else None,
                        "close": t_close,
                        "volume": float(h["Volume"].dropna().iloc[-1]) if "Volume" in h else None,
                    }
                if c not in prev_map:
                    prev_map[c] = {"close": p_close}
                break
            except Exception:
                continue

    return today_map, prev_map


# ========= HK turnover =========
HKEX_DAYQUOT = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{yymmdd}e.htm"
HKEX_DAYQUOT_REFERER = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/qtn.asp"
AASTOCKS_HSI_URL = "https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI&o=0&p=&s=8&t=6"

def _curl_get_text(url: str, timeout: int = 30, insecure: bool = False, http1: bool = False, extra_headers=None) -> str:
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
    headers = {"User-Agent": UA, "Referer": HKEX_DAYQUOT_REFERER}
    try:
        r = _SESSION.get(url, timeout=30, headers=headers)
        r.raise_for_status()
        html = r.text or ""
        _debug_save(f"hkex_dayquot_{yymmdd}_requests.html", html)
        return html
    except Exception:
        curl_headers = [f"Referer: {HKEX_DAYQUOT_REFERER}", "Accept-Language: en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7"]
        try:
            html = _curl_get_text(url, timeout=30, http1=True, extra_headers=curl_headers)
            _debug_save(f"hkex_dayquot_{yymmdd}_curl.html", html)
            return html
        except Exception:
            html = _curl_get_text(url, timeout=30, insecure=True, http1=True, extra_headers=curl_headers)
            _debug_save(f"hkex_dayquot_{yymmdd}_curl_insecure.html", html)
            return html

def parse_hkex_turnover_hkd(html: str) -> float:
    rx = r"Total\s+Market\s+Turnover\s*\(\s*HK\$\s*Million\s*\)[^0-9]{0,300}([0-9][0-9,]*\.?[0-9]*)"
    m = re.search(rx, html, flags=re.I)
    if m:
        v = float(m.group(1).replace(",", ""))
        return v * 1_000_000.0
    # fallback table
    tables = pd.read_html(StringIO(html))
    for df in tables:
        df = df.fillna("")
        cols = [str(c).strip() for c in df.columns]
        if not cols:
            continue
        idx = None
        for i, c in enumerate(cols):
            if "Turnover" in c or "成交額" in c or "成交金額" in c:
                idx = i
                break
        if idx is None:
            continue
        col = df.iloc[:, idx].tolist()
        nums = [_to_float(x) for x in col if _to_float(x) is not None]
        if nums:
            v = float(nums[-1])
            # heuristic: HK$ million
            if v >= 10000:
                return v * 1_000_000.0
            return v
    raise RuntimeError("HKEX 找不到 Turnover")

def fetch_aastocks_hsi_html() -> str:
    r = _SESSION.get(AASTOCKS_HSI_URL, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    html = r.text or ""
    _debug_save("aastocks_hsi.html", html)
    return html

def parse_aastocks_turnover_yi(html: str) -> float:
    m = re.search(r"(?:成交額|Turnover)[^0-9]{0,50}([0-9][0-9,]*\.?[0-9]*)\s*([BbMm億])", html, flags=re.I)
    if not m:
        raise RuntimeError("AASTOCKS 找不到成交額")
    num = float(m.group(1).replace(",", ""))
    unit = m.group(2)
    if unit == "億":
        return round(num, 2)
    if unit in ("B", "b"):
        return round(num * 10.0, 2)
    if unit in ("M", "m"):
        return round(num / 100.0, 2)
    return round(num, 2)

def normalize_hk_turnover_to_yi(val):
    if val is None:
        return None
    x = float(val)
    if x < 0:
        x = abs(x)
    # HKD amount -> yi
    if x >= 1e7:
        x = x / 1e8
    # too big for yi -> treat as million
    if x >= 50000:
        x = x / 1e6
    return round(x, 2)

def hk_turnover_two_days(hsi_today_dt, hsi_prev_dt):
    out_today = None
    out_prev = None
    try:
        d = hsi_today_dt.to_pydatetime() if isinstance(hsi_today_dt, pd.Timestamp) else hsi_today_dt
        hkd = parse_hkex_turnover_hkd(fetch_hkex_dayquot_html(d))
        out_today = normalize_hk_turnover_to_yi(hkd)
    except Exception:
        try:
            out_today = normalize_hk_turnover_to_yi(parse_aastocks_turnover_yi(fetch_aastocks_hsi_html()))
        except Exception:
            out_today = None

    try:
        d = hsi_prev_dt.to_pydatetime() if isinstance(hsi_prev_dt, pd.Timestamp) else hsi_prev_dt
        hkd = parse_hkex_turnover_hkd(fetch_hkex_dayquot_html(d))
        out_prev = normalize_hk_turnover_to_yi(hkd)
    except Exception:
        out_prev = None

    return out_today, out_prev

def hk_turnover_scan_prev(base_dt: datetime, max_back_days: int = 10) -> float | None:
    # Scan backwards to find a valid previous trading day turnover
    for i in range(1, max_back_days + 1):
        d = base_dt - timedelta(days=i)
        try:
            hkd = parse_hkex_turnover_hkd(fetch_hkex_dayquot_html(d))
            yi = normalize_hk_turnover_to_yi(hkd)
            if yi is not None:
                return yi
        except Exception:
            continue
    return None


# ========= Find rows =========
def find_stock_rows_from_sheet(col_a: list[str], col_b: list[str]):
    tw_header, hk_header = None, None
    n = max(len(col_a), len(col_b))
    for i in range(n):
        a = (col_a[i] if i < len(col_a) else "") or ""
        b = (col_b[i] if i < len(col_b) else "") or ""
        v = f"{a} {b}".strip()
        if "台股" in v and "台幣" in v:
            tw_header = i + 1
        if "港股" in v and "港幣" in v:
            hk_header = i + 1
    if tw_header is None or hk_header is None or hk_header <= tw_header:
        raise RuntimeError("找不到『台股（台幣）』或『港股（港幣）』標題列，請確認分頁版面")

    tw_rows = []
    for r in range(tw_header + 1, hk_header):
        if r - 1 >= len(col_a):
            break
        code = (col_a[r - 1] or "").strip()
        if code == "":
            break
        if code.isdigit():
            tw_rows.append((r, code))

    hk_rows = []
    for r in range(hk_header + 1, hk_header + 1 + 80):
        if r - 1 >= len(col_a):
            break
        code = (col_a[r - 1] or "").strip()
        if code == "":
            break
        code = code.replace(".HK", "").replace("HK", "").strip()
        if code.isdigit():
            hk_rows.append((r, code.zfill(4)))

    return tw_rows, hk_rows


# ========= Revenue (營收) =========
REV_TAB_DEFAULT = "營收"
MOPSFIN_LISTED_CSV = "https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv"
MOPSFIN_OTC_CSV    = "https://mopsfin.twse.com.tw/opendata/t187ap05_O.csv"

def _ym_add(year: int, month: int, delta_months: int) -> tuple[int, int]:
    y, m = year, month
    m = m + delta_months
    while m <= 0:
        y -= 1
        m += 12
    while m >= 13:
        y += 1
        m -= 12
    return y, m

def _ym_label(year: int, month: int) -> str:
    return f"{year}/{month:02d}月"

def _parse_ym_any(v) -> tuple[int, int] | None:
    if v is None:
        return None
    s = re.sub(r"\D", "", str(v))
    if not s:
        return None
    # ROC yyyMM (5 digits) or AD yyyyMM (6 digits)
    if len(s) == 5:
        roc_y = int(s[:3])
        m = int(s[3:])
        return (roc_y + 1911, m)
    if len(s) >= 6:
        y = int(s[:4])
        m = int(s[4:6])
        if 1 <= m <= 12:
            return (y, m)
    return None

def _find_colname(cols: list[str], includes: list[str], excludes: list[str] = None) -> str | None:
    excludes = excludes or []
    for c in cols:
        cs = str(c)
        if all(k in cs for k in includes) and not any(x in cs for x in excludes):
            return c
    return None

def _download_csv_to_df(url: str) -> pd.DataFrame:
    r = _SESSION.get(url, timeout=40)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig", errors="ignore")
    return pd.read_csv(StringIO(text), dtype=str)

def fetch_monthly_revenue_maps() -> tuple[tuple[int, int] | None, dict]:
    """
    Return: (dataset_ym, revenue_map)
    revenue_map[code] = {"this": float, "last_year": float, "last_month": float}
    Unit: NTD thousand (仟元) as provided by dataset.
    """
    frames = []
    for url in (MOPSFIN_LISTED_CSV, MOPSFIN_OTC_CSV):
        try:
            df = _download_csv_to_df(url)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception:
            continue

    if not frames:
        return None, {}

    df = pd.concat(frames, ignore_index=True)
    cols = list(df.columns)

    code_col = _find_colname(cols, ["公司", "代號"]) or _find_colname(cols, ["證券", "代號"]) or _find_colname(cols, ["公司代碼"])
    ym_col   = _find_colname(cols, ["資料", "年月"]) or _find_colname(cols, ["資料年月"]) or _find_colname(cols, ["年月"])

    this_col = _find_colname(cols, ["當月營收"], excludes=["累計"])
    lastm_col= _find_colname(cols, ["上月營收"], excludes=["累計"])
    lasty_col= _find_colname(cols, ["去年當月營收"], excludes=["累計"]) or _find_colname(cols, ["去年同期營收"], excludes=["累計"])

    if not code_col or not this_col or not lastm_col or not lasty_col:
        return None, {}

    dataset_ym = None
    if ym_col:
        yms = []
        for v in df[ym_col].dropna().tolist():
            p = _parse_ym_any(v)
            if p:
                yms.append(p)
        if yms:
            dataset_ym = sorted(set(yms))[-1]

    rev_map = {}
    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).strip()
        code = re.sub(r"\D", "", code)
        if not code:
            continue
        this_v = _to_float(row.get(this_col))
        lasty_v= _to_float(row.get(lasty_col))
        lastm_v= _to_float(row.get(lastm_col))
        if this_v is None and lasty_v is None and lastm_v is None:
            continue
        rev_map[code] = {"this": this_v, "last_year": lasty_v, "last_month": lastm_v}

    return dataset_ym, rev_map

def find_revenue_rows_from_sheet(col_a: list[str]):
    # Revenue sheet layout: row2 is header, data starts row3; col A has codes.
    rows = []
    for r in range(3, 260 + 1):
        idx = r - 1
        if idx >= len(col_a):
            break
        v = (col_a[idx] or "").strip()
        if v == "":
            break
        # might be like "2926.0" if read as float
        v2 = re.sub(r"[^\d]", "", v)
        if v2.isdigit():
            rows.append((r, v2))
    return rows

def update_revenue_tab(svc, sheet_id: str):
    tab = os.getenv("GSHEET_TAB_REVENUE", REV_TAB_DEFAULT).strip() or REV_TAB_DEFAULT
    tab_q = f"'{tab}'" if re.search(r"[^A-Za-z0-9_]", tab) else tab

    ab = get_values(svc, sheet_id, f"{tab_q}!A1:B260")
    col_a = [row[0] if len(row) > 0 else "" for row in ab]

    rows = find_revenue_rows_from_sheet(col_a)

    # Decide the reference month (usually previous month of today), but align to dataset month if available.
    now = _today_taipei()
    exp_y, exp_m = _ym_add(now.year, now.month, -1)

    dataset_ym, rev_map = fetch_monthly_revenue_maps()
    use_y, use_m = (dataset_ym if dataset_ym else (exp_y, exp_m))

    # Headers:
    # C2 = use_y/use_m
    # D2 = use_y-1/use_m
    # F2 = use_y/use_m - 1 month
    y_ly, m_ly = use_y - 1, use_m
    y_lm, m_lm = _ym_add(use_y, use_m, -1)

    updates = []
    updates.append((f"{tab_q}!C2", [[_ym_label(use_y, use_m)]]))
    updates.append((f"{tab_q}!D2", [[_ym_label(y_ly, m_ly)]]))
    updates.append((f"{tab_q}!F2", [[_ym_label(y_lm, m_lm)]]))

    for r, code in rows:
        d = rev_map.get(code, {})
        updates.append((f"{tab_q}!C{r}", [[d.get("this")]]))
        updates.append((f"{tab_q}!D{r}", [[d.get("last_year")]]))
        updates.append((f"{tab_q}!F{r}", [[d.get("last_month")]]))

    if updates:
        batch_update_values(svc, sheet_id, updates, value_input="USER_ENTERED")

    print(f"Revenue tab updated: {tab} | month={use_y}-{use_m:02d} | rows={len(rows)}")


# ========= Main =========
TICKER_TWII = "^TWII"
TICKER_HSI  = "^HSI"

def main():
    svc, sheet_id, tab = gsheet_service()
    tab_q = f"'{tab}'" if re.search(r"[^A-Za-z0-9_]", tab) else tab

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
    tw_today_yi = twse_turnover_yi(tw_t_dt)
    tw_prev_yi  = twse_turnover_yi(tw_p_dt)

    hk_today_yi, hk_prev_yi = hk_turnover_two_days(hsi.get("t_date"), hsi.get("p_date"))

    # If HK prev is missing:
    if hk_prev_yi is None:
        if (not first_run) and (old_hk_today is not None):
            # right-shift from last run
            hk_prev_yi = _round2(old_hk_today)
        else:
            # first run => scan back to find a valid previous trading day
            base_dt = (hsi.get("t_date").to_pydatetime() if isinstance(hsi.get("t_date"), pd.Timestamp) else _today_taipei())
            hk_prev_yi = hk_turnover_scan_prev(base_dt, max_back_days=10)

    # TW stocks
    tw_codes = [code for _, code in tw_rows]
    tw_today_map, tw_prev_map = tw_price_pack_for_codes(tw_codes, tw_t_dt, tw_p_dt)

    # HK stocks via yfinance
    hk_codes = [code for _, code in hk_rows]
    hk_tickers = [f"{int(c):04d}.HK" for c in hk_codes]
    hk_stock_map = {}
    for tkr in hk_tickers:
        try:
            h = hist_one(tkr)
        except Exception:
            h = pd.DataFrame()
        if h is None or h.empty or "Close" not in h.columns or h["Close"].dropna().shape[0] < 2:
            hk_stock_map[tkr] = {}
            continue
        t_date, t_close, p_date, p_close = last_two(h["Close"])

        def _last(col):
            if col not in h.columns:
                return None
            s = h[col].dropna()
            return float(s.iloc[-1]) if len(s) else None

        hk_stock_map[tkr] = {
            "close": t_close,
            "prev_close": p_close,
            "open": _last("Open"),
            "high": _last("High"),
            "low":  _last("Low"),
            "volume": _last("Volume"),
        }
        time.sleep(0.2)

    # build updates
    updates = []
    now = _today_taipei()
    updates.append((f"{tab_q}!L3", [[now.strftime("%Y-%m-%d %H:%M:%S")]]))

    updates.append((f"{tab_q}!D6", [[_round2(twii.get("close"))]]))
    updates.append((f"{tab_q}!E6", [[_round2(twii.get("prev_close"))]]))
    updates.append((f"{tab_q}!H6", [[tw_today_yi]]))
    updates.append((f"{tab_q}!I6", [[tw_prev_yi]]))

    updates.append((f"{tab_q}!D8", [[_round2(hsi.get("close"))]]))
    updates.append((f"{tab_q}!E8", [[_round2(hsi.get("prev_close"))]]))
    updates.append((f"{tab_q}!H8", [[hk_today_yi]]))
    updates.append((f"{tab_q}!I8", [[hk_prev_yi]]))

    # TW rows D,E,H,I,J,K
    for r, code in tw_rows:
        t = tw_today_map.get(code, {})
        p = tw_prev_map.get(code, {})
        close = _round2(t.get("close"))
        prev  = _round2(p.get("close")) if isinstance(p, dict) else None
        opn   = _round2(t.get("open"))
        low   = _round2(t.get("low"))
        high  = _round2(t.get("high"))
        vol   = t.get("volume")
        lots = None
        if vol is not None:
            try:
                lots = int(round(float(vol) / 1000.0))
            except Exception:
                lots = None

        updates.append((f"{tab_q}!D{r}", [[close]]))
        updates.append((f"{tab_q}!E{r}", [[prev]]))
        updates.append((f"{tab_q}!H{r}", [[opn]]))
        updates.append((f"{tab_q}!I{r}", [[low]]))
        updates.append((f"{tab_q}!J{r}", [[high]]))
        updates.append((f"{tab_q}!K{r}", [[lots]]))

    # HK rows D,E,H,I,J,K
    for (r, code), tkr in zip(hk_rows, hk_tickers):
        d = hk_stock_map.get(tkr, {})
        close = _round2(d.get("close"))
        prev  = _round2(d.get("prev_close"))
        opn   = _round2(d.get("open"))
        low   = _round2(d.get("low"))
        high  = _round2(d.get("high"))
        vol   = d.get("volume")
        hands = None
        lot_size = 1000
        if vol is not None:
            try:
                hands = int(round(float(vol) / lot_size))
            except Exception:
                hands = None

        updates.append((f"{tab_q}!D{r}", [[close]]))
        updates.append((f"{tab_q}!E{r}", [[prev]]))
        updates.append((f"{tab_q}!H{r}", [[opn]]))
        updates.append((f"{tab_q}!I{r}", [[low]]))
        updates.append((f"{tab_q}!J{r}", [[high]]))
        updates.append((f"{tab_q}!K{r}", [[hands]]))

    batch_update_values(svc, sheet_id, updates, value_input="USER_ENTERED")

    # NEW: update Revenue tab
    update_revenue_tab(svc, sheet_id)

    print("DONE: updated Google Sheet")
    print(f"TW rows: {len(tw_rows)} | HK rows: {len(hk_rows)}")
    print(f"TW dates: {tw_t_dt.date()} / {tw_p_dt.date()}")
    print(f"TWII turnover (today/prev, 億元): {tw_today_yi} / {tw_prev_yi}")
    print(f"HK turnover (today/prev, 億港幣): {hk_today_yi} / {hk_prev_yi}")
    print(f"first_run={first_run}")


if __name__ == "__main__":
    main()
