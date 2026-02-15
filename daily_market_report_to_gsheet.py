# -*- coding: utf-8 -*-
import os
import json
import math
import time
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional

import pandas as pd
import yfinance as yf
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ============ 基本設定 ============
TICKER_TWII = "^TWII"
TICKER_HSI  = "^HSI"
TWSE_FMTQIK = "https://www.twse.com.tw/exchangeReport/FMTQIK"
HKEX_DAYQUOT = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{yymmdd}e.htm"

CACHE_FILE = "tw_suffix_cache.json"

# Google Sheet env
GSHEET_ID  = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "IR_updated (PC HOME)").strip()
GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})

# ============ 小工具 ============
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def hist_one(ticker: str) -> pd.DataFrame:
    return yf.Ticker(ticker).history(period="1mo", interval="1d", auto_adjust=False)

def has_enough_prices(hist: pd.DataFrame) -> bool:
    if hist is None or hist.empty:
        return False
    if "Close" not in hist.columns:
        return False
    return hist["Close"].dropna().shape[0] >= 2

def resolve_tw_ticker(code: str, cache: dict) -> str:
    code = str(code).strip()
    if code in cache:
        return f"{code}.{cache[code]}"

    for suf in ["TWO", "TW"]:
        t = f"{code}.{suf}"
        try:
            h = hist_one(t)
            if has_enough_prices(h):
                cache[code] = suf
                return t
        except Exception:
            continue

    cache[code] = "TW"
    return f"{code}.TW"

def hk_ticker(code: str) -> str:
    return f"{int(str(code)):04d}.HK"

def last_two(series: pd.Series):
    s = series.dropna()
    if len(s) < 2:
        return (pd.NaT, math.nan, pd.NaT, math.nan)
    return s.index[-1], float(s.iloc[-1]), s.index[-2], float(s.iloc[-2])

def build_ohlcv_map(ticker_list: List[str]) -> Dict[str, Dict[str, Any]]:
    out = {}
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
            "low":  _last("Low"),
            "volume": _last("Volume"),
        }
        time.sleep(0.2)
    return out

# ============ TWSE 成交金額 ============
def _ad_to_twse_date_str(dt: datetime) -> str:
    # TWSE FMTQIK 第一欄通常是民國日期：YYY/MM/DD
    roc = dt.year - 1911
    return f"{roc}/{dt.month:02d}/{dt.day:02d}"

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
    try:
        obj = fetch_fmtqik_month_json(dt)
        val = extract_turnover_from_fmtqik(obj, dt)
        if val is None:
            return None
        return round(val / 1e8, 2)
    except Exception:
        return None

# ============ HKEX 成交額（簡化版：抓網頁內的 “Turnover” 數字） ============
def _fetch_hkex_dayquot_html(dt: datetime) -> str:
    yymmdd = dt.strftime("%y%m%d")
    url = HKEX_DAYQUOT.format(yymmdd=yymmdd)
    r = _SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def _parse_hkex_turnover_from_html(html: str) -> Optional[float]:
    # 很粗但實務可用：抓 “Turnover” 後面最大的數字（單位通常為 HK$ million）
    # 你原本 fixhk6 有更完整邏輯；想要 100% 沿用也可以把那段搬過來。
    txt = re.sub(r"\s+", " ", html)
    m = re.search(r"Turnover[^0-9]{0,50}([0-9,]+\.[0-9]+|[0-9,]+)", txt, re.IGNORECASE)
    if not m:
        return None
    s = m.group(1).replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def hkex_turnover_yi(dt: datetime) -> Optional[float]:
    """
    回傳：億港幣
    HKEX dayquot 常見 turnover 單位是 HK$ million（百萬港幣）
    若抓到的是 million，轉成 億：million / 100
    """
    try:
        html = _fetch_hkex_dayquot_html(dt)
        val_million = _parse_hkex_turnover_from_html(html)
        if val_million is None:
            return None
        return round(val_million / 100.0, 2)
    except Exception:
        return None

def get_two_hk_turnover_by_dates(t_date, p_date) -> Tuple[Optional[float], Optional[float]]:
    def to_dt(x):
        if isinstance(x, pd.Timestamp):
            return x.to_pydatetime()
        if isinstance(x, datetime):
            return x
        return None
    dt1 = to_dt(t_date)
    dt2 = to_dt(p_date)
    return (hkex_turnover_yi(dt1) if dt1 else None, hkex_turnover_yi(dt2) if dt2 else None)

# ============ Google Sheets ============
def gsheet_service():
    if not (GSHEET_ID and GCP_SA_JSON):
        raise RuntimeError("缺少 GSHEET_ID 或 GCP_SA_JSON（請在 GitHub Secrets/Env 設定）")
    info = json.loads(GCP_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def a1(col: str, row: int) -> str:
    return f"{col}{row}"

def fetch_col_a_values(svc, max_rows=250):
    rng = f"'{GSHEET_TAB}'!A1:A{max_rows}"
    res = svc.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=rng).execute()
    vals = res.get("values", [])
    # 轉成 list[str]，空的補 ""
    out = []
    for i in range(max_rows):
        if i < len(vals) and len(vals[i]) > 0:
            out.append(str(vals[i][0]))
        else:
            out.append("")
    return out  # index 0 => row 1

def find_stock_rows_from_sheet(col_a: List[str]) -> Tuple[List[Tuple[int,str]], List[Tuple[int,str]]]:
    """
    依你 excel 模板的找法：先找「台股（台幣）」與「港股（港幣）」標題列，
    然後往下抓代碼直到遇到空白。
    """
    tw_start = None
    hk_start = None
    for idx, v in enumerate(col_a, start=1):
        s = str(v).strip()
        if tw_start is None and "台股" in s and "台幣" in s:
            tw_start = idx + 1
        if hk_start is None and "港股" in s and "港幣" in s:
            hk_start = idx + 1

    if tw_start is None or hk_start is None:
        raise RuntimeError("找不到『台股（台幣）』或『港股（港幣）』標題列，請確認分頁版面")

    tw_rows = []
    r = tw_start
    while r < hk_start:
        code = col_a[r-1].strip()
        if not code:
            break
        if code.isdigit():
            tw_rows.append((r, code))
        r += 1

    hk_rows = []
    r = hk_start
    for _ in range(0, 80):
        code = col_a[r-1].strip()
        if not code:
            break
        code = code.replace(".HK", "").replace("HK", "").strip()
        hk_rows.append((r, code))
        r += 1

    return tw_rows, hk_rows

def batch_update(svc, updates: List[Tuple[str, Any]]):
    """
    updates: [(A1, value), ...]
    """
    data = []
    for cell, val in updates:
        # Google Sheets 的 datetime 建議直接寫字串，避免時區顯示亂掉
        if isinstance(val, datetime):
            val = val.strftime("%Y-%m-%d %H:%M:%S")
        data.append({"range": f"'{GSHEET_TAB}'!{cell}", "values": [[val]]})

    body = {"valueInputOption": "USER_ENTERED", "data": data}
    svc.spreadsheets().values().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()


# ========= 台股個股 OHLCV（不用 Yahoo，避免 GitHub Actions 404） =========
# 上市：TWSE STOCK_DAY
TWSE_STOCK_DAY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
# 上櫃：TPEx st43_result (json)
TPEX_STOCK_DAY = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

def _roc_to_ad_date_str(roc_date: str) -> str | None:
    """'115/02/15' -> '2026-02-15'"""
    s = str(roc_date).strip()
    m = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", s)
    if not m:
        return None
    y = int(m.group(1)) + 1911
    mm = int(m.group(2))
    dd = int(m.group(3))
    return f"{y:04d}-{mm:02d}-{dd:02d}"

def _num_to_float(x):
    s = str(x).strip().replace(",", "")
    if s in ("", "--", "NaN", "nan", "None"):
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def _num_to_int(x):
    v = _num_to_float(x)
    if v is None:
        return None
    try:
        return int(round(v))
    except Exception:
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_twse_stock_month(code: str, yyyymmdd: str) -> dict:
    # yyyymmdd: '20260201'
    params = {"response": "json", "date": yyyymmdd, "stockNo": str(code).strip()}
    r = _SESSION.get(TWSE_STOCK_DAY, params=params, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_tpex_stock_month(code: str, roc_y_mm: str) -> dict:
    # roc_y_mm: '115/02'
    params = {"l": "zh-tw", "d": roc_y_mm, "stkno": str(code).strip()}
    r = _SESSION.get(TPEX_STOCK_DAY, params=params, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.json()

def _collect_twse_rows(obj: dict) -> list[dict]:
    out = []
    data = obj.get("data", [])
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, list) or len(row) < 7:
            continue
        ad = _roc_to_ad_date_str(row[0])
        if not ad:
            continue
        vol = _num_to_int(row[1])  # 成交股數
        opn = _num_to_float(row[3])
        high = _num_to_float(row[4])
        low = _num_to_float(row[5])
        close = _num_to_float(row[6])
        if close is None:
            continue
        out.append({"date": ad, "open": opn, "high": high, "low": low, "close": close, "volume": vol})
    return out

def _collect_tpex_rows(obj: dict) -> list[dict]:
    out = []
    data = obj.get("aaData") or obj.get("data") or []
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, list) or len(row) < 7:
            continue
        ad = _roc_to_ad_date_str(row[0])
        if not ad:
            continue
        vol = _num_to_int(row[1])  # 成交股數
        opn = _num_to_float(row[3])
        high = _num_to_float(row[4])
        low = _num_to_float(row[5])
        close = _num_to_float(row[6])
        if close is None:
            continue
        out.append({"date": ad, "open": opn, "high": high, "low": low, "close": close, "volume": vol})
    return out

def tw_stock_last_two(code: str, ref_dt: datetime) -> dict:
    """回傳 dict: {t_date, p_date, close, prev_close, open, high, low, volume}
    - 先試 TWSE（上市），無資料再試 TPEx（上櫃）
    - 會抓「本月 + 上月」避免月初抓不到前一交易日
    """
    code = str(code).strip()

    cur = ref_dt
    prev = (ref_dt.replace(day=1) - timedelta(days=1))

    def yyyymm_first(d: datetime) -> str:
        return d.strftime("%Y%m") + "01"

    def roc_y_mm(d: datetime) -> str:
        return f"{d.year - 1911:03d}/{d.month:02d}"

    rows = []
    # --- TWSE ---
    try:
        obj_prev = fetch_twse_stock_month(code, yyyymm_first(prev))
        obj_cur  = fetch_twse_stock_month(code, yyyymm_first(cur))
        rows = _collect_twse_rows(obj_prev) + _collect_twse_rows(obj_cur)
    except Exception:
        rows = []

    # --- TPEx ---
    if len(rows) < 2:
        try:
            obj_prev = fetch_tpex_stock_month(code, roc_y_mm(prev))
            obj_cur  = fetch_tpex_stock_month(code, roc_y_mm(cur))
            rows = _collect_tpex_rows(obj_prev) + _collect_tpex_rows(obj_cur)
        except Exception:
            rows = []

    if len(rows) < 2:
        return {}

    rows = sorted(rows, key=lambda x: x["date"])
    t = rows[-1]
    p = rows[-2]

    t_date = pd.to_datetime(t["date"])
    p_date = pd.to_datetime(p["date"])

    return {
        "t_date": t_date,
        "p_date": p_date,
        "close": float(t["close"]),
        "prev_close": float(p["close"]),
        "open": float(t["open"]) if t["open"] is not None else math.nan,
        "high": float(t["high"]) if t["high"] is not None else math.nan,
        "low":  float(t["low"])  if t["low"]  is not None else math.nan,
        "volume": float(t["volume"]) if t["volume"] is not None else math.nan,
    }

def build_tw_stock_map(codes: list[str], ref_dt: datetime) -> dict:
    out = {}
    for c in codes:
        try:
            out[str(c).strip()] = tw_stock_last_two(str(c).strip(), ref_dt)
        except Exception:
            out[str(c).strip()] = {}
        time.sleep(0.2)
    return out

def main():
    svc = gsheet_service()

    col_a = fetch_col_a_values(svc, max_rows=260)
    tw_rows, hk_rows = find_stock_rows_from_sheet(col_a)

    tw_codes = [code for _, code in tw_rows]
    hk_codes = []
    for _, code in hk_rows:
        c = str(code).strip().replace(".HK", "")
        c = c.zfill(4)
        hk_codes.append(c)

    # HK stocks & indices still use yfinance; TW stocks use TWSE/TPEx official endpoints (avoid Yahoo 404)
    hk_tickers = [hk_ticker(c) for c in hk_codes]

    tw_map  = build_tw_stock_map(tw_codes, now)
    hk_map  = build_ohlcv_map(hk_tickers)
    idx_map = build_ohlcv_map([TICKER_TWII, TICKER_HSI])


    now = datetime.now()

    # ===== Indices =====
    twii = idx_map.get(TICKER_TWII, {})
    hsi  = idx_map.get(TICKER_HSI, {})

    updates = []
    # L3：時間戳
    updates.append(("L3", now))

    # D6/E6：TWII close/prev
    updates.append(("D6", _round2(twii.get("close"))))
    updates.append(("E6", _round2(twii.get("prev_close"))))

    # D8/E8：HSI close/prev
    updates.append(("D8", _round2(hsi.get("close"))))
    updates.append(("E8", _round2(hsi.get("prev_close"))))

    # H6/I6：台股成交額（億元），用 TWII 的交易日去對齊
    tw_t_date = twii.get("t_date")
    tw_p_date = twii.get("p_date")
    try:
        h = hist_one(TICKER_TWII)
        t_date, _, p_date, _ = last_two(h["Close"])
        tw_t_date, tw_p_date = t_date, p_date
    except Exception:
        pass

    tw_today_yi = twse_turnover_yi(tw_t_date.to_pydatetime()) if isinstance(tw_t_date, pd.Timestamp) else None
    tw_prev_yi  = twse_turnover_yi(tw_p_date.to_pydatetime()) if isinstance(tw_p_date, pd.Timestamp) else None
    updates.append(("H6", tw_today_yi))
    updates.append(("I6", tw_prev_yi))

    # H8/I8：港股成交額（億港幣）
    hk_today_yi, hk_prev_yi = get_two_hk_turnover_by_dates(hsi.get("t_date"), hsi.get("p_date"))
    updates.append(("H8", hk_today_yi))
    updates.append(("I8", hk_prev_yi))

    # ===== Stocks：只寫你原本要餵公式的欄位（D/E/H/I/J/K）=====
    # 台股：成交張數 = volume/1000
    for (r, code) in tw_rows:
        d = tw_map.get(code, {})
        close = _round2(d.get("close"))
        prev  = _round2(d.get("prev_close"))
        opn   = _round2(d.get("open"))
        low   = _round2(d.get("low"))
        high  = _round2(d.get("high"))
        vol   = d.get("volume")

        lots = None
        if not _isnan(vol):
            lots = int(round(float(vol) / 1000))

        updates += [
            (a1("D", r), close),
            (a1("E", r), prev),
            (a1("H", r), opn),
            (a1("I", r), low),
            (a1("J", r), high),
            (a1("K", r), lots),
        ]

    # 港股：這裡先用 “手數=volume/1000” 當保守預設（你若有 hk_lot.csv 也可再加回去）
    for (r, _raw), code, ticker in zip(hk_rows, hk_codes, hk_tickers):
        d = hk_map.get(ticker, {})
        close = _round2(d.get("close"))
        prev  = _round2(d.get("prev_close"))
        opn   = _round2(d.get("open"))
        low   = _round2(d.get("low"))
        high  = _round2(d.get("high"))
        vol   = d.get("volume")

        hands = None
        if not _isnan(vol):
            hands = int(round(float(vol) / 1000))

        updates += [
            (a1("D", r), close),
            (a1("E", r), prev),
            (a1("H", r), opn),
            (a1("I", r), low),
            (a1("J", r), high),
            (a1("K", r), hands),
        ]

    # 一次寫回
    batch_update(svc, updates)

    print("DONE: updated Google Sheet")
    print(f"TW rows: {len(tw_rows)} | HK rows: {len(hk_rows)}")
    print(f"TWII turnover (today/prev, 億元): {tw_today_yi} / {tw_prev_yi}")
    print(f"HK turnover (today/prev, 億港幣): {hk_today_yi} / {hk_prev_yi}")

if __name__ == "__main__":
    main()