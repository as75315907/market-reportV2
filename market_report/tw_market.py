from datetime import datetime
import pandas as pd

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ModuleNotFoundError:
    def retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

    def stop_after_attempt(*args, **kwargs):
        return None

    def wait_exponential(*args, **kwargs):
        return None

TWSE_FMTQIK = "https://www.twse.com.tw/exchangeReport/FMTQIK"
TWSE_MI_INDEX = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
TPEX_ST43 = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
TPEX_OPENAPI_DAILY_CLOSE_QUOTES = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"


def ad_to_twse_date_str(dt: datetime) -> str:
    roc_year = dt.year - 1911
    return f"{roc_year:03d}/{dt.month:02d}/{dt.day:02d}"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_fmtqik_month_json(session, dt: datetime) -> dict:
    month_key = dt.strftime("%Y%m") + "01"
    params = {"response": "json", "date": month_key}
    response = session.get(TWSE_FMTQIK, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    stat = str(payload.get("stat", ""))
    if "沒有" in stat or "No data" in stat:
        raise RuntimeError(f"FMTQIK no data for {month_key}")
    return payload


def extract_turnover_from_fmtqik(obj: dict, dt: datetime) -> int | None:
    target = ad_to_twse_date_str(dt)
    data = obj.get("data", [])
    if not isinstance(data, list):
        return None

    fields = obj.get("fields", [])
    turnover_col = None
    if isinstance(fields, list):
        for idx, field in enumerate(fields):
            if "成交金額" in str(field):
                turnover_col = idx
                break

    for row in data:
        if not isinstance(row, list) or not row:
            continue
        if str(row[0]).strip() != target:
            continue
        if turnover_col is not None and turnover_col < len(row):
            value = str(row[turnover_col]).replace(",", "").strip()
            if value.isdigit():
                return int(value)
        for cell in reversed(row):
            value = str(cell).replace(",", "").strip()
            if value.isdigit():
                return int(value)
    return None


def twse_turnover_yi(session, dt: datetime) -> float | None:
    try:
        payload = fetch_fmtqik_month_json(session, dt)
        value = extract_turnover_from_fmtqik(payload, dt)
        if value is None:
            return None
        return round(value / 1e8, 2)
    except Exception:
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_mi_index(session, date_dt: datetime) -> dict:
    params = {"response": "json", "date": date_dt.strftime("%Y%m%d"), "type": "ALLBUT0999"}
    response = session.get(TWSE_MI_INDEX, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def pick_idx(fields: list[str], key_words: list[str]) -> int | None:
    for idx, field in enumerate(fields):
        field_str = str(field)
        if any(keyword in field_str for keyword in key_words):
            return idx
    return None


def parse_mi_index_map(obj: dict, *, to_float) -> dict:
    out = {}
    tables = obj.get("tables", [])
    if not isinstance(tables, list):
        return out
    for table in tables:
        fields = table.get("fields", [])
        data = table.get("data", [])
        if not isinstance(fields, list) or not isinstance(data, list):
            continue
        field_names = [str(item) for item in fields]
        i_code = 0
        i_open = pick_idx(field_names, ["開盤"])
        i_high = pick_idx(field_names, ["最高"])
        i_low = pick_idx(field_names, ["最低"])
        i_close = pick_idx(field_names, ["收盤"])
        i_vol = pick_idx(field_names, ["成交股數"])
        if i_open is None or i_high is None or i_low is None or i_close is None or i_vol is None:
            continue

        for row in data:
            if not isinstance(row, list) or len(row) <= max(i_vol, i_close, i_open, i_high, i_low, i_code):
                continue
            code = str(row[i_code]).strip()
            if not code.isdigit():
                continue
            close = to_float(row[i_close])
            if close is None:
                continue
            out[code] = {
                "open": to_float(row[i_open]),
                "high": to_float(row[i_high]),
                "low": to_float(row[i_low]),
                "close": close,
                "volume": to_float(row[i_vol]),
            }
    return out


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_tpex_st43(session, code: str, date_dt: datetime) -> dict | None:
    params = {"l": "zh-tw", "o": "json", "d": ad_to_twse_date_str(date_dt), "stkno": str(code).strip()}
    response = session.get(TPEX_ST43, params=params, timeout=30)
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except Exception:
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch_tpex_openapi_daily_close_quotes(session) -> list[dict]:
    response = session.get(TPEX_OPENAPI_DAILY_CLOSE_QUOTES, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return payload
    return []


def roc_compact_date_str(dt: datetime) -> str:
    roc_year = dt.year - 1911
    return f"{roc_year:03d}{dt.month:02d}{dt.day:02d}"


def parse_tpex_openapi_map(rows: list[dict], date_dt: datetime, *, to_float) -> dict:
    """OpenAPI /tpex_mainboard_daily_close_quotes -> {code: quote} for target date."""
    out: dict = {}
    target = roc_compact_date_str(date_dt)
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("Date", "")).strip() != target:
            continue
        code = str(row.get("SecuritiesCompanyCode", "")).strip()
        if not code.isdigit():
            continue
        out[code] = {
            "open": to_float(row.get("Open")),
            "high": to_float(row.get("High")),
            "low": to_float(row.get("Low")),
            "close": to_float(row.get("Close")),
            "volume": to_float(row.get("TradingShares")),
        }
    return out


def parse_tpex_st43(obj: dict, *, to_float) -> dict | None:
    """
    TPEx st43_result.php (個股日成交資訊) 常見欄位順序：
    [日期, 成交股數, 成交金額(仟元), 開盤, 最高, 最低, 收盤, 漲跌, 筆數]

    先前索引誤用會把「成交金額」當收盤價、把「筆數」當成交量。
    這裡改成正確欄位，且在「無成交」(收盤為 -/--) 時仍回傳 volume=0，
    讓上層不要 fallback 到 yfinance 抓到前一日資料。
    """
    data = obj.get("aaData") or obj.get("data") or None
    if not isinstance(data, list) or not data:
        return None

    row = data[0]
    if not isinstance(row, list) or len(row) < 7:
        return None

    volume = to_float(row[1])  # 成交股數
    open_price = to_float(row[3]) if len(row) > 3 else None
    high = to_float(row[4]) if len(row) > 4 else None
    low = to_float(row[5]) if len(row) > 5 else None
    close = to_float(row[6]) if len(row) > 6 else None

    # 若連成交股數都取不到，視為無效資料
    if volume is None and close is None and open_price is None and high is None and low is None:
        return None

    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def tw_price_pack_for_codes(
    codes: list[str],
    t_date: datetime,
    p_date: datetime,
    *,
    session,
    hist_one,
    last_two,
    to_float,
) -> tuple[dict, dict]:
    today_map, prev_map = {}, {}

    try:
        today_map = parse_mi_index_map(fetch_mi_index(session, t_date), to_float=to_float)
    except Exception:
        today_map = {}

    try:
        prev_map = parse_mi_index_map(fetch_mi_index(session, p_date), to_float=to_float)
    except Exception:
        prev_map = {}

    # TPEx OpenAPI 優先（較穩定），再退回舊 st43 endpoint
    try:
        openapi_rows = fetch_tpex_openapi_daily_close_quotes(session)
    except Exception:
        openapi_rows = []

    openapi_today_map = parse_tpex_openapi_map(openapi_rows, t_date, to_float=to_float)
    openapi_prev_map = parse_tpex_openapi_map(openapi_rows, p_date, to_float=to_float)

    for code in codes:
        if code not in today_map and code in openapi_today_map:
            today_map[code] = openapi_today_map[code]
        if code not in prev_map and code in openapi_prev_map:
            prev_map[code] = {"close": openapi_prev_map[code].get("close")}

    for code in codes:
        if code not in today_map:
            try:
                parsed = parse_tpex_st43(fetch_tpex_st43(session, code, t_date) or {}, to_float=to_float)
                if parsed:
                    today_map[code] = parsed
            except Exception:
                pass
        if code not in prev_map:
            try:
                parsed = parse_tpex_st43(fetch_tpex_st43(session, code, p_date) or {}, to_float=to_float)
                if parsed:
                    prev_map[code] = parsed
            except Exception:
                pass

    for code in codes:
        if code in today_map and code in prev_map:
            continue

        # 需求特例：2926（誠品生活）若交易所當日資料缺失，不用 yfinance 補當日，
        # 避免把錯日成交帶入（例如應為無成交卻抓到其他日期成交）。
        if code == "2926" and code not in today_map:
            continue

        for suffix in ("TW", "TWO"):
            ticker = f"{code}.{suffix}"
            try:
                history = hist_one(ticker)
                if history is None or history.empty or "Close" not in history.columns:
                    continue

                # yfinance fallback 必須對齊指定日期，不能直接取最後兩根K（會抓到錯日）
                h = history.copy()
                idx = h.index
                if getattr(idx, "tz", None) is not None:
                    # 保留交易所在地日期，不轉 UTC，避免日期整體往前一天
                    idx = idx.tz_localize(None)
                h.index = pd.to_datetime(idx).normalize()

                t_key = pd.Timestamp(t_date.date())
                p_key = pd.Timestamp(p_date.date())

                t_row = h.loc[h.index == t_key]
                p_row = h.loc[h.index == p_key]

                # 若指定當日沒有K棒（停牌/無成交/非交易日），不要用更早資料硬塞今日欄位
                if code not in today_map:
                    if t_row.empty:
                        continue
                    tr = t_row.iloc[-1]
                    today_map[code] = {
                        "open": float(tr.get("Open")) if pd.notna(tr.get("Open")) else None,
                        "high": float(tr.get("High")) if pd.notna(tr.get("High")) else None,
                        "low": float(tr.get("Low")) if pd.notna(tr.get("Low")) else None,
                        "close": float(tr.get("Close")) if pd.notna(tr.get("Close")) else None,
                        "volume": float(tr.get("Volume")) if pd.notna(tr.get("Volume")) else None,
                    }

                if code not in prev_map:
                    if not p_row.empty:
                        pr = p_row.iloc[-1]
                        prev_map[code] = {"close": float(pr.get("Close")) if pd.notna(pr.get("Close")) else None}
                    else:
                        # 指定前日缺值時，取「<= p_date 的最近一筆」而非直接用今日收盤，
                        # 避免把 prev_close 錯寫成今日 close。
                        older = h.loc[h.index <= p_key]
                        if not older.empty:
                            pr = older.iloc[-1]
                            prev_map[code] = {
                                "close": float(pr.get("Close")) if pd.notna(pr.get("Close")) else None
                            }
                        else:
                            t_close = today_map.get(code, {}).get("close") if isinstance(today_map.get(code), dict) else None
                            if t_close is not None:
                                prev_map[code] = {"close": t_close}
                break
            except Exception:
                continue

    # 需求特例：2926（誠品生活）若今日缺官方資料，強制視為無成交日
    # -> 開高低=0、成交量=0、今日收盤=前日收盤（若有）
    if "2926" in codes and "2926" not in today_map:
        p_close = None
        p_obj = prev_map.get("2926")
        if isinstance(p_obj, dict):
            p_close = p_obj.get("close")
        today_map["2926"] = {
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": p_close,
            "volume": 0.0,
        }

    # 無成交日保護：若今日有價格但成交量=0，且前日收盤缺值，
    # 以前日收盤=今日收盤，避免 fallback 來源帶入更早日期的收盤價。
    for code in codes:
        t = today_map.get(code)
        p = prev_map.get(code)
        if not isinstance(t, dict):
            continue
        try:
            vol = t.get("volume")
            close = t.get("close")
            vol_f = float(vol) if vol is not None else None
        except Exception:
            vol_f = None
            close = t.get("close")

        if vol_f == 0 and close is not None:
            if not isinstance(p, dict) or p.get("close") is None:
                prev_map[code] = {"close": close}

    return today_map, prev_map
