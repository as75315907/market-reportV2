from datetime import datetime

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


def parse_tpex_st43(obj: dict, *, to_float) -> dict | None:
    data = obj.get("aaData") or obj.get("data") or None
    if not isinstance(data, list) or not data:
        return None
    row = data[0]
    if not isinstance(row, list) or len(row) < 8:
        return None
    close = to_float(row[2])
    if close is None:
        return None
    return {
        "open": to_float(row[4]),
        "high": to_float(row[5]),
        "low": to_float(row[6]),
        "close": close,
        "volume": to_float(row[7]),
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
        for suffix in ("TW", "TWO"):
            ticker = f"{code}.{suffix}"
            try:
                history = hist_one(ticker)
                if history is None or history.empty or history["Close"].dropna().shape[0] < 2:
                    continue
                _, t_close, _, p_close = last_two(history["Close"])
                if code not in today_map:
                    today_map[code] = {
                        "open": float(history["Open"].dropna().iloc[-1]) if "Open" in history else None,
                        "high": float(history["High"].dropna().iloc[-1]) if "High" in history else None,
                        "low": float(history["Low"].dropna().iloc[-1]) if "Low" in history else None,
                        "close": t_close,
                        "volume": float(history["Volume"].dropna().iloc[-1]) if "Volume" in history else None,
                    }
                if code not in prev_map:
                    prev_map[code] = {"close": p_close}
                break
            except Exception:
                continue

    return today_map, prev_map
