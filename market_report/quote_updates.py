import time
import pandas as pd


def _normalize_history_index(history):
    h = history.copy()
    idx = h.index
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    h.index = pd.to_datetime(idx).normalize()
    return h


def _date_key(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    return pd.Timestamp(value.date())


def fetch_hk_stock_map(hk_tickers: list[str], *, hist_one, last_two=None, expected_today=None, expected_prev=None) -> dict:
    stock_map = {}
    expected_today_key = _date_key(expected_today)
    expected_prev_key = _date_key(expected_prev)

    for ticker in hk_tickers:
        today_key = expected_today_key
        prev_key = expected_prev_key
        try:
            history = hist_one(ticker)
        except Exception:
            history = None
        if history is None or history.empty or "Close" not in history.columns or history["Close"].dropna().shape[0] < 2:
            stock_map[ticker] = {}
            continue

        h = _normalize_history_index(history)

        if today_key is not None:
            today_row = h.loc[h.index == today_key]
            if today_row.empty:
                stock_map[ticker] = {}
                continue
            today = today_row.iloc[-1]
        else:
            today = h.iloc[-1]
            today_key = h.index[-1]

        if prev_key is not None:
            prev_row = h.loc[h.index == prev_key]
            if prev_row.empty:
                older = h.loc[h.index < today_key]
                prev = older.iloc[-1] if not older.empty else None
            else:
                prev = prev_row.iloc[-1]
        else:
            older = h.loc[h.index < today_key]
            prev = older.iloc[-1] if not older.empty else None

        if prev is None:
            stock_map[ticker] = {}
            continue

        close = float(today.get("Close")) if pd.notna(today.get("Close")) else None
        prev_close = float(prev.get("Close")) if pd.notna(prev.get("Close")) else None

        def _today_value(col: str):
            if col not in h.columns:
                return None
            value = today.get(col)
            return float(value) if pd.notna(value) else None

        stock_map[ticker] = {
            "close": close,
            "prev_close": prev_close,
            "open": _today_value("Open"),
            "high": _today_value("High"),
            "low": _today_value("Low"),
            "volume": _today_value("Volume"),
            "quote_date": str(today_key.date()) if today_key is not None else None,
        }
        time.sleep(0.2)
    return stock_map


def build_tw_stock_updates(tab_q: str, tw_rows: list[tuple[int, str]], tw_today_map: dict, tw_prev_map: dict, *, round_price) -> tuple[list[tuple[str, list[list]]], list[str]]:
    updates: list[tuple[str, list[list]]] = []
    missing_volume_codes: list[str] = []
    for row_no, code in tw_rows:
        today_data = tw_today_map.get(code, {})
        prev_data = tw_prev_map.get(code, {})
        if not today_data and not prev_data:
            continue
        close = round_price(today_data.get("close"))
        prev = round_price(prev_data.get("close")) if isinstance(prev_data, dict) else None
        if close is None:
            continue
        open_price = round_price(today_data.get("open"))
        low = round_price(today_data.get("low"))
        high = round_price(today_data.get("high"))
        volume = today_data.get("volume")

        lots = None
        if volume is not None:
            try:
                lots = int(round(float(volume) / 1000.0))
            except Exception:
                lots = None
        else:
            # 台股成交量缺失：明確寫 N/A 並記錄清單
            lots = "N/A"
            missing_volume_codes.append(code)

        # 無成交日：成交張數應為 0，且開高低以 0 呈現（避免沿用外部來源殘值）
        if lots == 0:
            open_price = 0
            low = 0
            high = 0

        updates.extend(
            [
                (f"{tab_q}!D{row_no}", [[close]]),
                (f"{tab_q}!E{row_no}", [[prev]]),
                (f"{tab_q}!H{row_no}", [[open_price]]),
                (f"{tab_q}!I{row_no}", [[low]]),
                (f"{tab_q}!J{row_no}", [[high]]),
                (f"{tab_q}!K{row_no}", [[lots]]),
            ]
        )
    return updates, missing_volume_codes


def build_hk_stock_updates(
    tab_q: str,
    hk_rows: list[tuple[int, str]],
    hk_tickers: list[str],
    hk_stock_map: dict,
    *,
    round_price,
    hk_hands_from_aastocks,
) -> list[tuple[str, list[list]]]:
    updates: list[tuple[str, list[list]]] = []
    for (row_no, code), ticker in zip(hk_rows, hk_tickers):
        data = hk_stock_map.get(ticker, {})
        if not data:
            continue
        close = round_price(data.get("close"))
        prev = round_price(data.get("prev_close"))
        if close is None:
            continue
        open_price = round_price(data.get("open"))
        low = round_price(data.get("low"))
        high = round_price(data.get("high"))

        if code in ("03368", "00825"):
            hands = hk_hands_from_aastocks(code)
            if hands is None:
                lot_size_map = {"03368": 500, "00825": 1000}
                volume = data.get("volume")
                try:
                    lot_size = lot_size_map.get(code)
                    hands = int(float(volume) // lot_size) if (volume is not None and lot_size) else None
                except Exception:
                    hands = None
        else:
            hands = hk_hands_from_aastocks(code)
            if hands is None:
                volume = data.get("volume")
                try:
                    hands = int(round(float(volume) / 1000.0)) if volume is not None else None
                except Exception:
                    hands = None

        updates.extend(
            [
                (f"{tab_q}!D{row_no}", [[close]]),
                (f"{tab_q}!E{row_no}", [[prev]]),
                (f"{tab_q}!H{row_no}", [[open_price]]),
                (f"{tab_q}!I{row_no}", [[low]]),
                (f"{tab_q}!J{row_no}", [[high]]),
                (f"{tab_q}!K{row_no}", [[hands]]),
            ]
        )
    return updates
