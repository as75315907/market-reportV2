import time


def fetch_hk_stock_map(hk_tickers: list[str], *, hist_one, last_two) -> dict:
    stock_map = {}
    for ticker in hk_tickers:
        try:
            history = hist_one(ticker)
        except Exception:
            history = None
        if history is None or history.empty or "Close" not in history.columns or history["Close"].dropna().shape[0] < 2:
            stock_map[ticker] = {}
            continue

        _, close, _, prev_close = last_two(history["Close"])

        def _last(col: str):
            if col not in history.columns:
                return None
            series = history[col].dropna()
            return float(series.iloc[-1]) if len(series) else None

        stock_map[ticker] = {
            "close": close,
            "prev_close": prev_close,
            "open": _last("Open"),
            "high": _last("High"),
            "low": _last("Low"),
            "volume": _last("Volume"),
        }
        time.sleep(0.2)
    return stock_map


def build_tw_stock_updates(tab_q: str, tw_rows: list[tuple[int, str]], tw_today_map: dict, tw_prev_map: dict, *, round_price) -> list[tuple[str, list[list]]]:
    updates: list[tuple[str, list[list]]] = []
    for row_no, code in tw_rows:
        today_data = tw_today_map.get(code, {})
        prev_data = tw_prev_map.get(code, {})
        close = round_price(today_data.get("close"))
        prev = round_price(prev_data.get("close")) if isinstance(prev_data, dict) else None
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
    return updates


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
        close = round_price(data.get("close"))
        prev = round_price(data.get("prev_close"))
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
