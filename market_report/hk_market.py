import re
import subprocess
from datetime import timedelta
from io import StringIO
import requests

HKEX_DAYQUOT = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{yymmdd}e.htm"
HKEX_DAYQUOT_REFERER = "https://www.hkex.com.hk/eng/stat/smstat/dayquot/qtn.asp"
AASTOCKS_HSI_URL = "https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI&o=0&p=&s=8&t=6"


def curl_get_text(url: str, *, timeout: int = 30, user_agent: str, insecure: bool = False, http1: bool = False, extra_headers=None) -> str:
    cmd = ["curl", "-L", "-s", "--max-time", str(timeout), "-A", user_agent]
    if http1:
        cmd.append("--http1.1")
    if extra_headers:
        for header in extra_headers:
            cmd += ["-H", header]
    if insecure:
        cmd.insert(1, "-k")
    cmd.append(url)
    return subprocess.check_output(cmd, text=True, encoding="utf-8", errors="ignore")


def hkex_yymmdd(dt) -> str:
    return dt.strftime("%y%m%d")


def fetch_hkex_dayquot_html(session, trade_dt, *, user_agent: str, debug_save) -> str:
    yymmdd = hkex_yymmdd(trade_dt)
    url = HKEX_DAYQUOT.format(yymmdd=yymmdd)
    headers = {"User-Agent": user_agent, "Referer": HKEX_DAYQUOT_REFERER}
    try:
        response = session.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        html = response.text or ""
        debug_save(f"hkex_dayquot_{yymmdd}_requests.html", html)
        return html
    except Exception:
        curl_headers = [f"Referer: {HKEX_DAYQUOT_REFERER}", "Accept-Language: en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7"]
        try:
            html = curl_get_text(url, timeout=30, user_agent=user_agent, http1=True, extra_headers=curl_headers)
            debug_save(f"hkex_dayquot_{yymmdd}_curl.html", html)
            return html
        except Exception:
            html = curl_get_text(url, timeout=30, user_agent=user_agent, insecure=True, http1=True, extra_headers=curl_headers)
            debug_save(f"hkex_dayquot_{yymmdd}_curl_insecure.html", html)
            return html


def parse_hkex_turnover_hkd(html: str, *, to_float) -> float:
    match = re.search(r"Total\s+Market\s+Turnover\s*\(\s*HK\$\s*Million\s*\)[^0-9]{0,300}([0-9][0-9,]*\.?[0-9]*)", html, flags=re.I)
    if match:
        value = float(match.group(1).replace(",", ""))
        return value * 1_000_000.0

    import pandas as pd

    tables = pd.read_html(StringIO(html))
    for df in tables:
        df = df.fillna("")
        cols = [str(col).strip() for col in df.columns]
        if not cols:
            continue
        turnover_idx = None
        for idx, col in enumerate(cols):
            if "Turnover" in col or "成交額" in col or "成交金額" in col:
                turnover_idx = idx
                break
        if turnover_idx is None:
            continue
        numbers = [to_float(item) for item in df.iloc[:, turnover_idx].tolist() if to_float(item) is not None]
        if numbers:
            value = float(numbers[-1])
            if value >= 10000:
                return value * 1_000_000.0
            return value
    raise RuntimeError("HKEX 找不到 Turnover")


def fetch_aastocks_hsi_html(session, *, user_agent: str, debug_save) -> str:
    response = session.get(AASTOCKS_HSI_URL, timeout=30, headers={"User-Agent": user_agent})
    response.raise_for_status()
    html = response.text or ""
    debug_save("aastocks_hsi.html", html)
    return html


def parse_aastocks_turnover_yi(html: str) -> float:
    match = re.search(r"(?:成交額|Turnover)[^0-9]{0,50}([0-9][0-9,]*\.?[0-9]*)\s*([BbMm億])", html, flags=re.I)
    if not match:
        raise RuntimeError("AASTOCKS 找不到成交額")
    number = float(match.group(1).replace(",", ""))
    unit = match.group(2)
    if unit == "億":
        return round(number, 2)
    if unit in ("B", "b"):
        return round(number * 10.0, 2)
    if unit in ("M", "m"):
        return round(number / 100.0, 2)
    return round(number, 2)


def hk_hands_from_aastocks(code: str, *, timeout: int = 20, user_agent: str) -> int | None:
    code = (code or "").strip()
    if not code:
        return None
    code = re.sub(r"\D", "", code)
    if len(code) == 4:
        code = code.zfill(5)
    if len(code) != 5:
        return None

    url = f"https://www.aastocks.com/tc/stocks/quote/detail-quote.aspx?symbol={code}"
    try:
        response = requests.get(url, timeout=timeout, headers={"User-Agent": user_agent})
        if response.status_code != 200:
            return None
        html = response.text
    except Exception:
        return None

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        label_node = soup.find(string=re.compile(r"成交量"))
        if label_node is not None:
            cell = label_node.parent
            while cell is not None and getattr(cell, "name", None) not in ("td", "th"):
                cell = cell.parent
            if cell is not None:
                nxt = cell.find_next("td")
                if nxt is not None:
                    text = nxt.get_text(" ", strip=True)
                    match = re.search(r"([0-9,]+)", text)
                    if match:
                        return int(match.group(1).replace(",", ""))

        page_text = soup.get_text(" ", strip=True)
        for pattern in [r"成交量\s*\(手\)\s*([0-9,]+)", r"成交量\s*([0-9,]+)\s*手"]:
            match = re.search(pattern, page_text)
            if match:
                return int(match.group(1).replace(",", ""))
    except Exception:
        pass

    for pattern in [r"成交量\s*\(手\)\s*</[^>]+>\s*<[^>]+>\s*([0-9,]+)\s*<", r"成交量\s*</[^>]+>\s*<[^>]+>\s*([0-9,]+)\s*<"]:
        match = re.search(pattern, html)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except Exception:
                return None
    return None


def normalize_hk_turnover_to_yi(val):
    if val is None:
        return None
    value = float(val)
    if value < 0:
        value = abs(value)
    if value >= 1e7:
        value = value / 1e8
    if value >= 50000:
        value = value / 1e6
    return round(value, 2)


def hk_turnover_two_days(hsi_today_dt, hsi_prev_dt, *, session, user_agent: str, debug_save, to_float):
    out_today = None
    out_prev = None
    try:
        today_dt = hsi_today_dt.to_pydatetime() if hasattr(hsi_today_dt, "to_pydatetime") else hsi_today_dt
        hkd = parse_hkex_turnover_hkd(fetch_hkex_dayquot_html(session, today_dt, user_agent=user_agent, debug_save=debug_save), to_float=to_float)
        out_today = normalize_hk_turnover_to_yi(hkd)
    except Exception:
        try:
            aastocks_html = fetch_aastocks_hsi_html(session, user_agent=user_agent, debug_save=debug_save)
            out_today = normalize_hk_turnover_to_yi(parse_aastocks_turnover_yi(aastocks_html))
        except Exception:
            out_today = None

    try:
        prev_dt = hsi_prev_dt.to_pydatetime() if hasattr(hsi_prev_dt, "to_pydatetime") else hsi_prev_dt
        hkd = parse_hkex_turnover_hkd(fetch_hkex_dayquot_html(session, prev_dt, user_agent=user_agent, debug_save=debug_save), to_float=to_float)
        out_prev = normalize_hk_turnover_to_yi(hkd)
    except Exception:
        out_prev = None

    return out_today, out_prev


def hk_turnover_scan_prev(base_dt, *, max_back_days: int = 10, session, user_agent: str, debug_save, to_float):
    for days_back in range(1, max_back_days + 1):
        date_dt = base_dt - timedelta(days=days_back)
        try:
            hkd = parse_hkex_turnover_hkd(fetch_hkex_dayquot_html(session, date_dt, user_agent=user_agent, debug_save=debug_save), to_float=to_float)
            yi = normalize_hk_turnover_to_yi(hkd)
            if yi is not None:
                return yi
        except Exception:
            continue
    return None
