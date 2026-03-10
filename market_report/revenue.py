import os
import re
from io import StringIO

import pandas as pd

from market_report.sheet_layout import find_revenue_rows_from_sheet

REV_TAB_DEFAULT = "營收"
MOPSFIN_LISTED_CSV = "https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv"
MOPSFIN_OTC_CSV = "https://mopsfin.twse.com.tw/opendata/t187ap05_O.csv"
TPEX_EMERGING_JSON = "https://www.tpex.org.tw/openapi/v1/t187ap05_R"
MOPS_COMPANY_REVENUE_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t05st10_ifrs"
MOPS_COMPANY_REVENUE_REFERER = "https://mopsov.twse.com.tw/mops/web/t05st10_ifrs"


def fetch_company_month_revenue_mops(session, user_agent: str, code: str, year_ad: int, month: int, to_float):
    """Fetch one company's monthly revenue from MOPS company query page.

    Returns dict with keys: this, last_year (both in仟元) or None when unavailable.
    """
    roc_year = year_ad - 1911
    payload = {
        "firstin": "1",
        "off": "1",
        "step": "1",
        "TYPEK": "all",
        "isnew": "false",
        "queryName": "co_id",
        "inpuType": "co_id",
        "co_id": str(code).zfill(4),
        "year": str(roc_year),
        "month": f"{int(month):02d}",
    }
    headers = {
        "User-Agent": user_agent,
        "Referer": MOPS_COMPANY_REVENUE_REFERER,
    }
    response = session.post(MOPS_COMPANY_REVENUE_URL, data=payload, headers=headers, timeout=40)
    response.raise_for_status()
    html = response.text or ""
    # MOPS anti-bot/invalid params response
    if "頁面無法執行" in html or "年度及月份 欄位錯誤" in html:
        return None

    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None

    detail = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("營業收入" in c for c in cols):
            detail = t
            break
    if detail is None or detail.empty:
        return None

    item_col = str(detail.columns[0])
    value_col = str(detail.columns[1]) if len(detail.columns) > 1 else None
    if not value_col:
        return None

    this_val = None
    last_year_val = None
    for _, row in detail.iterrows():
        key = str(row.get(item_col, "")).strip()
        val = to_float(row.get(value_col))
        if key == "本月":
            this_val = val
        elif key == "去年同期":
            last_year_val = val

    if this_val is None and last_year_val is None:
        return None

    return {"this": this_val, "last_year": last_year_val}


def ym_add(year: int, month: int, delta_months: int) -> tuple[int, int]:
    y, m = year, month + delta_months
    while m <= 0:
        y -= 1
        m += 12
    while m >= 13:
        y += 1
        m -= 12
    return y, m


def ym_label(year: int, month: int) -> str:
    return f"{year}/{month:02d}月"


def parse_ym_any(value) -> tuple[int, int] | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    if len(digits) == 5:
        roc_year = int(digits[:3])
        month = int(digits[3:])
        if 1 <= month <= 12:
            return roc_year + 1911, month
    if len(digits) >= 6:
        year = int(digits[:4])
        month = int(digits[4:6])
        if 1 <= month <= 12:
            return year, month
    return None


def clean_colname(name: str) -> str:
    return str(name).strip().replace("\ufeff", "")


def find_colname(cols: list[str], includes: list[str], excludes: list[str] | None = None) -> str | None:
    excludes = excludes or []
    for col in cols:
        col_str = str(col)
        if all(key in col_str for key in includes) and not any(ex in col_str for ex in excludes):
            return col
    return None


def download_csv_to_df(session, url: str) -> pd.DataFrame:
    response = session.get(url, timeout=40)
    response.raise_for_status()
    text = response.content.decode("utf-8-sig", errors="ignore")
    df = pd.read_csv(StringIO(text), dtype=str)
    df.columns = [clean_colname(col) for col in df.columns]
    return df


def download_json_to_df(session, user_agent: str, url: str) -> pd.DataFrame:
    response = session.get(url, timeout=40, headers={"User-Agent": user_agent})
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    if not isinstance(data, list):
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df.columns = [clean_colname(col) for col in df.columns]
    return df


def fetch_monthly_revenue_maps_all(session, user_agent: str, to_float) -> tuple[tuple[int, int] | None, dict]:
    frames: list[pd.DataFrame] = []

    for url in (MOPSFIN_LISTED_CSV, MOPSFIN_OTC_CSV):
        try:
            df = download_csv_to_df(session, url)
            if not df.empty:
                frames.append(df)
        except Exception:
            pass

    try:
        df_r = download_json_to_df(session, user_agent, TPEX_EMERGING_JSON)
        if not df_r.empty:
            frames.append(df_r)
    except Exception:
        pass

    if not frames:
        return None, {}

    df = pd.concat(frames, ignore_index=True)
    cols = list(df.columns)

    code_col = (
        find_colname(cols, ["公司", "代號"])
        or find_colname(cols, ["證券", "代號"])
        or find_colname(cols, ["公司代碼"])
    )
    ym_col = find_colname(cols, ["資料", "年月"]) or find_colname(cols, ["資料年月"]) or find_colname(cols, ["年月"])
    this_col = find_colname(cols, ["當月營收"], excludes=["累計"])
    lastm_col = find_colname(cols, ["上月營收"], excludes=["累計"])
    lasty_col = (
        find_colname(cols, ["去年當月營收"], excludes=["累計"])
        or find_colname(cols, ["去年同期營收"], excludes=["累計"])
    )

    if not code_col or not this_col or not lastm_col or not lasty_col:
        return None, {}

    dataset_ym = None
    if ym_col:
        yms = []
        for value in df[ym_col].dropna().tolist():
            parsed = parse_ym_any(value)
            if parsed:
                yms.append(parsed)
        if yms:
            dataset_ym = sorted(set(yms))[-1]

    revenue_map = {}
    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).strip()
        code = re.sub(r"\D", "", code)
        if not code:
            continue
        revenue_map[code] = {
            "this": to_float(row.get(this_col)),
            "last_year": to_float(row.get(lasty_col)),
            "last_month": to_float(row.get(lastm_col)),
        }

    return dataset_ym, revenue_map


def update_revenue_tab(
    svc,
    sheet_id: str,
    *,
    get_values,
    batch_update_values,
    today_taipei,
    session,
    user_agent: str,
    to_float,
) -> None:
    tab = os.getenv("GSHEET_TAB_REVENUE", REV_TAB_DEFAULT).strip() or REV_TAB_DEFAULT
    tab_q = f"'{tab}'" if re.search(r"[^A-Za-z0-9_]", tab) else tab

    ab = get_values(svc, sheet_id, f"{tab_q}!A1:B260")
    col_a = [row[0] if len(row) > 0 else "" for row in ab]
    rows = find_revenue_rows_from_sheet(col_a)

    now = today_taipei()
    exp_y, exp_m = ym_add(now.year, now.month, -1)
    dataset_ym, rev_map = fetch_monthly_revenue_maps_all(session, user_agent, to_float)

    # Fallback: if open data not updated to expected month yet, query MOPS company page per code.
    # This is slower but helps bridge the release lag for the tracked code list.
    enable_fallback = os.getenv("REVENUE_ENABLE_MOPS_FALLBACK", "1").strip() == "1"
    expected_ym = (exp_y, exp_m)
    if enable_fallback and rows and (dataset_ym is None or dataset_ym < expected_ym):
        previous_month_map = {code: (rev_map.get(code, {}) or {}).get("this") for _, code in rows}
        fetched = 0
        for _, code in rows:
            try:
                mops = fetch_company_month_revenue_mops(session, user_agent, code, exp_y, exp_m, to_float)
            except Exception:
                mops = None
            if not mops:
                continue
            rev_map.setdefault(code, {})
            rev_map[code]["this"] = mops.get("this")
            rev_map[code]["last_year"] = mops.get("last_year")
            # MOPS company page doesn't provide previous month directly; use open-data "this" as proxy.
            if previous_month_map.get(code) is not None:
                rev_map[code]["last_month"] = previous_month_map.get(code)
            fetched += 1

        if fetched > 0:
            dataset_ym = expected_ym
            print(f"Revenue fallback used: MOPS company query | month={exp_y}-{exp_m:02d} | fetched={fetched}")

    use_y, use_m = dataset_ym if dataset_ym else (exp_y, exp_m)
    y_ly, m_ly = use_y - 1, use_m
    y_lm, m_lm = ym_add(use_y, use_m, -1)

    updates: list[tuple[str, list[list]]] = [
        (f"{tab_q}!C2", [[ym_label(use_y, use_m)]]),
        (f"{tab_q}!D2", [[ym_label(y_ly, m_ly)]]),
        (f"{tab_q}!F2", [[ym_label(y_lm, m_lm)]]),
    ]

    missing: list[str] = []
    for row_no, code in rows:
        data = rev_map.get(code)
        if not data:
            missing.append(code)
            updates.append((f"{tab_q}!C{row_no}", [[None]]))
            updates.append((f"{tab_q}!D{row_no}", [[None]]))
            updates.append((f"{tab_q}!F{row_no}", [[None]]))
            continue

        updates.append((f"{tab_q}!C{row_no}", [[data.get("this")]]))
        updates.append((f"{tab_q}!D{row_no}", [[data.get("last_year")]]))
        updates.append((f"{tab_q}!F{row_no}", [[data.get("last_month")]]))

    if updates:
        batch_update_values(svc, sheet_id, updates, value_input="USER_ENTERED")

    print(f"Revenue tab updated: {tab} | month={use_y}-{use_m:02d} | rows={len(rows)}")
    if missing:
        print("Revenue missing codes:", ", ".join(missing))
