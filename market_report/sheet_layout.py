import re


def find_stock_rows_from_sheet(col_a: list[str], col_b: list[str]) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
    tw_header, hk_header = None, None
    n = max(len(col_a), len(col_b))
    for i in range(n):
        a = (col_a[i] if i < len(col_a) else "") or ""
        b = (col_b[i] if i < len(col_b) else "") or ""
        value = f"{a} {b}".strip()
        if "台股" in value and "台幣" in value:
            tw_header = i + 1
        if "港股" in value and "港幣" in value:
            hk_header = i + 1

    if tw_header is None or hk_header is None or hk_header <= tw_header:
        raise RuntimeError("找不到『台股（台幣）』或『港股（港幣）』標題列，請確認分頁版面")

    tw_rows: list[tuple[int, str]] = []
    for row_no in range(tw_header + 1, hk_header):
        if row_no - 1 >= len(col_a):
            break
        code = (col_a[row_no - 1] or "").strip()
        if code == "":
            break
        if code.isdigit():
            tw_rows.append((row_no, code))

    hk_rows: list[tuple[int, str]] = []
    for row_no in range(hk_header + 1, hk_header + 1 + 80):
        if row_no - 1 >= len(col_a):
            break
        code = (col_a[row_no - 1] or "").strip()
        if code == "":
            break
        normalized = code.replace(".HK", "").replace("HK", "").strip()
        if normalized.isdigit():
            hk_rows.append((row_no, normalized.zfill(5)))

    return tw_rows, hk_rows


def find_revenue_rows_from_sheet(col_a: list[str]) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    for row_no in range(3, 260 + 1):
        idx = row_no - 1
        if idx >= len(col_a):
            break
        value = (col_a[idx] or "").strip()
        if value == "":
            break
        normalized = re.sub(r"[^\d]", "", value)
        if normalized.isdigit():
            rows.append((row_no, normalized))
    return rows
