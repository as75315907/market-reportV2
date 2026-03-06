import re


def get_sheet_properties(svc, spreadsheet_id: str, tab_name: str) -> dict:
    metadata = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return props
    raise RuntimeError(f"Sheet not found: {tab_name}")


def hide_column_a(svc, spreadsheet_id: str, sheet_id: int) -> None:
    body = {
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            }
        ]
    }
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
