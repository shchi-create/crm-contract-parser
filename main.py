import os
from flask import Flask, request, render_template_string
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import json

# --------------------
# CONFIG
# --------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
OUTPUT_DOC_ID = os.getenv("OUTPUT_DOC_ID")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents"
]

# --------------------
# GOOGLE SHEETS
# --------------------
if SERVICE_ACCOUNT_JSON is None:
    raise ValueError("SERVICE_ACCOUNT_JSON is not set")
if SPREADSHEET_ID is None:
    raise ValueError("SPREADSHEET_ID is not set")
if OUTPUT_DOC_ID is None:
    raise ValueError("OUTPUT_DOC_ID is not set")

credentials_info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# --------------------
# GOOGLE DOCS
# --------------------
docs_service = build("docs", "v1", credentials=credentials)

def write_json_to_doc(data: dict):
    """Очищаем документ и вставляем JSON безопасно для любых размеров"""
    text = json.dumps(data, ensure_ascii=False, indent=2)

    # Получаем текущий документ
    doc = docs_service.documents().get(documentId=OUTPUT_DOC_ID).execute()
    content = doc.get('body', {}).get('content', [])

    if not content:
        end_index = 1
    else:
        # Последний элемент содержит реальный endIndex
        end_index = content[-1]['endIndex']

    # Не включаем последний символ (обычно это новая строка)
    if end_index > 1:
        end_index -= 1

    requests = [
        # Очистить весь контент кроме завершающего символа
        {"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end_index}}},
        # Вставить новый текст
        {"insertText": {"location": {"index": 1}, "text": text}}
    ]

    docs_service.documents().batchUpdate(documentId=OUTPUT_DOC_ID, body={"requests": requests}).execute()

# --------------------
# HELPERS
# --------------------
def load_sheet_records(sheet_name):
    """
    Читает лист с двухстрочным заголовком:
    1 строка — ключи
    2 строка — описания
    Данные — с 3 строки
    """
    worksheet = spreadsheet.worksheet(sheet_name)
    values = worksheet.get_all_values()

    if len(values) < 3:
        return []

    headers = values[0]
    data_rows = values[2:]

    records = []
    for row in data_rows:
        if any(cell.strip() for cell in row):
            records.append(dict(zip(headers, row)))
    return records

def collect_data_by_trip_id(trip_id: str):
    """Собираем все данные по Trip_ID и Client_ID"""
    all_sheets = spreadsheet.worksheets()
    sheet_names = [ws.title for ws in all_sheets]

    # 1. Получаем данные с Trips
    trips_records = load_sheet_records("Trips")
    matching_trips = [r for r in trips_records if r.get("Trip_ID","").strip() == trip_id]

    if not matching_trips:
        return None, f"Trip_ID {trip_id} not found in Trips sheet"

    # 2. Получаем список всех Client_ID для этого Trip_ID
    client_ids = set(r.get("Client_ID","").strip() for r in matching_trips if r.get("Client_ID"))

    # 3. Собирать все строки со всех листов по этим Client_ID
    data_by_sheet = {"Trips": matching_trips}

    for sheet_name in sheet_names:
        if sheet_name == "Trips":
            continue
        records = load_sheet_records(sheet_name)
        filtered = [r for r in records if r.get("Client_ID","").strip() in client_ids]
        if filtered:
            data_by_sheet[sheet_name] = filtered

    return data_by_sheet, None

# --------------------
# FLASK APP
# --------------------
app = Flask(__name__)

HTML_FORM = """
<!doctype html>
<title>Generate Trip JSON</title>
<h2>Generate JSON by Trip_ID</h2>
<form method="post">
  <input type="text" name="trip_id" placeholder="Trip_ID" required>
  <button type="submit">Run</button>
</form>
<p>{{ message }}</p>
"""

@app.route("/run", methods=["GET", "POST"])
def run():
    message = ""
    if request.method == "POST":
        trip_id = request.form["trip_id"].strip()
        data, error = collect_data_by_trip_id(trip_id)

        if error:
            return render_template_string(HTML_FORM, message=error)

        try:
            write_json_to_doc(data)
            message = f"JSON for Trip_ID {trip_id} written to Google Doc successfully."
        except Exception as e:
            message = f"Error writing to Google Doc: {str(e)}"

    return render_template_string(HTML_FORM, message=message)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
