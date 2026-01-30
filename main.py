import os
import json
from flask import Flask, request, render_template_string

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --------------------
# ENV VARIABLES
# --------------------

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
OUTPUT_DOC_ID = os.getenv("OUTPUT_DOC_ID")

if not all([SPREADSHEET_ID, SERVICE_ACCOUNT_JSON, OUTPUT_DOC_ID]):
    raise RuntimeError("Missing required environment variables")

# --------------------
# GOOGLE AUTH
# --------------------

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/documents",
]

credentials_info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=SCOPES
)

# Sheets
gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# Docs
docs_service = build("docs", "v1", credentials=credentials)

# --------------------
# HELPERS
# --------------------

def load_sheet_records(sheet_name: str) -> list[dict]:
    """
    Sheet format:
    row 1 — keys
    row 2 — descriptions
    row 3+ — data
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


def write_json_to_doc(data: dict) -> None:
    # превращаем dict в json
    text = json.dumps(data, ensure_ascii=False, indent=2)

    # получаем текущее состояние документа
    doc = docs_service.documents().get(documentId=OUTPUT_DOC_ID).execute()
    end_index = doc['body']['content'][-1]['endIndex']

    requests = []

    # если документ не пустой — удалить содержимое
    if end_index > 1:
        requests.append({
            "deleteContentRange": {
                "range": {
                    "startIndex": 1,
                    "endIndex": end_index - 1  # обязательно меньше, чем endIndex последнего элемента
                }
            }
        })

    # вставить новый текст
    requests.append({
        "insertText": {
            "location": {"index": 1},
            "text": text
        }
    })

    docs_service.documents().batchUpdate(
        documentId=OUTPUT_DOC_ID,
        body={"requests": requests}
    ).execute()


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

        trips = load_sheet_records("Trips")

        matching_trips = [
            t for t in trips
            if t.get("Trip_ID", "").strip() == trip_id
        ]

        if not matching_trips:
            return render_template_string(
                HTML_FORM,
                message=f"Trip_ID {trip_id} not found in Trips sheet"
            )

        result_json = {
            "Trip_ID": trip_id,
            "Trips": matching_trips
        }

        write_json_to_doc(result_json)

        message = f"JSON for Trip_ID {trip_id} written to document"

    return render_template_string(HTML_FORM, message=message)

# --------------------
# ENTRYPOINT
# --------------------

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080))
    )
