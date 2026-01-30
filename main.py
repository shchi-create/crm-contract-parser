import os
from flask import Flask, request, render_template_string
import gspread
from google.oauth2.service_account import Credentials
import json

# --------------------
# CONFIG
# --------------------

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --------------------
# GOOGLE SHEETS
# --------------------

credentials_info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = Credentials.from_service_account_info(
    credentials_info, scopes=SCOPES
)

gc = gspread.authorize(credentials)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

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

        result = {
            "Trip_ID": trip_id,
            "Trips": matching_trips
        }

        # запись json в Google Docs
        doc = gc.create(f"trip_{trip_id}_json")
        doc.share(None, perm_type="anyone", role="reader")

        doc_worksheet = doc.add_worksheet(
            title="json",
            rows=1000,
            cols=1
        )
        doc_worksheet.update("A1", [[json.dumps(result, ensure_ascii=False, indent=2)]])

        message = f"JSON created: {doc.url}"

    return render_template_string(HTML_FORM, message=message)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
