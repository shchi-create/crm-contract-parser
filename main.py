import os
import io
import json
import datetime
from flask import Flask, request, render_template, redirect, url_for
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ---------- Config ----------
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SERVICE_ACCOUNT_JSON_ENV = os.environ.get("SERVICE_ACCOUNT_JSON")
if not SERVICE_ACCOUNT_JSON_ENV:
    raise RuntimeError("SERVICE_ACCOUNT_JSON environment variable is not set")

# SERVICE_ACCOUNT_JSON may be raw JSON or base64 encoded (try to parse)
try:
    # try load directly
    service_account_info = json.loads(SERVICE_ACCOUNT_JSON_ENV)
except Exception:
    # try base64 decode
    import base64
    try:
        decoded = base64.b64decode(SERVICE_ACCOUNT_JSON_ENV).decode("utf-8")
        service_account_info = json.loads(decoded)
    except Exception as e:
        raise RuntimeError("Cannot parse SERVICE_ACCOUNT_JSON: %s" % str(e))

# scopes needed for Sheets read and Drive/Docs write
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/documents"
]

credentials = service_account.Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)

# gspread client
gc = gspread.service_account_from_dict(service_account_info)

# build drive service
drive_service = build("drive", "v3", credentials=credentials)
docs_service = build("docs", "v1", credentials=credentials)

app = Flask(__name__)

# ---------- Helpers ----------
def get_sheet_records(sheet_name):
    """Return list of dicts for sheet_name or [] if sheet not found."""
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        values = worksheet.get_all_values()

headers = values[0]        # первая строка — ключи
data_rows = values[2:]     # пропускаем вторую (описания)

records = [
    dict(zip(headers, row))
    for row in data_rows
]
        return records
    except Exception as e:
        # if sheet doesn't exist or other error
        return []

def truthy(val):
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("true", "1", "yes", "y", "да")

def first_nonempty(*values):
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s != "":
            return s
    return None

def build_json_for_trip(trip_id):
    # load sheets
    profiles = get_sheet_records("Profile")
    trips = get_sheet_records("Trips")
    contacts = get_sheet_records("Contacts")
    payments = get_sheet_records("Payments")

    # build clients dict from profiles (keyed by Client_ID)
    clients = {}
    for r in profiles:
        cid = str(r.get("Client_ID") or "").strip()
        if not cid:
            continue
        clients[cid] = {
            "client_id": cid,
            "Gender": r.get("Gender") or "",
            "Date_of_birth": r.get("Date_of_birth") or "",
            "RU_LastName": r.get("RU_LastName") or "",
            "RU_FirstName": r.get("RU_FirstName") or "",
            "RU_MiddleName": r.get("RU_MiddleName") or "",
            "RU_IDS": r.get("RU_IDS") or "",
            "RU_IDN": r.get("RU_IDN") or "",
            "INT_LastName": r.get("INT_LastName") or "",
            "INT_FirstName": r.get("INT_FirstName") or "",
            "INT_ID": r.get("INT_ID") or ""
        }

    # contacts list -> map by client_id
    contacts_map = {}
    for r in contacts:
        cid = str(r.get("Client_ID") or "").strip()
        if not cid:
            continue
        contacts_map[cid] = {
            "phone": r.get("Phone") or r.get("phone") or "",
            "email": r.get("email") or r.get("Email") or ""
        }

    # find all trip rows that match trip_id
    trip_rows = [r for r in trips if str(r.get("Trip_ID") or "").strip() == str(trip_id)]
    if not trip_rows:
        raise ValueError(f"Trip_ID {trip_id} not found in Trips sheet")

    # aggregate trip-level fields from the first matching row (fallback to any non-empty)
    first = trip_rows[0]
    russia_val = None
    # some sheets may store boolean True/False or 'true'/'false'
    russia_val = truthy(first.get("Russia", first.get("russia")))
    # but if some rows have different Russia values, prefer first non-empty:
    for rr in trip_rows:
        if rr.get("Russia") is not None and str(rr.get("Russia")).strip() != "":
            russia_val = truthy(rr.get("Russia"))
            break

    # gather passengers (there may be multiple rows with same trip id for each client)
    passengers = []
    main_tourist_client_id = None
    passenger_ids_seen = set()
    for rr in trip_rows:
        cid = str(rr.get("Client_ID") or "").strip()
        if not cid:
            continue
        if cid in passenger_ids_seen:
            continue
        passenger_ids_seen.add(cid)
        mt = truthy(rr.get("mainTourist") or rr.get("main_tourist") or rr.get("mainTourist", False))
        passengers.append({"client_id": cid, "mainTourist": bool(mt)})
        if mt:
            main_tourist_client_id = cid

    # if main tourist not explicit, try to find row where field mainTourist true in any row
    if not main_tourist_client_id:
        for rr in trip_rows:
            if truthy(rr.get("mainTourist") or rr.get("main_tourist")):
                main_tourist_client_id = str(rr.get("Client_ID") or "").strip()
                break

    # If still none, try to fallback to first passenger (not ideal, but avoids crash)
    if not main_tourist_client_id and passengers:
        main_tourist_client_id = passengers[0]["client_id"]

    # trip-level aggregated fields:
    trip_obj = {
        "trip_id": str(trip_id),
        "russia": bool(russia_val),
        "client_id": first.get("Client_ID") or "",
        "operator_id": first.get("operator_id") or "",
        "operator_order_number": first.get("operator_order_number") or "",
        "main_tourist_client_id": main_tourist_client_id,
        "From": first_nonempty(first.get("From"), first.get("from")),
        "Destination": first_nonempty(first.get("Destination"), first.get("destination")),
        "Start_Date": first.get("Start_Date") or first.get("start_date") or "",
        "End_Date": first.get("End_Date") or first.get("end_date") or "",
        "accommodation": first.get("accommodation") or "",
        "flights": first.get("flights") or "",
        "transfer": first.get("transfer") or "",
        "additionalServices": first.get("additionalServices") or "",
        "Insurance": first.get("Insurance") or "",
        "passengers": passengers
    }

    # main_tourist block: always use RU_* for main tourist as per spec
    main_block = {}
    if main_tourist_client_id and main_tourist_client_id in clients:
        c = clients[main_tourist_client_id]
        main_block = {
            "client_id": main_tourist_client_id,
            "RU_LastName": c.get("RU_LastName"),
            "RU_FirstName": c.get("RU_FirstName"),
            "RU_MiddleName": c.get("RU_MiddleName"),
            "RU_IDS": c.get("RU_IDS"),
            "RU_IDN": c.get("RU_IDN"),
        }
        # merge contacts if present
        if main_tourist_client_id in contacts_map:
            main_block["phone"] = contacts_map[main_tourist_client_id].get("phone", "")
            main_block["email"] = contacts_map[main_tourist_client_id].get("email", "")
    trip_obj["main_tourist"] = main_block

    # build clients array (only those referenced in passengers; but spec said clients could be up to 10 in one request)
    clients_arr = []
    for cid in passenger_ids_seen:
        if cid in clients:
            # include both RU and INT fields, plus Gender and DOB. Contacts optional.
            c = clients[cid].copy()
            if cid in contacts_map:
                c["contacts"] = contacts_map[cid]
            clients_arr.append(c)
        else:
            # unknown client in Trips; create minimal record
            clients_arr.append({"client_id": cid})

    # payments: find all rows in payments sheet matching trip_id
    payments_rows = [r for r in payments if str(r.get("Trip_ID") or "").strip() == str(trip_id)]
    payments_obj = []
    if payments_rows:
        # payments rows may be structured as one row per trip or one per client
        # we'll aggregate by Trip_ID, but create a single payments entry with per_client list
        # assume payment-level Prepay_percent, Payment_Link are same for the trip (use first non-empty)
        first_pay = payments_rows[0]
        prepay_percent = first_nonempty(*(r.get("Prepay_percent") for r in payments_rows))
        payment_link = first_nonempty(*(r.get("Payment_Link") for r in payments_rows))
        per_client = []
        for r in payments_rows:
            cid = str(r.get("Client_ID") or "").strip()
            if not cid:
                # some sheets may use a column with different name for client id, try "client_id"
                cid = str(r.get("client_id") or "").strip()
            if not cid:
                # try to detect column for client id by heuristics (skip if none)
                continue
            per_client.append({
                "client_id": cid,
                "T_Prepay_amount": r.get("T_Prepay_amount") or r.get("T_Prepay_Amount") or 0,
                "T_Total_RUB": r.get("T_Total_RUB") or 0,
                "T_Total": r.get("T_Total") or 0,
                "T_DueDate": r.get("T_DueDate") or ""
            })
        payments_obj.append({
            "trip_id": str(trip_id),
            "Prepay_percent": float(prepay_percent) if prepay_percent not in (None, "") else None,
            "Payment_Link": payment_link or "",
            "per_client": per_client
        })

    # final JSON structure
    result = {
        "meta": {
            "version": "1.0",
            "generated_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "source": "CRM"
        },
        "clients": clients_arr,
        "contacts": [{"client_id": k, **v} for k, v in contacts_map.items()],
        "trips": [trip_obj],
        "payments": payments_obj
    }
    return result

def create_google_doc_with_json(json_obj, trip_id):
    """Create Google Doc with JSON content and return the doc URL"""
    title = f"trip_{trip_id}_json_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    json_str = json.dumps(json_obj, ensure_ascii=False, indent=2)
    # create Google Doc via Drive API uploading a plain text that will be converted to Google Doc
    media = MediaIoBaseUpload(io.BytesIO(json_str.encode("utf-8")), mimetype="text/plain", resumable=False)
    file_metadata = {"name": title, "mimeType": "application/vnd.google-apps.document"}
    created = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    file_id = created.get("id")
    link = f"https://docs.google.com/document/d/{file_id}/edit"
    return link, file_id, json_str

# ---------- Routes ----------
@app.route("/")
def index():
    return redirect(url_for("run_trip"))

@app.route("/project_name/run", methods=["GET", "POST"])
def run_trip():
    if request.method == "GET":
        return render_template("run.html")
    trip_id = request.form.get("trip_id", "").strip()
    if not trip_id:
        return render_template("run.html", error="trip_id не указан")
    try:
        json_obj = build_json_for_trip(trip_id)
    except Exception as e:
        return render_template("run.html", error=str(e))
    try:
        link, file_id, json_str = create_google_doc_with_json(json_obj, trip_id)
    except Exception as e:
        return render_template("run.html", error="Ошибка при создании Google Document: " + str(e))
    # show first ~2000 chars as preview
    preview = json_str[:2000] + ("..." if len(json_str) > 2000 else "")
    return render_template("run.html", doc_link=link, json_preview=preview)

# API endpoint (optional) — return JSON directly
@app.route("/project_name/api/generate", methods=["GET"])
def api_generate():
    trip_id = request.args.get("trip_id")
    if not trip_id:
        return {"error": "trip_id required"}, 400
    try:
        json_obj = build_json_for_trip(trip_id)
    except Exception as e:
        return {"error": str(e)}, 400
    return json_obj

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
