"""
Nadlan Proxy Server v4
- Cities/Streets/Deals: data.gov.il (open government data)
"""
import os, json, logging, threading, time, csv, io
import requests
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from compensation import compensation_bp, init_db

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

GOV_API          = "https://data.gov.il/api/3/action/datastore_search"
CITIES_RESOURCE  = "b7cf8f14-64a2-4b33-8d4b-edb286fdbd37"
STREETS_RESOURCE = "a7296d1a-f6c1-4b8a-b6b8-3f7fcf7e5dfd"
DEALS_RESOURCE   = "0f63699b-f9a5-4e72-abc8-cb46af0e4d13"

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "NadlanProxy/4.0"
})

def gov_get(resource_id, filters=None, q=None, limit=10, sort=None):
    params = {"resource_id": resource_id, "limit": limit}
    if q:       params["q"] = q
    if filters: params["filters"] = json.dumps(filters, ensure_ascii=False)
    if sort:    params["sort"] = sort
    r = SESSION.get(GOV_API, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

@app.route("/")
@app.route("/dashboard")
def dashboard():
    return send_from_directory("static", "index.html")

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "nadlan-proxy-v4"})

@app.route("/cities")
def cities():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    try:
        data = gov_get(CITIES_RESOURCE, q=q, limit=8)
        records = data.get("result", {}).get("records", [])
        seen, results = set(), []
        for r in records:
            name = (r.get("שם_ישוב") or r.get("city_name") or "").strip()
            code = str(r.get("סמל_ישוב") or r.get("city_code") or "")
            if name and name not in seen:
                seen.add(name)
                results.append({"name": name, "code": code})
        return jsonify({"results": results})
    except Exception as e:
        log.error("cities error: %s", e)
        return jsonify({"results": [], "error": str(e)})

@app.route("/streets")
def streets():
    q         = request.args.get("q", "").strip()
    city_code = request.args.get("city_code", "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    try:
        filters = {}
        if city_code:
            filters["סמל_ישוב"] = city_code
        data = gov_get(STREETS_RESOURCE,
                       filters=filters if filters else None,
                       q=q, limit=10)
        records = data.get("result", {}).get("records", [])
        seen, results = set(), []
        for r in records:
            name = (r.get("שם_רחוב") or r.get("street_name") or "").strip()
            if name and name not in seen:
                seen.add(name)
                results.append({"name": name})
        return jsonify({"results": results})
    except Exception as e:
        log.error("streets error: %s", e)
        return jsonify({"results": [], "error": str(e)})

@app.route("/deals")
def deals():
    street = request.args.get("street", "").strip()
    city   = request.args.get("city",   "").strip()
    limit  = int(request.args.get("limit", 8))

    if not street or not city:
        return jsonify({"deals": [], "error": "missing params"})

    try:
        # Try with street+city filter
        filters = {"STREETNAME": street, "CITYNAME": city}
        data = gov_get(DEALS_RESOURCE,
                       filters=filters,
                       limit=limit,
                       sort="DEALDATETIME desc")
        records = data.get("result", {}).get("records", [])
        log.info("deals found %d for %s %s", len(records), street, city)

        # If no results, try city only
        if not records:
            data = gov_get(DEALS_RESOURCE,
                           filters={"CITYNAME": city},
                           q=street,
                           limit=limit,
                           sort="DEALDATETIME desc")
            records = data.get("result", {}).get("records", [])
            log.info("deals fallback found %d", len(records))

        deals_out = []
        for d in records:
            try:
                price = float(d.get("DEALAMOUNT") or 0)
                area  = float(d.get("DEALSIZE")   or 0)
                rooms = d.get("ASSETROOMNUM") or ""
                floor = d.get("FLOORNO") or ""
                date  = str(d.get("DEALDATETIME") or "")[:10]
                addr  = (d.get("STREETNAME") or street)
                house = d.get("HOUSENUM") or ""
                if house:
                    addr = addr + " " + str(house)
                ppm = round(price / area) if area > 0 and price > 0 else 0
                deals_out.append({
                    "address": addr,
                    "city":    city,
                    "price":   price,
                    "size":    area,
                    "rooms":   rooms,
                    "floor":   floor,
                    "date":    date,
                    "ppm":     ppm
                })
            except Exception as pe:
                log.warning("parse error: %s", pe)
                continue

        return jsonify({
            "deals": deals_out,
            "source": "data.gov.il",
            "count": len(deals_out)
        })

    except Exception as e:
        log.error("deals error: %s", e)
        return jsonify({"deals": [], "error": str(e)})

app.register_blueprint(compensation_bp)
init_db(app)

# ── Google Sheets auto-sync ────────────────────────────────────────────────
# Reads the public CSV export of the sheet every SHEETS_SYNC_INTERVAL seconds.
# Set SHEETS_ID env var to the Google Sheets document ID.
# The sheet must be shared as "Anyone with the link can view".
# Each tab to sync is listed in SHEETS_TABS (comma-separated, e.g. "רחובות,יבנה").
# Column mapping (1-based, configurable via env):
#   SHEETS_COL_NAME=1, SHEETS_COL_COMMISSION=3, SHEETS_COL_TARGET=5, SHEETS_COL_QUARTERLY=6

SHEETS_ID       = os.environ.get("SHEETS_ID", "1MAnI-x5KzdHSymdb1ep7XzxCWJrI7BBq")
SHEETS_TABS     = [t.strip() for t in os.environ.get("SHEETS_TABS", "רחובות,יבנה").split(",")]
SHEETS_INTERVAL = int(os.environ.get("SHEETS_SYNC_INTERVAL", "300"))  # seconds (default 5 min)
COL_NAME        = int(os.environ.get("SHEETS_COL_NAME", "1")) - 1
COL_COMMISSION  = int(os.environ.get("SHEETS_COL_COMMISSION", "3")) - 1
COL_TARGET      = int(os.environ.get("SHEETS_COL_TARGET", "5")) - 1
COL_QUARTERLY   = int(os.environ.get("SHEETS_COL_QUARTERLY", "6")) - 1
SHEETS_ADMIN_KEY = os.environ.get("COMP_ADMIN_KEY", "")


def _parse_amount(val):
    """Parse '₪48,425' or '48425' or '48,425' → float."""
    if not val:
        return 0.0
    cleaned = str(val).replace("₪", "").replace(",", "").replace(" ", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _sync_sheet_tab(tab_name):
    gid_map = {t: str(i) for i, t in enumerate(SHEETS_TABS)}
    gid = gid_map.get(tab_name, "0")
    url = f"https://docs.google.com/spreadsheets/d/{SHEETS_ID}/export?format=csv&gid={gid}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            log.warning("Sheets sync: tab %s returned %s", tab_name, r.status_code)
            return 0
        reader = csv.reader(io.StringIO(r.text))
        rows = list(reader)
        synced = 0
        with app.app_context():
            for row in rows:
                if len(row) <= max(COL_NAME, COL_COMMISSION):
                    continue
                name = row[COL_NAME].strip()
                if not name or name in ("סוכן", "שם", "סה״כ", "סה\"כ", ""):
                    continue
                commission = _parse_amount(row[COL_COMMISSION] if len(row) > COL_COMMISSION else "")
                target_ann = _parse_amount(row[COL_TARGET] if len(row) > COL_TARGET else "")
                target_qrt = _parse_amount(row[COL_QUARTERLY] if len(row) > COL_QUARTERLY else "")
                if commission <= 0 and target_ann <= 0:
                    continue
                # Call the sync endpoint internally
                with app.test_request_context():
                    from compensation.routes import sync_from_sheets as _sync
                payload = {
                    "name": name,
                    "tab": tab_name,
                    "total_commission": commission,
                    "target_annual": target_ann or None,
                    "target_quarterly": target_qrt or None,
                    "sync_key": SHEETS_ADMIN_KEY,
                    "fiscal_year": __import__("datetime").date.today().year,
                }
                with app.test_client() as c:
                    resp = c.post("/comp/sync/sheets", json=payload)
                    if resp.status_code == 200:
                        synced += 1
        return synced
    except Exception as e:
        log.error("Sheets sync error (tab %s): %s", tab_name, e)
        return 0


def _sheets_sync_loop():
    time.sleep(10)  # let the app fully start first
    while True:
        if SHEETS_ID:
            for tab in SHEETS_TABS:
                n = _sync_sheet_tab(tab)
                if n:
                    log.info("Sheets auto-sync: %d agents synced from tab '%s'", n, tab)
        time.sleep(SHEETS_INTERVAL)


# Start background sync thread (only if SHEETS_ID is configured)
if SHEETS_ID and os.environ.get("DISABLE_SHEETS_SYNC") != "1":
    _t = threading.Thread(target=_sheets_sync_loop, daemon=True)
    _t.start()
    log.info("Google Sheets auto-sync started (interval=%ds, tabs=%s)", SHEETS_INTERVAL, SHEETS_TABS)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
