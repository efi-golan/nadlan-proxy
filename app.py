"""
Nadlan Proxy Server v2
Uses data.gov.il (open government data) for all queries.
Routes:
  GET /health          - health check
  GET /cities?q=       - autocomplete cities
  GET /streets?q=&city_code= - autocomplete streets
  GET /deals?street=&city=&rooms=&limit= - real estate transactions
"""

import os, json, logging
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

GOV_API          = "https://data.gov.il/api/3/action/datastore_search"
DEALS_RESOURCE   = "b8ef3d6b-f708-4c5a-a600-b45ef46a47ac"
CITIES_RESOURCE  = "b7cf8f14-64a2-4b33-8d4b-edb286fdbd37"
STREETS_RESOURCE = "a7296d1a-f6c1-4b8a-b6b8-3f7fcf7e5dfd"

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "NadlanProxy/2.0"})

def gov_get(resource_id, filters=None, q=None, limit=10, sort=None):
    params = {"resource_id": resource_id, "limit": limit}
    if q:       params["q"] = q
    if filters: params["filters"] = json.dumps(filters, ensure_ascii=False)
    if sort:    params["sort"] = sort
    r = SESSION.get(GOV_API, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "nadlan-proxy-v2"})

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
            name = (r.get("שם_ישוב") or r.get("שם_יישוב") or "").strip()
            code = str(r.get("סמל_ישוב") or "")
            if name and name not in seen:
                seen.add(name)
                results.append({"name": name, "code": code})
        return jsonify({"results": results})
    except Exception as e:
        log.error(f"/cities: {e}")
        return jsonify({"error": str(e)}), 502

@app.route("/streets")
def streets():
    q         = request.args.get("q", "").strip()
    city_code = request.args.get("city_code", "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    try:
        filters = {"סמל_ישוב": city_code} if city_code else None
        data    = gov_get(STREETS_RESOURCE, q=q, filters=filters, limit=12)
        records = data.get("result", {}).get("records", [])
        seen, results = set(), []
        for r in records:
            name = (r.get("שם_רחוב") or "").strip()
            if name and name not in seen:
                seen.add(name)
                results.append({"name": name})
        return jsonify({"results": results})
    except Exception as e:
        log.error(f"/streets: {e}")
        return jsonify({"error": str(e)}), 502

@app.route("/deals")
def deals():
    street = request.args.get("street", "").strip()
    city   = request.args.get("city", "").strip()
    limit  = min(int(request.args.get("limit", 8)), 12)

    if not city:
        return jsonify({"error": "Missing city"}), 400
    try:
        # Try with exact street + city
        filters = {"עיר_עסקה": city}
        if street:
            filters["רחוב_עסקה"] = street
        data    = gov_get(DEALS_RESOURCE, filters=filters, limit=limit, sort="תאריך_עסקה desc")
        records = data.get("result", {}).get("records", [])

        # Fallback: city + street as free text
        if not records and street:
            data    = gov_get(DEALS_RESOURCE, filters={"עיר_עסקה": city}, q=street, limit=limit, sort="תאריך_עסקה desc")
            records = data.get("result", {}).get("records", [])

        out = []
        for r in records:
            price = r.get("מחיר") or r.get("שווי_עסקה") or 0
            size  = r.get("שטח_דירה") or r.get("שטח_כולל") or 0
            try: price = float(price)
            except: price = 0
            try: size = float(size)
            except: size = 0
            ppm = round(price / size) if price > 0 and size > 0 else 0
            out.append({
                "address": ((r.get("רחוב_עסקה") or "") + " " + str(r.get("מספר_בית") or "")).strip(),
                "city":    r.get("עיר_עסקה") or "",
                "price":   price,
                "size":    size,
                "rooms":   r.get("מספר_חדרים") or "",
                "floor":   r.get("קומה") or "",
                "date":    (r.get("תאריך_עסקה") or "")[:7],
                "type":    r.get("סוג_נכס") or "",
                "ppm":     ppm,
            })

        return jsonify({"deals": out, "total": len(out)})
    except Exception as e:
        log.error(f"/deals: {e}")
        return jsonify({"error": str(e)}), 502

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
