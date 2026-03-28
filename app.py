"""
Nadlan Proxy Server v4
- Cities/Streets/Deals: data.gov.il (open government data)
"""
import os, json, logging
import requests
from flask import Flask, request, jsonify
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
