"""
Nadlan Proxy Server v3
- Cities/Streets: data.gov.il
- Deals: nadlan.gov.il CloudFront API
Routes:
  GET /health
  GET /cities?q=
  GET /streets?q=&city_code=
  GET /deals?street=&city=&limit=
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
CITIES_RESOURCE  = "b7cf8f14-64a2-4b33-8d4b-edb286fdbd37"
STREETS_RESOURCE = "a7296d1a-f6c1-4b8a-b6b8-3f7fcf7e5dfd"
NADLAN_API       = "https://x4006fhmy5.execute-api.il-central-1.amazonaws.com/api/deal"

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "NadlanProxy/3.0"})

NADLAN_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.nadlan.gov.il",
    "Referer": "https://www.nadlan.gov.il/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

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
    return jsonify({"status": "ok", "service": "nadlan-proxy-v3"})

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
        data = gov_get(STREETS_RESOURCE, filters=filters if filters else None, q=q, limit=10)
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

    # Try nadlan.gov.il CloudFront API
    try:
        payload = {
            "cityName": city,
            "streetName": street,
            "houseNum": "",
            "pageNum": 1
        }
        r = requests.post(
            NADLAN_API,
            json=payload,
            headers=NADLAN_HEADERS,
            timeout=20
        )
        r.raise_for_status()
        data = r.json()
        log.info("nadlan response type=%s keys=%s", type(data).__name__,
                 list(data.keys())[:10] if isinstance(data, dict) else str(data)[:200])

        # Handle list or dict response
        if isinstance(data, list):
            raw = data
        elif isinstance(data, dict):
            inner = (data.get("data") or data.get("Data") or
                     data.get("allDeals") or data.get("deals") or
                     data.get("results") or data.get("Records"))
            log.info("nadlan inner type=%s val=%s", type(inner).__name__, str(inner)[:300])
            if isinstance(inner, list):
                raw = inner
            elif isinstance(inner, dict):
                raw = (inner.get("allDeals") or inner.get("deals") or
                       inner.get("data") or inner.get("results") or [])
                if not isinstance(raw, list):
                    for v in inner.values():
                        if isinstance(v, list):
                            raw = v
                            break
                    else:
                        raw = []
            else:
                raw = []
        else:
            raw = []
        log.info("nadlan raw deals count=%d", len(raw))

        deals_out = []
        for d in list(raw)[:limit]:
            try:
                price = float(d.get("DEALAMOUNT") or d.get("price") or 0)
                size  = float(d.get("ASSETROOMNUM") or d.get("DEALNATUREDESCRIPTION") or 0)
                # size might be rooms, look for area
                area  = float(d.get("FLOORNO") or d.get("area") or d.get("DEALSIZE") or 0)
                rooms = d.get("ASSETROOMNUM") or d.get("rooms") or ""
                floor = d.get("FLOORNO") or d.get("floor") or ""
                date  = d.get("DEALDATETIME") or d.get("DEALDATE") or d.get("date") or ""
                addr  = d.get("STREETNAME") or d.get("address") or street
                house = d.get("HOUSENUM") or d.get("housenum") or ""
                if house:
                    addr = addr + " " + str(house)
                ppm   = round(price / area) if area > 0 and price > 0 else 0
                # Format date
                if date and len(str(date)) > 10:
                    date = str(date)[:10]
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
                log.warning("parse deal error: %s", pe)
                continue

        log.info("nadlan API returned %d deals for %s %s", len(deals_out), street, city)
        return jsonify({"deals": deals_out, "source": "nadlan.gov.il", "raw_count": len(raw)})

    except Exception as e:
        log.error("nadlan API error: %s", e)
        return jsonify({"deals": [], "error": str(e), "source": "nadlan-failed"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
