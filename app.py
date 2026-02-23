"""
Nadlan Proxy Server
-------------------
Bridges the browser-based real estate app to nadlan.gov.il's API,
bypassing CORS restrictions that prevent direct browser access.

Routes:
  GET  /health              – health check
  GET  /search?q=...        – search for address / street / neighborhood
  POST /deals               – fetch transactions for a given ObjectID
  GET  /cities?q=...        – autocomplete city names
  GET  /streets?q=&city=... – autocomplete street names (filtered by city)
"""

import os, json, time, logging
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

NADLAN_BASE = "https://www.nadlan.gov.il/Nadlan.REST/Main"
GOV_API     = "https://data.gov.il/api/3/action/datastore_search"

HEADERS = {
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "he-IL,he;q=0.9,en-US;q=0.8",
    "Content-Type":     "application/json;charset=UTF-8",
    "Origin":           "https://www.nadlan.gov.il",
    "Referer":          "https://www.nadlan.gov.il/",
    "User-Agent":       (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── helpers ──────────────────────────────────────────────────────────────────

def nadlan_post(endpoint, payload, retries=3):
    url = f"{NADLAN_BASE}/{endpoint}"
    for attempt in range(retries):
        try:
            r = SESSION.post(url, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"All {retries} attempts to nadlan failed")


def nadlan_get(endpoint, params=None):
    url = f"{NADLAN_BASE}/{endpoint}"
    r = SESSION.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def gov_get(resource_id, q, filters=None, limit=10):
    """Query data.gov.il CKAN datastore."""
    params = {
        "resource_id": resource_id,
        "q":           q,
        "limit":       limit,
    }
    if filters:
        params["filters"] = json.dumps(filters)
    r = requests.get(GOV_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


# ── routes ───────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "nadlan-proxy"})


# ── AUTOCOMPLETE: CITIES ─────────────────────────────────────────────────────
@app.route("/cities")
def cities():
    """
    Return city name suggestions from data.gov.il.
    Query param: q (min 2 chars)
    """
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    try:
        data = gov_get(
            resource_id="b7cf8f14-64a2-4b33-8d4b-edb286fdbd37",
            q=q, limit=8
        )
        records = data.get("result", {}).get("records", [])
        results = []
        for r in records:
            name = r.get("שם_ישוב") or r.get("שם_יישוב") or ""
            code = r.get("סמל_ישוב") or ""
            if name:
                results.append({"name": name, "code": code})
        return jsonify({"results": results})
    except Exception as e:
        log.error(f"/cities error: {e}")
        return jsonify({"error": str(e)}), 502


# ── AUTOCOMPLETE: STREETS ────────────────────────────────────────────────────
@app.route("/streets")
def streets():
    """
    Return street name suggestions from data.gov.il.
    Query params: q (min 2 chars), city_code (optional סמל_ישוב)
    """
    q         = request.args.get("q", "").strip()
    city_code = request.args.get("city_code", "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    try:
        filters = {"סמל_ישוב": city_code} if city_code else None
        data    = gov_get(
            resource_id="a7296d1a-f6c1-4b8a-b6b8-3f7fcf7e5dfd",
            q=q, filters=filters, limit=12
        )
        records = data.get("result", {}).get("records", [])
        seen, results = set(), []
        for r in records:
            name = r.get("שם_רחוב") or r.get("street_name") or ""
            if name and name not in seen:
                seen.add(name)
                results.append({"name": name})
        return jsonify({"results": results})
    except Exception as e:
        log.error(f"/streets error: {e}")
        return jsonify({"error": str(e)}), 502


# ── NADLAN SEARCH ─────────────────────────────────────────────────────────────
@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Missing query parameter 'q'"}), 400
    try:
        data    = nadlan_get("GetSuggestV2", params={"searchText": q, "resultType": 0})
        results = data if isinstance(data, list) else data.get("Results", [])
        return jsonify({"results": results})
    except Exception as e:
        log.error(f"/search error: {e}")
        return jsonify({"error": str(e)}), 502


# ── NADLAN DEALS ──────────────────────────────────────────────────────────────
@app.route("/deals", methods=["POST"])
def deals():
    body = request.get_json(silent=True) or {}
    required = ["ObjectID", "DescLayerID", "ResultLable"]
    missing  = [k for k in required if not body.get(k)]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    payload = {
        "MoreAssestsType":      0,
        "FillterRoomNum":       int(body.get("Rooms", 0)),
        "GridDisplayType":      0,
        "ResultLable":          body["ResultLable"],
        "ResultType":           1,
        "ObjectID":             body["ObjectID"],
        "ObjectIDType":         body.get("ObjectIDType", "text"),
        "ObjectKey":            body.get("ObjectKey", "UNIQ_ID"),
        "DescLayerID":          body["DescLayerID"],
        "Alert":                None,
        "X":                    body.get("X", 0),
        "Y":                    body.get("Y", 0),
        "Gush":                 "",
        "Parcel":               "",
        "showLotParcel":        False,
        "showLotAddress":       False,
        "OriginalSearchString": body.get("ResultLable", ""),
        "MutipuleResults":      False,
        "ResultsOptions":       None,
        "CurrentLavel":         3,
        "Navs":                 [],
        "QueryMapParams":       None,
        "isHistorical":         False,
        "PageNo":               int(body.get("PageNo", 1)),
        "OrderByFilled":        "DEALDATETIME",
        "OrderByDescending":    True,
        "Distance":             0,
    }
    try:
        data = nadlan_post("GetAssestAndDeals", payload)
        return jsonify({
            "deals":      data.get("AllResults", []),
            "totalCount": data.get("TotalCount", 0),
            "pageNo":     payload["PageNo"],
        })
    except Exception as e:
        log.error(f"/deals error: {e}")
        return jsonify({"error": str(e)}), 502


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
