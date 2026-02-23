"""
Nadlan Proxy Server
-------------------
Bridges the browser-based real estate app to nadlan.gov.il's API,
bypassing CORS restrictions that prevent direct browser access.

Routes:
  GET  /health           – health check
  GET  /search?q=...     – search for address / street / neighborhood
  POST /deals            – fetch transactions for a given ObjectID
"""

import os, json, time, logging
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # allow all origins (our HTML app can call this freely)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

NADLAN_BASE = "https://www.nadlan.gov.il/Nadlan.REST/Main"

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


# ── helpers ─────────────────────────────────────────────────────────────────

def nadlan_post(endpoint: str, payload: dict, retries: int = 3) -> dict:
    """POST to nadlan.gov.il with simple retry logic."""
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


def nadlan_get(endpoint: str, params: dict = None) -> dict:
    """GET to nadlan.gov.il."""
    url = f"{NADLAN_BASE}/{endpoint}"
    r = SESSION.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ── routes ───────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "nadlan-proxy"})


@app.route("/search")
def search():
    """
    Search nadlan for an address / street / city.
    Returns a list of result objects each with ObjectID, ObjectIDType,
    ResultLable, DescLayerID, X, Y – everything needed to fetch deals.

    Query param:  q  (required) – free-text Hebrew search string
    """
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Missing query parameter 'q'"}), 400

    try:
        data = nadlan_get("GetSuggestV2", params={"searchText": q, "resultType": 0})
        # nadlan returns a list directly
        results = data if isinstance(data, list) else data.get("Results", [])
        return jsonify({"results": results})
    except Exception as e:
        log.error(f"/search error: {e}")
        return jsonify({"error": str(e)}), 502


@app.route("/deals", methods=["POST"])
def deals():
    """
    Fetch real estate transactions for a given location.

    Expected JSON body:
    {
      "ObjectID":      "65210861",          // from /search
      "ObjectIDType":  "text",
      "ObjectKey":     "UNIQ_ID",
      "DescLayerID":   "STREETS_LAYER",
      "ResultLable":   "שי עגנון, ראשון לציון",
      "X":             182567.23,           // optional
      "Y":             643770.91,           // optional
      "PageNo":        1,                   // 1-based page number
      "Rooms":         0                    // 0 = all rooms
    }
    """
    body = request.get_json(silent=True) or {}

    required = ["ObjectID", "DescLayerID", "ResultLable"]
    missing = [k for k in required if not body.get(k)]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    payload = {
        "MoreAssestsType":    0,
        "FillterRoomNum":     int(body.get("Rooms", 0)),
        "GridDisplayType":    0,
        "ResultLable":        body["ResultLable"],
        "ResultType":         1,
        "ObjectID":           body["ObjectID"],
        "ObjectIDType":       body.get("ObjectIDType", "text"),
        "ObjectKey":          body.get("ObjectKey", "UNIQ_ID"),
        "DescLayerID":        body["DescLayerID"],
        "Alert":              None,
        "X":                  body.get("X", 0),
        "Y":                  body.get("Y", 0),
        "Gush":               "",
        "Parcel":             "",
        "showLotParcel":      False,
        "showLotAddress":     False,
        "OriginalSearchString": body.get("ResultLable", ""),
        "MutipuleResults":    False,
        "ResultsOptions":     None,
        "CurrentLavel":       3,
        "Navs":               [],
        "QueryMapParams":     None,
        "isHistorical":       False,
        "PageNo":             int(body.get("PageNo", 1)),
        "OrderByFilled":      "DEALDATETIME",
        "OrderByDescending":  True,
        "Distance":           0,
    }

    try:
        data = nadlan_post("GetAssestAndDeals", payload)
        # The API returns AllResults (list of deals) + TotalCount
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
