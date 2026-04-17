"""
EEH BT Dashboard server
Run:  python3 outputs/server.py
Then open http://localhost:8765
"""
import csv
import gzip
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE    = Path(__file__).parent
MATS    = HERE / "matrices"
PERIODS = {
    "typical_week":   MATS / "typical_week_by_mode",
    "weekday_AMpeak": MATS / "weekday_AMpeak_by_mode",
}
MODES      = ["BUS", "CYCLE", "MOTORCYCLE", "PRIVATE_CAR", "RAIL", "SUBWAY", "WALKING"]
PURPOSES   = {0: "All purposes", 1: "Commuting", 2: "Employer Business",
              3: "Education", 4: "Shopping", 5: "Personal Business",
              6: "Social/leisure", 7: "Visit families and friends", 8: "Holiday/day trip"}
PURPOSES_CSV = HERE / "purposes.csv"

GEO_DIR    = HERE.parent / "data" / "raw" / "geo"
GEOJSON    = GEO_DIR / "Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V3_4916445166053426.geojson"

# ── Centroid cache ─────────────────────────────────────────────────────────────
_centroids: dict | None = None   # {msoa_code: {"name": str, "lat": float, "lon": float}}
_centroid_lock = threading.Lock()

def load_centroids() -> dict:
    global _centroids
    with _centroid_lock:
        if _centroids is not None:
            return _centroids
        print("  Loading MSOA centroids from GeoJSON ...", flush=True)
        t0 = time.time()
        result = {}
        with open(GEOJSON) as f:
            data = json.load(f)
        for feat in data["features"]:
            p = feat["properties"]
            result[p["MSOA21CD"]] = {
                "name": p["MSOA21NM"],
                "lat":  p["LAT"],
                "lon":  p["LONG"],
            }
        _centroids = result
        print(f"  Centroids ready: {len(result)} MSOAs in {time.time()-t0:.1f}s", flush=True)
        return result

# ── Index cache ───────────────────────────────────────────────────────────────
# key: (period, mode, purpose)  value: {"path": Path, "index": {msoa: byte_offset}, "header": [msoa,...]}
_cache: dict = {}
_cache_lock = threading.Lock()

def load_purposes() -> list[dict]:
    purposes = [{"value": 0, "label": PURPOSES[0]}]
    if PURPOSES_CSV.exists():
        with open(PURPOSES_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    value = int(row.get("Purpose", "").strip())
                except (TypeError, ValueError):
                    continue
                label = (row.get("Description") or "").strip()
                if label:
                    purposes.append({"value": value, "label": label})
    else:
        for value in sorted(k for k in PURPOSES.keys() if k != 0):
            purposes.append({"value": value, "label": PURPOSES[value]})
    return purposes

def csv_path(period: str, mode: str, purpose: int) -> Path:
    folder = PERIODS[period]
    if purpose == 0:
        return folder / f"OD_matrix_{mode}_adjusted.csv"
    return folder / f"OD_matrix_{mode}_adjusted_by_purpose{purpose}.csv"

def build_index(path: Path) -> tuple[dict, list]:
    """Scan file once; record byte offset of each row and parse header."""
    print(f"  Building index for {path.name} ...", flush=True)
    t0 = time.time()
    index = {}   # origin_msoa -> byte offset of that line
    header = []
    with open(path, "rb") as f:
        first = True
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            if first:
                header = line.decode().strip().split(",")
                first = False
            else:
                comma = line.index(b",")
                msoa = line[:comma].decode().strip()
                index[msoa] = offset
    print(f"  Done in {time.time()-t0:.1f}s — {len(index)} origins", flush=True)
    return index, header[1:]   # header without 'origin_msoa'

def get_index(period: str, mode: str, purpose: int):
    key = (period, mode, purpose)
    with _cache_lock:
        if key not in _cache:
            path = csv_path(period, mode, purpose)
            if not path.exists():
                return None
            idx, hdr = build_index(path)
            _cache[key] = {"path": path, "index": idx, "header": hdr}
        return _cache[key]

def lookup_flow(period: str, mode: str, purpose: int, origin: str, dest: str) -> float | None:
    entry = get_index(period, mode, purpose)
    if entry is None:
        return None
    idx   = entry["index"]
    hdr   = entry["header"]
    path  = entry["path"]
    if origin not in idx:
        return None
    if dest not in hdr:
        return None
    col_i = hdr.index(dest)
    with open(path, "rb") as f:
        f.seek(idx[origin])
        line = f.readline().decode().strip()
    vals = line.split(",")
    try:
        return float(vals[col_i + 1])   # +1 for the origin_msoa column
    except (IndexError, ValueError):
        return None

def lookup_top(period: str, mode: str, purpose: int, origin: str, n: int = 15) -> list:
    """Return top N destinations from an origin, sorted descending."""
    entry = get_index(period, mode, purpose)
    if entry is None:
        return []
    idx  = entry["index"]
    hdr  = entry["header"]
    path = entry["path"]
    if origin not in idx:
        return []
    with open(path, "rb") as f:
        f.seek(idx[origin])
        line = f.readline().decode().strip()
    vals = line.split(",")[1:]   # skip origin label
    pairs = []
    for i, v in enumerate(vals):
        try:
            val = float(v)
            if val > 0:
                pairs.append((hdr[i], val))
        except ValueError:
            pass
    pairs.sort(key=lambda x: x[1], reverse=True)
    return [{"dest": d, "flow": round(v, 2)} for d, v in pairs[:n]]

def get_msoa_list(period: str, mode: str, purpose: int) -> list:
    entry = get_index(period, mode, purpose)
    if entry is None:
        return []
    return list(entry["index"].keys())

def lookup_top_with_coords(period: str, mode: str, purpose: int, origin: str, n: int = 50) -> list:
    """Top N destinations enriched with centroid coordinates."""
    tops = lookup_top(period, mode, purpose, origin, n)
    centroids = load_centroids()
    result = []
    for item in tops:
        c = centroids.get(item["dest"])
        if c:
            result.append({**item, "name": c["name"], "lat": c["lat"], "lon": c["lon"]})
    return result

# ── HTTP handler ──────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass   # suppress default access log

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, path: Path):
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        qs     = parse_qs(parsed.query)

        def q(key, default=""):
            return qs.get(key, [default])[0]

        path = parsed.path.rstrip("/") or "/"

        if path == "/" or path == "/dashboard":
            self.send_html(HERE / "dashboard.html")

        elif path == "/od":
            self.send_html(HERE / "od_tool.html")

        elif path == "/map":
            self.send_html(HERE / "map.html")

        elif path == "/api/msoas":
            period  = q("period", "typical_week")
            mode    = q("mode", "PRIVATE_CAR")
            purpose = int(q("purpose", "0"))
            msoas   = get_msoa_list(period, mode, purpose)
            self.send_json({"msoas": msoas})

        elif path == "/api/flow":
            period  = q("period", "typical_week")
            mode    = q("mode", "PRIVATE_CAR")
            purpose = int(q("purpose", "0"))
            origin  = q("origin")
            dest    = q("dest")
            if not origin or not dest:
                self.send_json({"error": "origin and dest required"}, 400)
                return
            val = lookup_flow(period, mode, purpose, origin, dest)
            if val is None:
                self.send_json({"error": f"Not found: {origin} -> {dest}"}, 404)
            else:
                self.send_json({"origin": origin, "dest": dest, "mode": mode,
                                "purpose": purpose, "period": period,
                                "flow": round(val, 4)})

        elif path == "/api/top":
            period  = q("period", "typical_week")
            mode    = q("mode", "PRIVATE_CAR")
            purpose = int(q("purpose", "0"))
            origin  = q("origin")
            n       = int(q("n", "15"))
            if not origin:
                self.send_json({"error": "origin required"}, 400)
                return
            results = lookup_top(period, mode, purpose, origin, n)
            self.send_json({"origin": origin, "mode": mode, "purpose": purpose,
                            "period": period, "top": results})

        elif path == "/api/centroids":
            centroids = load_centroids()
            # Return as lightweight list for the map
            out = [{"code": k, "name": v["name"], "lat": v["lat"], "lon": v["lon"]}
                   for k, v in centroids.items()]
            self.send_json({"centroids": out})

        elif path == "/api/purposes":
            self.send_json({"purposes": load_purposes()})

        elif path == "/api/flows-map":
            period  = q("period", "typical_week")
            mode    = q("mode", "PRIVATE_CAR")
            purpose = int(q("purpose", "0"))
            origin  = q("origin")
            n       = int(q("n", "50"))
            if not origin:
                self.send_json({"error": "origin required"}, 400)
                return
            tops = lookup_top_with_coords(period, mode, purpose, origin, n)
            centroids = load_centroids()
            orig_c = centroids.get(origin)
            self.send_json({
                "origin": origin,
                "origin_name": orig_c["name"] if orig_c else origin,
                "origin_lat":  orig_c["lat"]  if orig_c else None,
                "origin_lon":  orig_c["lon"]  if orig_c else None,
                "mode": mode, "purpose": purpose, "period": period,
                "flows": tops
            })

        else:
            self.send_response(404)
            self.end_headers()


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    server = HTTPServer(("localhost", port), Handler)
    print(f"Server running at http://localhost:{port}")
    print(f"  Dashboard:  http://localhost:{port}/")
    print(f"  OD Tool:    http://localhost:{port}/od")
    print(f"  Map:        http://localhost:{port}/map")
    print(f"  Purposes:   http://localhost:{port}/api/purposes")
    print("Note: first lookup per file builds an index (takes ~10-30s for large files)")
    server.serve_forever()
