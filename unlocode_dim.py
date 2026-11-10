# Databricks notebook source

# MAGIC %md
# MAGIC # UN/LOCODE location dim (market_map rebuild)
# MAGIC
# MAGIC Builds one validated coordinate per consultant rate location (UN/LOCODE ports, "CITY, POSTAL" trucking
# MAGIC points, free-text cities, Drewry port sub-codes) and re-derives the pair-level `market_map` from it.
# MAGIC
# MAGIC **Source priority** (first candidate that passes validation and is not outvoted wins):
# MAGIC 1. `manual` – reviewed overrides (cell *Manual overrides*)
# MAGIC 2. `unlocode` – UN/LOCODE coordinates, cross-checked against port gazetteers
# MAGIC 3. `wpi` / `searoute` / `seaports` – port gazetteers (NGA World Port Index when reachable, Eurostat searoute ports, sea-ports)
# MAGIC 4. `legacy` – previous market_map point, only if it passes validation
# MAGIC 5. `postal` – GeoNames postal code (exact)
# MAGIC 6. `postal_prefix` – GeoNames postal codes sharing the longest prefix
# MAGIC 7. `city` – GeoNames cities500 / postal place names / admin-1 fallback
# MAGIC
# MAGIC **Validation of every candidate and final point**
# MAGIC - point-in-polygon against Natural Earth admin-0 for its country (10 km tolerance land, 30 km ports)
# MAGIC - shared-point detection (one coordinate used by differently named places = geocoder fallback)
# MAGIC - source disagreement > 50 km (a candidate with no support is dropped when ≥2 other independent sources agree elsewhere)
# MAGIC - ocean ports > 40 km from the coast without a port-gazetteer confirmation
# MAGIC
# MAGIC **Output**: `cdl_mda_prod.internal_material_data_asset.unlocode_dim` (one row per location) plus CSVs in the
# MAGIC output volume: pair map in legacy `Geo_Enriched` format, manual review list and validation report.
# MAGIC Cells whose first line is `# CORE` are plain Python (no Spark) so the same code runs locally
# MAGIC (`market_map/run_local.py`).

# COMMAND ----------

# MAGIC %pip install shapely==2.0.6 unidecode==1.3.8

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_TABLE = "cdl_mda_prod.internal_material_data_asset.unlocode_dim"
PAIR_MAP_TABLE = None  # set a table name to also persist the pair map, e.g. "...market_map"

VOLUME_DIR = "/Volumes/cdl_mda_prod/internal_material_data_asset/market_map"
REF_DIR = f"{VOLUME_DIR}/reference"   # downloaded reference data (cached)
IN_DIR = f"{VOLUME_DIR}/input"        # uploaded inputs
OUT_DIR = f"{VOLUME_DIR}/output"      # CSV outputs
REFRESH_REFS = False                  # True = re-download reference data

# Inputs: a /Volumes path to a CSV, or a UC table name
CONSULTANT_LOCATIONS = f"{IN_DIR}/All_consultant_locations.csv"
UNLOCODE = f"{IN_DIR}/UNLOCODE_with_coordinates.csv"
LEGACY_MARKET_MAP = f"{IN_DIR}/Geo_Enriched.csv"

REF_URLS = {
    "gn_postal_all.zip": "https://download.geonames.org/export/zip/allCountries.zip",
    "gn_postal_GB.zip": "https://download.geonames.org/export/zip/GB_full.csv.zip",
    "gn_postal_NL.zip": "https://download.geonames.org/export/zip/NL_full.csv.zip",
    "gn_postal_CA.zip": "https://download.geonames.org/export/zip/CA_full.csv.zip",
    "gn_cities500.zip": "https://download.geonames.org/export/dump/cities500.zip",
    "gn_admin1.txt": "https://download.geonames.org/export/dump/admin1CodesASCII.txt",
    "ne_admin0.geojson": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_countries.geojson",
    "ne_coastline.geojson": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_coastline.geojson",
    "wpi.csv": "https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv",
    "searoute_ports.geojson": "https://raw.githubusercontent.com/genthalili/searoute-py/main/searoute/data/ports.geojson",
    "seaports.json": "https://raw.githubusercontent.com/marchah/sea-ports/master/lib/ports.json",
}
OPTIONAL_REFS = {"wpi.csv", "searoute_ports.geojson", "seaports.json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download reference data to the volume

# COMMAND ----------

import os
import urllib.request
import zipfile


def _download(name, url):
    dst = f"{REF_DIR}/{name}"
    if os.path.exists(dst) and not REFRESH_REFS:
        return dst
    tmp = f"/local_disk0/tmp/{name}"
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "market-map-build"})
    with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
        f.write(r.read())
    with open(tmp, "rb") as f:
        head = f.read(200).lower()
    if b"<html" in head or b"<!doctype" in head:
        raise RuntimeError(f"{url} returned an HTML page (service unavailable?)")
    dbutils.fs.cp(f"file:{tmp}", dst)
    return dst


os.makedirs(REF_DIR, exist_ok=True)
local = {}
for name, url in REF_URLS.items():
    try:
        local[name] = _download(name, url)
        print(f"ok   {name}")
    except Exception as e:  # optional port gazetteers may be down; the build then runs without them
        if name not in OPTIONAL_REFS:
            raise
        print(f"SKIP {name}: {e}")

unz = "/local_disk0/tmp/market_map_refs"
os.makedirs(unz, exist_ok=True)
for name in ["gn_postal_all.zip", "gn_postal_GB.zip", "gn_postal_NL.zip", "gn_postal_CA.zip", "gn_cities500.zip"]:
    zipfile.ZipFile(local[name]).extractall(unz)


def _input(src, name):
    """CSV path on a volume, or a UC table exported to a local CSV."""
    if src.startswith("/"):
        return src
    out = f"/local_disk0/tmp/{name}.csv"
    spark.read.table(src).toPandas().to_csv(out, index=False)
    return out


PATHS = {
    "consultant": _input(CONSULTANT_LOCATIONS, "consultant"),
    "unlocode": _input(UNLOCODE, "unlocode"),
    "legacy": _input(LEGACY_MARKET_MAP, "legacy"),
    "gn_postal": [f"{unz}/allCountries.txt", f"{unz}/GB_full.txt", f"{unz}/NL_full.txt", f"{unz}/CA_full.txt"],
    "gn_cities": f"{unz}/cities500.txt",
    "gn_admin1": local["gn_admin1.txt"],
    "ne_admin0": local["ne_admin0.geojson"],
    "ne_coast": local["ne_coastline.geojson"],
    "wpi": local.get("wpi.csv"),
    "searoute": local.get("searoute_ports.geojson"),
    "seaports": local.get("seaports.json"),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core: helpers and reference loaders

# COMMAND ----------

# CORE
import difflib
import json
import math
import os
import re
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from shapely.geometry import Point, shape
from shapely.ops import nearest_points, unary_union
from shapely.prepared import prep
from shapely.strtree import STRtree
from unidecode import unidecode

MAX_DISAGREE_KM = 50.0   # two sources further apart than this disagree
LAND_TOL_KM = 10.0       # allowed distance outside the country polygon (land points)
PORT_TOL_KM = 30.0       # allowed distance outside the country polygon (ports, offshore terminals)
PORT_INLAND_KM = 40.0    # ocean port further than this from the coast needs a gazetteer confirmation
SHARED_DECIMALS = 3      # ~100 m: points rounded to this are "the same point"
SHARED_SUPPORT_KM = 10.0 # a legacy point shared with one other place needs a source this close
APPROX_SOURCES = ("postal_prefix", "city")  # area centroids: expected to coincide, not checked for sharing

PRIORITY = ["manual", "unlocode", "wpi", "searoute", "seaports", "legacy", "postal", "postal_prefix", "city"]
PORT_REFS = ("wpi", "searoute", "seaports")
# Overseas territories that Natural Earth draws inside the parent polygon
COUNTRY_ALIASES = {"RE": ["FR"], "GP": ["FR"], "MQ": ["FR"], "GF": ["FR"], "YT": ["FR"], "PM": ["FR"],
                   "BL": ["FR"], "MF": ["FR"], "SJ": ["NO"], "BQ": ["NL"], "IC": ["ES"]}


def hav(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742.0 * math.asin(min(1.0, math.sqrt(a)))


_ABBR = {"ST": "SAINT", "STE": "SAINTE", "MT": "MOUNT", "FT": "FORT", "PTO": "PUERTO", "S": "SAN"}


def norm(s):
    if s is None:
        return ""
    s = unidecode(str(s)).upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)


def norm_city(s):
    toks = norm(s).split()
    return " ".join(_ABBR.get(t, t) for t in toks)


_STOP = {"DIST", "DISTRICT", "CO", "COUNTY", "PROVINCE", "PROV", "CITY", "INDUSTRIAL", "PARQUE", "PARK", "ZONA",
         "ZONE", "ZI", "PORT", "PT", "APT", "INTERNATIONAL", "AIRPORT", "TERMINAL", "PRAD", "PRADESH", "STATE",
         "CEDEX", "PUERTO", "PORTO"}
_PORT_WORDS = {"PORT", "PT", "APT", "TERMINAL", "INTERNATIONAL", "AIRPORT", "CEDEX"}
_NOT_PLACES = {"USA", "US", "UNITED STATES", "ETATS UNIS", "NULL", "NONE", "NA", "N A"}


def name_variants(names):
    """Ordered name variants: (variant, loose) - loose = derived by dropping words."""
    out = {}  # variant -> loose (dict keeps insertion order)

    def add(v, loose):
        if v and len(v) >= 3 and v not in _NOT_PLACES:
            out[v] = out.get(v, True) and loose

    loose_later = []
    for nm in names:
        if not nm or str(nm).strip().lower() == "null":
            continue
        parts = [nm] + [p for p in re.split(r"[,/()]", str(nm)) if p.strip()]
        for i, part in enumerate(parts):
            n = norm_city(part)
            add(n, i > 0)
            n2 = " ".join(t for t in n.split() if not t.isdigit())
            add(n2, i > 0)
            add(" ".join(t for t in n2.split() if t not in _PORT_WORDS), i > 0)
            n3 = " ".join(t for t in n2.split() if t not in _STOP)
            add(n3, True)
            toks = n3.split()
            if len(toks) > 1:
                loose_later += [t for t in (toks[-1], toks[0]) if len(t) >= 4]
    for t in loose_later:
        add(t, True)
    return list(out.items())


def name_match(place_norm, variants):
    if not place_norm:
        return False
    ptoks = set(place_norm.split())
    for v, _ in variants:
        if v == place_norm or v.replace(" ", "") == place_norm.replace(" ", ""):
            return True
        vt = set(v.split())
        if len(v) >= 4 and (vt <= ptoks or (ptoks <= vt and len(place_norm) >= 4)):
            return True
        if difflib.SequenceMatcher(None, v, place_norm).ratio() >= 0.85:
            return True
    return False


def parse_unlocode_ll(s):
    m = re.fullmatch(r"(\d{2})(\d{2})([NS])\s+(\d{3})(\d{2})([EW])", str(s).strip())
    if not m or int(m[2]) >= 60 or int(m[5]) >= 60:
        return None
    lat = int(m[1]) + int(m[2]) / 60
    lon = int(m[4]) + int(m[5]) / 60
    if lat > 90 or lon > 180:
        return None
    return (-lat if m[3] == "S" else lat, -lon if m[6] == "W" else lon)


def _median_point(lats, lons):
    lat, lon = float(np.median(lats)), float(np.median(lons))
    spread = max((hav(lat, lon, a, b) for a, b in zip(lats, lons)), default=0.0)
    return lat, lon, spread


class Countries:
    """Natural Earth admin-0 polygons keyed by ISO alpha-2."""

    def __init__(self, path):
        with open(path, encoding="utf-8") as f:
            gj = json.load(f)
        parts = defaultdict(list)
        for ft in gj["features"]:
            p = ft["properties"]
            code = p.get("ISO_A2_EH") if p.get("ISO_A2_EH") not in (None, "-99") else p.get("ISO_A2")
            if code in (None, "-99"):
                continue
            parts[code].append(shape(ft["geometry"]))
        self.geom = {k: unary_union(v) for k, v in parts.items()}
        self.prep = {k: prep(g) for k, g in self.geom.items()}
        self._codes = list(self.geom)
        self._tree = STRtree([self.geom[c] for c in self._codes])

    def check(self, cc, lat, lon):
        """('inside'|'outside'|'no_polygon', km outside)."""
        codes = [c for c in [cc] + COUNTRY_ALIASES.get(cc, []) if c in self.geom]
        if not codes:
            return "no_polygon", None
        pt = Point(lon, lat)
        if any(self.prep[c].contains(pt) for c in codes):
            return "inside", 0.0
        best = None
        for c in codes:
            p1, _ = nearest_points(self.geom[c], pt)
            d = hav(lat, lon, p1.y, p1.x)
            best = d if best is None else min(best, d)
        return "outside", best

    def locate(self, lat, lon):
        pt = Point(lon, lat)
        for i in self._tree.query(pt):
            if self.prep[self._codes[i]].contains(pt):
                return self._codes[i]
        return None


class Coast:
    def __init__(self, path):
        with open(path, encoding="utf-8") as f:
            gj = json.load(f)
        lines = []
        for ft in gj["features"]:
            g = shape(ft["geometry"])
            lines.extend(getattr(g, "geoms", [g]))
        self.lines = lines
        self.tree = STRtree(lines)

    def km(self, lat, lon):
        pt = Point(lon, lat)
        cand = self.tree.query(pt.buffer(2.0))
        if len(cand) == 0:
            cand = [self.tree.nearest(pt)]
        best = None
        for i in cand:
            p1, _ = nearest_points(self.lines[i], pt)
            d = hav(lat, lon, p1.y, p1.x)
            best = d if best is None else min(best, d)
        return best


def load_unlocode(path):
    u = pd.read_csv(path, dtype=str, keep_default_na=False)
    out = {}
    for r in u.itertuples(index=False):
        code = (r.country_code + r.loc_code).upper()
        ll = parse_unlocode_ll(r.long_lat)
        if code not in out or (out[code]["lat"] is None and ll):
            out[code] = {"name": r.loc_name, "function": r.function, "status": r.status,
                         "lat": ll[0] if ll else None, "lon": ll[1] if ll else None}
    return out


def load_port_refs(paths):
    refs = {}
    if paths.get("wpi") and os.path.exists(paths["wpi"]):
        w = pd.read_csv(paths["wpi"], dtype=str, keep_default_na=False)
        if {"UN/LOCODE", "Latitude", "Longitude"} <= set(w.columns) and len(w) > 1000:
            d = {}
            for code, lat, lon in zip(w["UN/LOCODE"], w["Latitude"], w["Longitude"]):
                code = re.sub(r"\s", "", code).upper()
                if len(code) == 5 and code not in d:
                    try:
                        d[code] = (float(lat), float(lon))
                    except ValueError:
                        pass
            refs["wpi"] = d
    if paths.get("searoute") and os.path.exists(paths["searoute"]):
        with open(paths["searoute"], encoding="utf-8") as f:
            gj = json.load(f)
        d = {}
        for ft in gj["features"]:
            code = (ft["properties"].get("port") or "").upper()
            lon, lat = ft["geometry"]["coordinates"][:2]
            if len(code) == 5 and code not in d:
                d[code] = (float(lat), float(lon))
        refs["searoute"] = d
    if paths.get("seaports") and os.path.exists(paths["seaports"]):
        with open(paths["seaports"], encoding="utf-8") as f:
            sp = json.load(f)
        d = {}
        for code, v in sp.items():
            c = v.get("coordinates")
            if c and len(code) == 5:
                d[code.upper()] = (float(c[1]), float(c[0]))
        refs["seaports"] = d
    return refs


class Postal:
    COLS = ["cc", "code", "place", "a1", "a1c", "a2", "a2c", "a3", "a3c", "lat", "lon", "acc"]

    def __init__(self, paths, ccs):
        frames = []
        for p in paths:
            df = pd.read_csv(p, sep="\t", header=None, names=self.COLS, dtype=str,
                             keep_default_na=False, quoting=3, usecols=range(12))
            frames.append(df[df.cc.isin(ccs)])
        df = pd.concat(frames, ignore_index=True)
        df["lat"] = pd.to_numeric(df.lat, errors="coerce")
        df["lon"] = pd.to_numeric(df.lon, errors="coerce")
        df = df.dropna(subset=["lat", "lon"])
        df["key"] = df.code.str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)
        df["pn"] = df.place.map(norm_city)
        df = df.drop_duplicates(["cc", "key", "pn", "lat", "lon"]).reset_index(drop=True)
        self.df = df
        self.idx = {k: np.asarray(v) for k, v in df.groupby(["cc", "key"]).indices.items()}
        self.keys = {cc: sorted(g.key.unique()) for cc, g in df.groupby("cc")}
        self.modal_len = df.groupby("cc").key.agg(lambda s: int(s.str.len().mode().iloc[0])).to_dict()
        self.place_idx = {k: np.asarray(v) for k, v in df.groupby(["cc", "pn"]).indices.items()}

    @staticmethod
    def clean(cc, raw):
        p = unidecode(str(raw or "")).upper().strip()
        if not re.search(r"\d", p):
            return None
        if cc == "US":
            m = re.search(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)", p) or re.search(r"(?<!\d)(\d{3,4})(?!\d)", p)
            return m.group(1).zfill(5) if m else None
        if cc == "CA":
            m = re.search(r"[A-Z]\d[A-Z]\s*\d[A-Z]\d", p)
            return re.sub(r"\s", "", m.group(0)) if m else re.sub(r"[^A-Z0-9]", "", p)
        return re.sub(r"[^A-Z0-9]", "", p)

    def _pick(self, rows, variants):
        sub = self.df.iloc[rows]
        hit = sub[sub.pn.map(lambda pn: name_match(pn, variants))] if variants else sub.iloc[0:0]
        use = hit if len(hit) else sub
        lat, lon, spread = _median_point(use.lat.tolist(), use.lon.tolist())
        return {"lat": lat, "lon": lon, "spread_km": spread, "name_match": bool(len(hit)),
                "place": use.place.iloc[0], "n": len(use)}

    def lookup(self, cc, raw, variants):
        key = self.clean(cc, raw)
        if not key or cc not in self.keys:
            return None
        tries = [key]
        if key.isdigit() and len(key) == self.modal_len.get(cc, 0) - 1:
            tries.append("0" + key)
        for k in tries:
            rows = self.idx.get((cc, k))
            if rows is not None:
                c = self._pick(rows, variants)
                c.update(src="postal", detail=f"code={k}")
                return c
        keys = self.keys[cc]
        for L in range(len(key) - 1, max(2, len(key) - 3) - 1, -1):
            pre = key[:L]
            i = bisect_left(keys, pre)
            matched = []
            while i < len(keys) and keys[i].startswith(pre):
                matched.append(keys[i])
                i += 1
            if matched:
                rows = np.concatenate([self.idx[(cc, k)] for k in matched])
                c = self._pick(rows, variants)
                c.update(src="postal_prefix", detail=f"prefix={pre} ({len(matched)} codes)")
                return c
        return None


class Cities:
    def __init__(self, cities_path, admin1_path, ccs, postal):
        cols = ["gid", "name", "ascii", "alt", "lat", "lon", "fcl", "fcode", "cc", "cc2", "a1", "a2", "a3", "a4",
                "pop", "elev", "dem", "tz", "mod"]
        df = pd.read_csv(cities_path, sep="\t", header=None, names=cols, dtype=str, keep_default_na=False, quoting=3)
        df = df[df.cc.isin(ccs)].copy()
        df["lat"] = df.lat.astype(float)
        df["lon"] = df.lon.astype(float)
        df["pop"] = pd.to_numeric(df["pop"], errors="coerce").fillna(0)
        self.df = df.reset_index(drop=True)
        idx = defaultdict(set)
        for i, (cc, name, ascii_, alt) in enumerate(zip(self.df.cc, self.df.name, self.df.ascii, self.df.alt)):
            for nm in [name, ascii_] + (alt.split(",") if alt else []):
                n = norm_city(nm)
                if len(n) >= 3:
                    idx[(cc, n)].add(i)
                    idx[(cc, n.replace(" ", ""))].add(i)
        self.idx = idx
        self.postal = postal
        a1 = pd.read_csv(admin1_path, sep="\t", header=None, names=["code", "name", "ascii", "gid"],
                         dtype=str, keep_default_na=False, quoting=3)
        top = self.df.sort_values("pop", ascending=False).drop_duplicates(["cc", "a1"])
        top_by = {(r.cc, r.a1): r for r in top.itertuples()}
        self.admin1 = {}
        self.a1labels = defaultdict(set)
        for r in a1.itertuples():
            cc, code = r.code.split(".", 1)
            for nm in (r.name, r.ascii):
                self.a1labels[(cc, code)].add(norm_city(nm))
            if code.isalpha() and len(code) == 2:
                self.a1labels[(cc, code)].add(code)
            if (cc, code) in top_by:
                for nm in (r.name, r.ascii):
                    self.admin1[(cc, norm_city(nm))] = top_by[(cc, code)]

    def _region_ok(self, cc, a1, tokens):
        for lab in self.a1labels.get((cc, a1), ()):
            lt = lab.split()
            if lt and (set(lt) <= tokens or (len(lt[0]) >= 4 and lt[0] in tokens)):
                return True
        return False

    def lookup(self, cc, variants, anchors, tokens=frozenset()):
        """tokens: all words of the location text; a word naming the city's admin-1 region confirms the match."""
        for v, loose in variants:
            rows = self.idx.get((cc, v)) or self.idx.get((cc, v.replace(" ", "")))
            if rows:
                sub = self.df.iloc[sorted(rows)]
                rest = set(tokens) - set(v.split())
                ok = sub.a1.map(lambda a: self._region_ok(cc, a, rest))
                region = bool(ok.any())
                if region:
                    sub = sub[ok]
                c = self._choose(sub.lat.tolist(), sub.lon.tolist(), sub["pop"].tolist(), sub.name.tolist(),
                                 anchors, f"cities500:{v}{' +region' if region else ''}", loose and not region)
                return c
        for v, loose in variants:
            rows = self.postal.place_idx.get((cc, v))
            if rows is not None:
                sub = self.postal.df.iloc[rows]
                lat, lon, spread = _median_point(sub.lat.tolist(), sub.lon.tolist())
                if spread <= 2 * MAX_DISAGREE_KM:
                    return {"src": "city", "lat": lat, "lon": lon, "detail": f"postal_place:{v}",
                            "place": sub.place.iloc[0], "n_homonyms": 1, "loose": loose, "anchored": False}
        for v, _ in variants:
            r = self.admin1.get((cc, v))
            if r is not None:
                return {"src": "city", "lat": r.lat, "lon": r.lon, "detail": f"admin1:{v}->{r.name}",
                        "place": r.name, "n_homonyms": 1, "loose": True, "anchored": False}
        return None

    @staticmethod
    def _choose(lats, lons, pops, names, anchors, detail, loose):
        # collapse homonyms that are the same place (within 25 km)
        order = sorted(range(len(lats)), key=lambda i: -pops[i])
        groups = []
        for i in order:
            if not any(hav(lats[i], lons[i], lats[g], lons[g]) <= 25 for g in groups):
                groups.append(i)
        anchored = False
        pick = groups[0]
        if len(groups) > 1 and anchors:
            dists = [min(hav(lats[g], lons[g], a, b) for a, b in anchors) for g in groups]
            if min(dists) <= MAX_DISAGREE_KM:
                pick = groups[int(np.argmin(dists))]
                anchored = True
        return {"src": "city", "lat": lats[pick], "lon": lons[pick], "detail": detail, "place": names[pick],
                "n_homonyms": len(groups), "loose": loose, "anchored": anchored,
                "dominant": len(groups) == 1 or pops[groups[0]] >= 10 * max(1, pops[groups[1]])}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core: manual overrides
# MAGIC Reviewed coordinates. `validate_cc` overrides the country used for validation (bad country code in source).

# COMMAND ----------

# CORE
MANUAL_OVERRIDES = [
    # cc, loc, lat, lon, validate_cc, note
    ("AS", "ASPPG", -14.2766, -170.6900, None, "UN/LOCODE 1416N has the wrong hemisphere; Pago Pago harbour is 14.28S"),
    ("CN", "CNYTN", 22.5800, 114.2800, None, "UN/LOCODE 2651N 11952E is in Fujian; Yantian port is in Shenzhen"),
    ("TR", "TREVY", 40.7667, 29.7000, None, "UN/LOCODE 4002N 04303E is in eastern Anatolia; Evyapan = Evyap Port Kocaeli (TREYP)"),
    ("IC", "ICDN EMBAKASI", -1.3180, 36.8800, "KE", "Embakasi ICD, Nairobi; source country code 'IC' is a parsing artefact"),
    ("CN", "CNXGG", 39.0000, 117.7000, None, "Tianjin Xingang (Binhai New Area); 'Binhai' city name is ambiguous"),
    ("CN", "CNYSA", 30.6300, 122.0700, None, "Yangshan deep-water port (Shanghai); no UN/LOCODE or gazetteer position"),
    ("EG", "EGDEK", 31.1330, 29.8000, None, "El Dekheila port (Alexandria), same port as EGEDK"),
    ("IN", "INTKD", 28.5060, 77.2690, None, "Tughlakabad ICD, New Delhi - inland container depot, inland by design"),
    ("US", "USCHT", 39.8476, -75.3577, None, "Chester, PA (Delaware River); legacy point was Chattanooga, TN"),
    ("SE", "HELSINKI, 00160", 60.1650, 24.9600, "FI", "Helsinki 00160 (Katajanokka); source country SE is wrong"),
    ("DE", "KISTA, 16440", 59.4030, 17.9440, "SE", "Kista 164 40, Stockholm; source country DE is wrong"),
    ("DE", "LE GRAND SACONNEX, 01218", 46.2320, 6.1210, "CH", "Le Grand-Saconnex 1218, Geneva; source country DE is wrong"),
    ("EG", "10TH RAMADAN, 44916", 30.2900, 31.7500, None, "10th of Ramadan City - approximate city centre"),
    ("EG", "6TH OCTOBER CITY, 001101", 29.9700, 30.9500, None, "6th of October City - approximate city centre"),
    ("TR", "ERGENE, 00000", 41.1950, 27.6250, None, "Ergene district (Velimese), Tekirdag - approximate centre"),
    ("MX", "Laredo,78040", 27.4763, -99.5164, None, "78040 is a Laredo TX ZIP; MX side of the crossing = Nuevo Laredo"),
    ("MX", "LAREDO", 27.4763, -99.5164, None, "Laredo border crossing, MX side = Nuevo Laredo; legacy point was in Michoacan"),
    ("PK", "PKQCT", 24.7814, 67.3594, None, "Qasim International Container Terminal, Port Qasim (not Karachi city)"),
    ("FR", "Lavera, 13117", 43.3890, 5.0050, None, "Lavera (Martigues) petrochemical port; legacy point was Strasbourg"),
    ("FR", "SALIN-DE-GIRAUD, 13129", 43.4120, 4.7310, None, "Salin-de-Giraud (Arles); legacy point was Strasbourg"),
    ("BE", "ROTTERDAM, 3195 ND", 51.8850, 4.3850, "NL", "3195 ND = Pernis, Rotterdam; source country BE is wrong"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core: build

# COMMAND ----------

# CORE
def parse_location(cc, loc, desc):
    s = str(loc).strip()
    d = None if str(desc).strip().lower() in ("", "null") else str(desc).strip()
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{3}", s):
        return {"loc_type": "locode", "base_locode": s, "names": [d], "postal_raw": None}
    m = re.fullmatch(r"([A-Z]{2}[A-Z0-9]{3})_([A-Z0-9]+)", s)
    if m:
        return {"loc_type": "locode_sub", "base_locode": m[1], "names": [d], "postal_raw": None}
    if "," in s:
        city, postal = s.rsplit(",", 1)
        postal = postal.strip()
        names = [city.strip(), d]
        if postal and not re.search(r"\d", postal):  # e.g. "Seoul", "BINH DUONG", "USA"
            names.append(postal)
            m = re.findall(r"(?<![\d-])(\d{4,6})(?![\d-])", city)  # "4404 Euclid Ave, East Chicago, IN 46312, USA"
            postal = m[-1] if m and city.count(",") >= 1 else None
            if postal:
                names = [d] + [x for x in city.split(",") if not re.search(r"\d", x)] + names
        return {"loc_type": "postal", "base_locode": None, "names": names, "postal_raw": postal or None}
    m = re.search(r"(\d[\d\- ]{2,}\d)\s*$", s)
    if m:
        city = s[: m.start()].strip()
        return {"loc_type": "postal", "base_locode": None, "names": [d, city], "postal_raw": m[1]}
    return {"loc_type": "city", "base_locode": None, "names": [s, d], "postal_raw": None}


def _family(c):
    if c["src"] == "legacy":
        return "unlocode" if c.get("same_as_unlocode") else "legacy"
    return {"postal_prefix": "gn_postal", "postal": "gn_postal"}.get(c["src"], c["src"])


def _level(c, alone=False):
    """Evidence strength: 3 manual, 2 strong, 1 medium, 0 weak. An unsupported candidate is dropped when a
    stronger one contradicts it (that is what makes legacy "validated"). alone=True: the candidate has no
    support - a single port-gazetteer position then counts as medium."""
    s = c["src"]
    if s == "manual":
        return 3
    if s == "unlocode":
        return 2
    if s in PORT_REFS:
        return 1 if alone else 2
    if s == "legacy":
        return 0
    if s == "postal":
        return 2 if c.get("name_match") else 0
    if s == "postal_prefix":
        return 1 if (c.get("name_match") and c.get("spread_km", 999) <= 25) else 0
    ci = c["city_info"]
    if ci["loose"] or not (ci["n_homonyms"] == 1 or ci.get("dominant") or ci["anchored"]):
        return 0
    return 1 if c.get("postal_named") else 2


def _choose(cands):
    """Highest-priority valid candidate that is
    - supported by another source when it needs support (legacy point shared with one other place),
    - not a weak, unsupported candidate contradicted by a strong one (this is what "validated legacy" means),
    - not outvoted by >=2 agreeing independent sources."""
    valid = [c for c in cands if c["valid"]]
    valid.sort(key=lambda c: PRIORITY.index(c["src"]))
    for c in valid:
        if c["src"] == "manual":
            return c
        others = [o for o in valid if _family(o) != _family(c)]
        sup = [o for o in others if hav(c["lat"], c["lon"], o["lat"], o["lon"]) <= MAX_DISAGREE_KM]
        det = [o for o in others if o not in sup]
        if c.get("needs_support") and not any(
                hav(c["lat"], c["lon"], o["lat"], o["lon"]) <= SHARED_SUPPORT_KM for o in sup):
            c["outvoted"] = True
            continue
        if not sup and any(_level(o) > _level(c, alone=True) for o in det):
            c["outvoted"] = True
            continue
        if not sup and det:
            agreeing = {_family(a) for a in det for b in det
                        if _family(a) != _family(b) and hav(a["lat"], a["lon"], b["lat"], b["lon"]) <= MAX_DISAGREE_KM}
            if len(agreeing) >= 2:
                c["outvoted"] = True
                continue
        return c
    return None


def _shared_groups(points):
    """points: list of (key, lat, lon, identity labels). Returns key -> number of distinct places at that point.
    Locations at one point are the same place when they share any label (base locode, postal code or name)."""
    by = defaultdict(list)
    for k, lat, lon, ident in points:
        by[(round(lat, SHARED_DECIMALS), round(lon, SHARED_DECIMALS))].append((k, ident))
    out = {}
    for members in by.values():
        clusters = []  # list of label sets
        for _, ident in members:
            merged = set(ident)
            for c in [c for c in clusters if c & merged]:
                merged |= c
                clusters.remove(c)
            clusters.append(merged)
        for k, _ in members:
            out[k] = len(clusters)
    return out


def _identity(cc, p):
    labels = {f"LOC:{p['base_locode']}"} if p["base_locode"] else set()
    v = name_variants(p["names"])
    if v:
        labels.add(f"NM:{cc}:{v[0][0]}")
    for nm in p["names"]:
        n = " ".join(t for t in norm_city(nm).split() if t not in _STOP and not t.isdigit())
        if len(n) >= 3 and nm and str(nm).lower() != "null":
            labels.add(f"NM:{cc}:{n}")
    key = Postal.clean(cc, p["postal_raw"]) if p["postal_raw"] else None
    if key:
        labels.add(f"PC:{cc}:{key}")
    return frozenset(labels or {f"RAW:{cc}"})


def build(paths, manual=MANUAL_OVERRIDES, log=print):
    t0 = datetime.now(timezone.utc)
    cons = pd.read_csv(paths["consultant"], dtype=str, keep_default_na=False)
    cons["cc"] = cons["cc"].str.strip().str.upper()
    cons["loc_key"] = cons["loc"].str.strip()
    # rows that differ only by surrounding whitespace are one location
    union = lambda v: "|".join(sorted({x for s_ in v for x in str(s_).split("|") if x}))
    cons = (cons.assign(n_records=pd.to_numeric(cons.n_records, errors="coerce").fillna(0).astype(int))
            .groupby(["cc", "loc_key"], as_index=False, sort=False)
            .agg(loc=("loc_key", "first"), loc_desc=("loc_desc", "first"), modes=("modes", union),
                 sources=("sources", union), n_records=("n_records", "sum"), last_valid_to=("last_valid_to", "max")))
    # locations used by the legacy pair map but missing from the consultant list are kept too
    leg = pd.read_csv(paths["legacy"], dtype=str, keep_default_na=False)
    leg_raw = leg.copy()  # pair map output keeps the legacy rows exactly as they were
    for s_ in ("origin", "dest"):
        leg[f"{s_}_loc_country_code"] = leg[f"{s_}_loc_country_code"].str.strip().str.upper()
    ends = pd.concat([leg[[f"{s}_loc_country_code", f"{s}_loc"]].set_axis(["cc", "loc"], axis=1)
                      for s in ("origin", "dest")])
    ends["loc_key"] = ends["loc"].str.strip()
    extra = ends.drop_duplicates(["cc", "loc_key"]).merge(cons[["cc", "loc_key"]], how="left", indicator=True)
    extra = extra[extra._merge == "left_only"].drop(columns="_merge")
    if len(extra):
        log(f"{len(extra)} legacy-only locations added: {extra.loc_key.tolist()[:10]}")
        extra = extra.assign(loc=extra.loc_key, loc_desc="null", modes="", sources="legacy_only", n_records=0,
                             last_valid_to="")
        cons = pd.concat([cons, extra[cons.columns]], ignore_index=True)
    manual_by = {(m[0], m[1]): m for m in manual}
    ccs = set(cons.cc) | {m[4] for m in manual if m[4]}
    ccs |= {a for c in list(ccs) for a in COUNTRY_ALIASES.get(c, [])}

    log("loading references ...")
    countries = Countries(paths["ne_admin0"])
    coast = Coast(paths["ne_coast"])
    unl = load_unlocode(paths["unlocode"])
    refs = load_port_refs(paths)
    postal = Postal(paths["gn_postal"], ccs)
    cities = Cities(paths["gn_cities"], paths["gn_admin1"], ccs, postal)
    log(f"port refs: { {k: len(v) for k, v in refs.items()} }")

    # legacy pair map -> one point per location
    lp = pd.concat([
        leg[["origin_loc_country_code", "origin_loc", "origin_lat", "origin_lon", "origin_geo_src"]].set_axis(
            ["cc", "loc", "lat", "lon", "src"], axis=1),
        leg[["dest_loc_country_code", "dest_loc", "dest_lat", "dest_lon", "dest_geo_src"]].set_axis(
            ["cc", "loc", "lat", "lon", "src"], axis=1)])
    lp["loc"] = lp["loc"].str.strip()
    lp["lat"] = pd.to_numeric(lp.lat, errors="coerce")
    lp["lon"] = pd.to_numeric(lp.lon, errors="coerce")
    lp = lp.dropna(subset=["lat", "lon"])
    legacy = {}
    for (cc, loc), g in lp.groupby(["cc", "loc"]):
        top = g.groupby(["lat", "lon", "src"]).size().sort_values(ascending=False)
        (lat, lon, src), _ = next(iter(top.items()))
        legacy[(cc, loc)] = {"lat": lat, "lon": lon, "legacy_src": src, "variants": len(top)}

    parsed = {}
    for r in cons.itertuples(index=False):
        p = parse_location(r.cc, r.loc_key, r.loc_desc)
        if p["base_locode"] and p["base_locode"] in unl:
            p["names"] = p["names"] + [unl[p["base_locode"]]["name"]]
        p["variants"] = name_variants(p["names"])
        p["identity"] = _identity(r.cc, p)
        parsed[(r.cc, r.loc_key)] = p

    # shared legacy points (geocoder fallbacks)
    lpts = [(k, v["lat"], v["lon"], parsed[k]["identity"] if k in parsed else frozenset({f"NM:{k[0]}:{norm_city(k[1])}"}))
            for k, v in legacy.items()]
    legacy_shared = _shared_groups(lpts)

    rows, cand_rows = [], []
    for r in cons.itertuples(index=False):
        key = (r.cc, r.loc_key)
        p = parsed[key]
        man = manual_by.get(key)
        vcc = (man[4] if man and man[4] else r.cc)
        is_port = "OCEAN" in r.modes and p["loc_type"] in ("locode", "locode_sub")
        tol = PORT_TOL_KM if is_port else LAND_TOL_KM
        cands = []

        def add(c):
            st, d = countries.check(vcc, c["lat"], c["lon"])
            c["country_check"], c["country_dist_km"] = st, d
            c["valid"] = st != "outside" or d <= tol
            c["why_invalid"] = None if c["valid"] else f"outside {vcc} by {d:.0f} km"
            cands.append(c)

        if man:
            add({"src": "manual", "lat": man[2], "lon": man[3], "detail": man[5]})
        if p["base_locode"]:
            u = unl.get(p["base_locode"])
            if u and u["lat"] is not None:
                add({"src": "unlocode", "lat": u["lat"], "lon": u["lon"], "detail": f"{u['name']} [{u['status']}]"})
            for s in PORT_REFS:
                if p["base_locode"] in refs.get(s, {}):
                    lat, lon = refs[s][p["base_locode"]]
                    add({"src": s, "lat": lat, "lon": lon, "detail": p["base_locode"]})
        lg = legacy.get(key)
        if lg:
            c = {"src": "legacy", "lat": lg["lat"], "lon": lg["lon"], "legacy_src": lg["legacy_src"],
                 "detail": f"{lg['legacy_src']} ({lg['variants']} variants)"}
            add(c)
            u = [o for o in cands if o["src"] == "unlocode"]
            c["same_as_unlocode"] = bool(u) and hav(c["lat"], c["lon"], u[0]["lat"], u[0]["lon"]) <= 1.0
            n_sh = legacy_shared.get(key, 1)
            if lg["legacy_src"] == "none":
                c["valid"], c["why_invalid"] = False, "legacy geo_src=none"
            elif n_sh >= 3:
                c["valid"] = False
                c["why_invalid"] = f"shared point ({n_sh} differently named places)"
            elif n_sh == 2:
                c["needs_support"] = True
                c["detail"] += "; shared with 1 other place"
        if p["postal_raw"]:
            pc = postal.lookup(r.cc, p["postal_raw"], p["variants"])
            if pc:
                add({"src": pc["src"], "lat": pc["lat"], "lon": pc["lon"],
                     "detail": f"{pc['detail']} {pc['place']} n={pc['n']} spread={pc['spread_km']:.0f}km"
                               f"{'' if pc['name_match'] else ' NAME-MISMATCH'}",
                     "name_match": pc["name_match"], "spread_km": pc["spread_km"]})
        anchors = [(c["lat"], c["lon"]) for c in cands if c["valid"]]
        tokens = {t for nm in p["names"] + [p["postal_raw"]] if nm for t in norm_city(nm).split()}
        cc_ = cities.lookup(r.cc, p["variants"], anchors, tokens)
        if cc_:
            add({"src": "city", "lat": cc_["lat"], "lon": cc_["lon"],
                 "detail": f"{cc_['detail']} -> {cc_['place']} homonyms={cc_['n_homonyms']}"
                           f"{' loose' if cc_['loose'] else ''}{' anchored' if cc_['anchored'] else ''}",
                 "city_info": cc_,
                 "postal_named": any(o.get("name_match") for o in cands if o["src"] in ("postal", "postal_prefix"))})

        chosen = _choose(cands)
        row = {"cc": r.cc, "loc": r.loc_key, "loc_desc": r.loc_desc, "loc_type": p["loc_type"],
               "base_locode": p["base_locode"], "postal_code": p["postal_raw"],
               "modes": r.modes, "sources": r.sources, "n_records": int(r.n_records or 0),
               "last_valid_to": r.last_valid_to, "validate_cc": vcc, "is_port": is_port,
               "identity": p["identity"]}
        if chosen:
            others = [o for o in cands if o is not chosen]
            valid_others = [o for o in others if o["valid"] and _family(o) != _family(chosen)]
            sup = [o["src"] for o in valid_others if hav(chosen["lat"], chosen["lon"], o["lat"], o["lon"]) <= MAX_DISAGREE_KM]
            dis = [(o["src"], hav(chosen["lat"], chosen["lon"], o["lat"], o["lon"])) for o in others]
            dis_far = [(s, d) for s, d in dis if d > MAX_DISAGREE_KM]
            lvl = _level(chosen, alone=not sup)
            dis_valid = [d for o, (s, d) in zip(others, dis) if o["valid"] and not o.get("outvoted")
                         and _level(o) >= min(lvl, 2) and _family(o) != _family(chosen)]
            row.update(lat=round(chosen["lat"], 6), lon=round(chosen["lon"], 6), geo_src=chosen["src"],
                       geo_detail=chosen.get("detail"), supporting_sources="|".join(sorted(set(sup))),
                       n_supporting=len(set(sup)),
                       max_disagree_km=round(max(dis_valid), 1) if dis_valid else None,
                       disagree_sources="|".join(f"{s}:{d:.0f}km" for s, d in dis_far),
                       outvoted_sources="|".join(o["src"] for o in cands if o.get("outvoted")),
                       country_check=chosen["country_check"],
                       country_dist_km=None if chosen["country_dist_km"] is None else round(chosen["country_dist_km"], 1))
            row["geo_conf"] = _confidence(chosen, row)
        else:
            row.update(lat=None, lon=None, geo_src="none", geo_conf="none", n_supporting=0)
        for c in cands:
            row[f"cand_{c['src']}"] = f"{c['lat']:.5f},{c['lon']:.5f}"
            cand_rows.append({"cc": r.cc, "loc": r.loc_key, "src": c["src"], "lat": c["lat"], "lon": c["lon"],
                              "valid": c["valid"], "why_invalid": c.get("why_invalid"),
                              "outvoted": bool(c.get("outvoted")), "chosen": c is chosen,
                              "country_check": c["country_check"], "country_dist_km": c["country_dist_km"],
                              "detail": c.get("detail")})
        if lg:
            row["legacy_lat"], row["legacy_lon"], row["legacy_geo_src"] = lg["lat"], lg["lon"], lg["legacy_src"]
            if chosen:
                row["legacy_moved_km"] = round(hav(lg["lat"], lg["lon"], chosen["lat"], chosen["lon"]), 1)
        port_pts = [(c["lat"], c["lon"]) for c in cands
                    if c["src"] in PORT_REFS and c["valid"] and not c.get("outvoted")]
        row["port_pts"] = port_pts
        rows.append(row)

    dim = pd.DataFrame(rows)
    _validate(dim, countries, coast, label="")
    # same checks on the legacy point, for the before/after comparison
    leg_dim = dim[["cc", "loc", "identity", "validate_cc", "is_port", "port_pts"]].copy()
    leg_dim["lat"], leg_dim["lon"] = dim.get("legacy_lat"), dim.get("legacy_lon")
    _validate(leg_dim, countries, coast, label="")
    for col in ["shared_point_n", "outside_country", "point_country", "port_misplaced", "coast_km"]:
        dim[f"legacy_{col}"] = leg_dim[col]

    dim["review_reasons"] = dim.apply(_review_reasons, axis=1)
    dim["status"] = np.where(dim.geo_src == "none", "MISSING", np.where(dim.review_reasons != "", "REVIEW", "OK"))
    dim["build_ts"] = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    dim = dim.drop(columns=["port_pts", "identity"])

    pair = _pair_map(leg_raw, dim)
    cands_df = pd.DataFrame(cand_rows)
    review = _review_list(dim, cands_df)
    report = _report(dim, cands_df, refs, paths)
    log(f"built {len(dim)} locations in {(datetime.now(timezone.utc) - t0).seconds}s")
    return {"dim": dim, "pair_map": pair, "review": review, "candidates": cands_df, "report": report,
            "manual": pd.DataFrame(manual, columns=["cc", "loc", "lat", "lon", "validate_cc", "note"])}


def _confidence(c, row):
    """high: manual, or a strong/exact source confirmed by another family.
    medium: one trustworthy source, or an approximation (prefix/city) confirmed by another family.
    low: a single weak source."""
    s, n = c["src"], row["n_supporting"]
    lvl = _level(c, alone=not n)
    if s == "manual" or (n and s not in APPROX_SOURCES):
        return "high"
    if n or lvl >= 1 or (s == "legacy" and not row.get("max_disagree_km")):
        return "medium"
    return "low"


def _validate(df, countries, coast, label):
    out_flag, dist, pc, shared, coast_km, misplaced = [], [], [], [], [], []
    precise = df["geo_src"].map(lambda x: x not in APPROX_SOURCES) if "geo_src" in df else pd.Series(True, df.index)
    pts = [((r.cc, r.loc), r.lat, r.lon, r.identity) for r, ok in zip(df.itertuples(), precise)
           if pd.notna(r.lat) and ok]
    sh = _shared_groups(pts)
    for r in df.itertuples():
        if pd.isna(r.lat):
            out_flag.append(None), dist.append(None), pc.append(None), shared.append(None)
            coast_km.append(None), misplaced.append(None)
            continue
        st, d = countries.check(r.validate_cc, r.lat, r.lon)
        tol = PORT_TOL_KM if r.is_port else LAND_TOL_KM
        out_flag.append(st == "outside" and d > tol)
        dist.append(None if d is None else round(d, 1))
        pc.append(countries.locate(r.lat, r.lon) if st == "outside" else r.validate_cc)
        shared.append(sh.get((r.cc, r.loc), 1))
        if r.is_port:
            ck = coast.km(r.lat, r.lon)
            coast_km.append(round(ck, 1))
            refs_ok = [hav(r.lat, r.lon, a, b) <= MAX_DISAGREE_KM for a, b in r.port_pts]
            misplaced.append((bool(r.port_pts) and not any(refs_ok)) or (not r.port_pts and ck > PORT_INLAND_KM))
        else:
            coast_km.append(None), misplaced.append(False)
    df["outside_country"], df["point_country"], df["shared_point_n"] = out_flag, pc, shared
    df["coast_km"], df["port_misplaced"] = coast_km, misplaced
    if "country_dist_km" not in df:
        df["country_dist_km"] = dist


def _review_reasons(r):
    if r.geo_src == "none":
        return "no_coordinates"
    out = []
    if r.geo_src == "manual":
        return ""
    if r.outside_country:
        out.append(f"outside_country({r.point_country})")
    if r.shared_point_n and r.shared_point_n > 1:
        out.append(f"shared_point({int(r.shared_point_n)})")
    if r.port_misplaced and r.n_supporting == 0:
        out.append("port_unconfirmed" if not r.coast_km or r.coast_km <= PORT_INLAND_KM else f"port_inland({r.coast_km:.0f}km)")
    if pd.notna(r.max_disagree_km) and r.max_disagree_km > MAX_DISAGREE_KM and r.n_supporting == 0:
        out.append(f"unresolved_disagreement({r.max_disagree_km:.0f}km)")
    if r.geo_conf == "low":
        out.append(f"low_confidence({r.geo_src})")
    return ";".join(out)


def _pair_map(leg, dim):
    look = {(r.cc, r.loc): (r.lat, r.lon, r.geo_src) for r in dim.itertuples()}
    out = leg.copy()
    for side in ("origin", "dest"):
        vals = [look.get((str(cc).strip().upper(), str(loc).strip()), (None, None, "none"))
                for cc, loc in zip(out[f"{side}_loc_country_code"], out[f"{side}_loc"])]
        out[f"{side}_lat"] = [v[0] for v in vals]
        out[f"{side}_lon"] = [v[1] for v in vals]
        out[f"{side}_geo_src"] = [v[2] if pd.notna(v[0]) else "none" for v in vals]
    return out


def _review_list(dim, cands):
    rv = dim[dim.status != "OK"].copy()
    cs = cands.assign(txt=lambda d: d.src + "=" + d.lat.round(4).astype(str) + "," + d.lon.round(4).astype(str)
                      + np.where(d.valid, "", " (invalid: " + d.why_invalid.fillna("") + ")")
                      + np.where(d.outvoted, " (outvoted)", ""))
    rv = rv.merge(cs.groupby(["cc", "loc"]).txt.agg(" | ".join).rename("candidates").reset_index(), how="left",
                  on=["cc", "loc"])
    rv["suggested_action"] = rv.review_reasons.map(_action)
    cols = ["status", "review_reasons", "suggested_action", "cc", "loc", "loc_desc", "loc_type", "modes", "sources",
            "n_records", "lat", "lon", "geo_src", "geo_conf", "geo_detail", "point_country", "country_dist_km",
            "max_disagree_km", "disagree_sources", "legacy_lat", "legacy_lon", "legacy_moved_km", "candidates"]
    rv = rv[[c for c in cols if c in rv]]
    return rv.sort_values(["status", "n_records"], ascending=[True, False]).reset_index(drop=True)


def _action(reasons):
    if "no_coordinates" in reasons:
        return "add manual override"
    if "outside_country" in reasons:
        return "check country code / add manual override"
    if "port" in reasons:
        return "confirm port position, add manual override if wrong"
    if "unresolved_disagreement" in reasons:
        return "pick correct candidate, add manual override"
    if "shared_point" in reasons:
        return "verify the places really coincide"
    return "spot-check; accept or override"


def _report(dim, cands, refs, paths):
    def issues(prefix):
        o = dim[f"{prefix}outside_country"].fillna(False).astype(bool)
        s = dim[f"{prefix}shared_point_n"].fillna(1) > 1
        p = dim[f"{prefix}port_misplaced"].fillna(False).astype(bool)
        has = (dim["legacy_lat"] if prefix == "legacy_" else dim["lat"]).notna()
        return {"has_point": int(has.sum()), "missing": int((~has).sum()), "outside_country": int(o.sum()),
                "shared_point": int(s.sum()), "port_misplaced": int(p.sum()), "any_issue": int((o | s | p | ~has).sum())}

    before, after = issues("legacy_"), issues("")
    L = [f"# market_map rebuild – validation report", "",
         f"Build: {dim.build_ts.iloc[0]}  ·  locations: {len(dim):,}  ·  pairs re-mapped from legacy market_map", "",
         "## Before vs after (same checks on legacy and rebuilt points)", "",
         "| check | legacy market_map | rebuilt |", "|---|---:|---:|"]
    for k in before:
        L.append(f"| {k} | {before[k]:,} | {after[k]:,} |")
    L += ["", "Checks: `outside_country` = beyond 10 km (land) / 30 km (ports) of the Natural Earth polygon; "
          "`shared_point` = same point (~100 m) used by differently named places; `port_misplaced` = ocean port "
          f"> {MAX_DISAGREE_KM:.0f} km from every port-gazetteer position, or > {PORT_INLAND_KM:.0f} km from the coast with no gazetteer entry.",
          "", "## Status", "", "| status | locations | records |", "|---|---:|---:|"]
    for s, g in dim.groupby("status"):
        L.append(f"| {s} | {len(g):,} | {g.n_records.sum():,} |")
    L += ["", "## Coordinate source", "", "| geo_src | high | medium | low | total |", "|---|---:|---:|---:|---:|"]
    ct = pd.crosstab(dim.geo_src, dim.geo_conf)
    for src in PRIORITY + ["none"]:
        if src in ct.index:
            r = ct.loc[src]
            L.append(f"| {src} | {r.get('high', 0):,} | {r.get('medium', 0):,} | {r.get('low', 0):,} | {r.sum():,} |")
    L += ["", "## Review reasons", "", "| reason | locations |", "|---|---:|"]
    rc = Counter(x.split("(")[0] for rr in dim.review_reasons for x in rr.split(";") if x)
    for k, v in rc.most_common():
        L.append(f"| {k} | {v} |")
    loc_ports = dim[dim.loc_type.isin(["locode", "locode_sub"])]
    uncoded = loc_ports.cand_unlocode.isna().sum() if "cand_unlocode" in dim else len(loc_ports)
    L += ["", "## Ports", "",
          f"- LOCODE locations: {len(loc_ports)}; without UN/LOCODE coordinates: {uncoded}; "
          f"still without any coordinate: {int((loc_ports.geo_src == 'none').sum())}",
          f"- UN/LOCODE rejected (outside country or outvoted by port gazetteers): "
          f"{int(((cands.src == 'unlocode') & (~cands.valid | cands.outvoted)).sum())}",
          f"- Port gazetteers used: " + ", ".join(f"{k} ({len(v):,} codes)" for k, v in refs.items())
          + ("" if "wpi" in refs else " — NGA World Port Index not available at build time"), "",
          "## Movement vs legacy", "", "| moved | locations |", "|---|---:|"]
    mv = dim.legacy_moved_km
    for lo, hi in [(0, 1), (1, 10), (10, 50), (50, 500), (500, 1e9)]:
        L.append(f"| {lo}–{'∞' if hi == 1e9 else hi} km | {int(((mv >= lo) & (mv < hi)).sum()):,} |")
    big = dim[mv >= 50].sort_values("n_records", ascending=False).head(40)
    L += ["", "### Largest corrections (moved ≥ 50 km, top 40 by records)", "",
          "| cc | loc | legacy src | moved km | new src | legacy issue |", "|---|---|---|---:|---|---|"]
    for r in big.itertuples():
        iss = ",".join(n for n, f in [("outside", r.legacy_outside_country), ("shared", (r.legacy_shared_point_n or 1) > 1),
                                       ("port", r.legacy_port_misplaced)] if f)
        L.append(f"| {r.cc} | {r.loc} | {r.legacy_geo_src} | {r.legacy_moved_km:,.0f} | {r.geo_src} | {iss} |")
    mo = dim[dim.geo_src == "manual"]
    L += ["", "### Manual overrides applied", "", "| cc | loc | lat | lon | moved vs legacy km | note |",
          "|---|---|---:|---:|---:|---|"]
    for r in mo.itertuples():
        mv_ = "" if pd.isna(r.legacy_moved_km) else f"{r.legacy_moved_km:,.0f}"
        L.append(f"| {r.cc} | {r.loc} | {r.lat} | {r.lon} | {mv_} | {r.geo_detail} |")
    sh = dim[dim.legacy_shared_point_n.fillna(1) > 1]
    top = sh.groupby([sh.legacy_lat.round(3), sh.legacy_lon.round(3)]).agg(
        n=("loc", "size"), ex=("loc", lambda s: ", ".join(list(s)[:4]))).sort_values("n", ascending=False).head(15)
    L += ["", "### Largest legacy shared points (geocoder fallbacks)", "", "| lat | lon | locations | examples |",
          "|---:|---:|---:|---|"]
    for (la, lo), r in top.iterrows():
        L.append(f"| {la} | {lo} | {r.n} | {r.ex} |")
    return "\n".join(L) + "\n"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run the build

# COMMAND ----------

res = build(PATHS)
dim = res["dim"]
print(res["report"][:3000])
display(dim.groupby(["status", "geo_src"]).size().reset_index(name="locations"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save table and CSV outputs

# COMMAND ----------

os.makedirs(OUT_DIR, exist_ok=True)
res["dim"].to_csv(f"{OUT_DIR}/unlocode_dim.csv", index=False)
res["pair_map"].to_csv(f"{OUT_DIR}/Geo_Enriched_rebuilt.csv", index=False)
res["review"].to_csv(f"{OUT_DIR}/manual_review.csv", index=False)
res["candidates"].to_csv(f"{OUT_DIR}/location_candidates.csv", index=False)
res["manual"].to_csv(f"{OUT_DIR}/manual_overrides.csv", index=False)
with open(f"{OUT_DIR}/validation_report.md", "w", encoding="utf-8") as f:
    f.write(res["report"])

from pyspark.sql import types as T


def to_spark(pdf):
    """pandas -> Spark with an explicit schema (object columns mix NaN/bool/str, so inference is unreliable)."""
    fields, cols = [], []
    for c in pdf.columns:
        s = pdf[c]
        vals = s.dropna()
        if pd.api.types.is_bool_dtype(s) or (len(vals) and vals.map(lambda v: isinstance(v, (bool, np.bool_))).all()):
            t, f = T.BooleanType(), bool
        elif pd.api.types.is_integer_dtype(s):
            t, f = T.LongType(), int
        elif pd.api.types.is_float_dtype(s):
            t, f = T.DoubleType(), float
        else:
            t, f = T.StringType(), str
        fields.append(T.StructField(c, t, True))
        cols.append([None if pd.isna(v) else f(v) for v in s])
    return spark.createDataFrame(list(zip(*cols)), T.StructType(fields))


sdf = to_spark(res["dim"])

dup = sdf.groupBy("cc", "loc").count().filter("count > 1").count()
if dup:
    raise ValueError(f"{dup} duplicate (cc, loc) keys - not saving")
if sdf.filter("lat IS NULL").count() > 0:
    print("WARNING: some locations have no coordinates - see manual_review.csv")

(sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
print(f"saved {sdf.count()} rows to {TARGET_TABLE}")

if PAIR_MAP_TABLE:
    to_spark(res["pair_map"]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PAIR_MAP_TABLE)
