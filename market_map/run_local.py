"""Run the `# CORE` cells of unlocode_dim.py locally and write the outputs.

usage: python market_map/run_local.py --consultant ... --unlocode ... --legacy ... --refs DIR --out DIR
DIR for --refs holds the unzipped GeoNames files and the downloads listed in REF_URLS of the notebook.
"""
import argparse
import os
import sys

NOTEBOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "unlocode_dim.py")


def load_core():
    src = open(NOTEBOOK, encoding="utf-8").read()
    ns = {"__name__": "unlocode_core"}
    for cell in src.split("# COMMAND ----------"):
        if cell.strip().startswith("# CORE"):
            exec(compile(cell, NOTEBOOK, "exec"), ns)
    return ns


def main():
    ap = argparse.ArgumentParser()
    for a in ("consultant", "unlocode", "legacy", "refs", "out"):
        ap.add_argument(f"--{a}", required=True)
    a = ap.parse_args()
    r = a.refs
    opt = lambda p: p if os.path.exists(p) else None
    paths = {
        "consultant": a.consultant, "unlocode": a.unlocode, "legacy": a.legacy,
        "gn_postal": [f"{r}/allCountries.txt", f"{r}/GB_full.txt", f"{r}/NL_full.txt", f"{r}/CA_full.txt"],
        "gn_cities": f"{r}/cities500.txt", "gn_admin1": f"{r}/admin1CodesASCII.txt",
        "ne_admin0": f"{r}/ne_10m_admin_0_countries.geojson", "ne_coast": f"{r}/ne_10m_coastline.geojson",
        "wpi": opt(f"{r}/wpi.csv"), "searoute": opt(f"{r}/searoute_ports.geojson"),
        "seaports": opt(f"{r}/sea_ports.json"),
    }
    core = load_core()
    res = core["build"](paths)
    os.makedirs(a.out, exist_ok=True)
    res["dim"].to_csv(f"{a.out}/unlocode_dim.csv", index=False)
    res["pair_map"].to_csv(f"{a.out}/Geo_Enriched_rebuilt.csv", index=False)
    res["review"].to_csv(f"{a.out}/manual_review.csv", index=False)
    res["candidates"].to_csv(f"{a.out}/location_candidates.csv", index=False)
    res["manual"].to_csv(f"{a.out}/manual_overrides.csv", index=False)
    open(f"{a.out}/validation_report.md", "w", encoding="utf-8").write(res["report"])
    print(res["report"])


if __name__ == "__main__":
    sys.exit(main())
