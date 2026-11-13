"""Run the `# CORE` cells of unlocode_dim.py locally, the same way Step 2 of the notebook does.

usage:
  python market_map/run_local.py --consultant ... --unlocode ... --legacy ... --refs DIR --out DIR
         [--previous PREVIOUS_DIM.csv] [--pairs PAIRS.csv] [--mode incremental|full]
--refs holds the unzipped GeoNames files and the downloads listed in REF_URLS of the notebook.
"""
import argparse
import os
import sys
from datetime import datetime, timezone

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
    ap.add_argument("--previous")
    ap.add_argument("--pairs")
    ap.add_argument("--mode", default="incremental", choices=["incremental", "full"])
    ap.add_argument("--max-unresolved-share", type=float, default=0.01)
    a = ap.parse_args()
    r = a.refs
    opt = lambda p: p if os.path.exists(p) else None
    paths = {
        "consultant": a.consultant, "unlocode": a.unlocode, "legacy": a.legacy, "pairs": a.pairs,
        "gn_postal": [f"{r}/allCountries.txt", f"{r}/GB_full.txt", f"{r}/NL_full.txt", f"{r}/CA_full.txt"],
        "gn_cities": f"{r}/cities500.txt", "gn_admin1": f"{r}/admin1CodesASCII.txt",
        "ne_admin0": f"{r}/ne_10m_admin_0_countries.geojson", "ne_coast": f"{r}/ne_10m_coastline.geojson",
        "wpi": opt(f"{r}/wpi.csv"), "searoute": opt(f"{r}/searoute_ports.geojson"),
        "seaports": opt(f"{r}/sea_ports.json"),
    }
    core = load_core()
    pd = core["pd"]
    prev = pd.read_csv(a.previous, keep_default_na=False, na_values=[""]) if a.previous else None
    res = core["build"](paths)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    manual_keys = {(m[0], m[1]) for m in core["MANUAL_OVERRIDES"]}
    dim = core["merge_incremental"](res["dim"], prev, manual_keys, run_date, full_rebuild=(a.mode == "full"))
    pair = core["_pair_map"](res["pairs_in"], dim)
    dq = core["run_dq_checks"](dim, pair, len(res["pairs_in"]), prev, a.max_unresolved_share)

    os.makedirs(a.out, exist_ok=True)
    dim.to_csv(f"{a.out}/unlocode_dim.csv", index=False)
    pair.to_csv(f"{a.out}/Geo_Enriched_rebuilt.csv", index=False)
    res["review"].to_csv(f"{a.out}/manual_review.csv", index=False)
    res["candidates"].to_csv(f"{a.out}/location_candidates.csv", index=False)
    res["manual"].to_csv(f"{a.out}/manual_overrides.csv", index=False)
    dq.to_csv(f"{a.out}/dq_checks.csv", index=False)
    open(f"{a.out}/validation_report.md", "w", encoding="utf-8").write(res["report"])
    print(dq[["severity", "passed", "failed", "total", "check"]].to_string(index=False))
    print(dim.groupby(["is_active", "change", "status"]).size().to_string())
    errors = dq[(dq.severity == "ERROR") & ~dq.passed]
    if len(errors):
        print("DQ gate FAILED")
        return 1
    print("DQ gate passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
