# market_map rebuild (consultant rate location coordinates)

`../unlocode_dim.py` is the Databricks notebook that builds the location dim (one validated coordinate per
location) and the pair-level market_map, validates both, and writes them as parquet:

| path | content |
|---|---|
| `/mnt/mda-pipeline-refined/market_map_location_dim` | location dim |
| `/mnt/mda-pipeline-refined/market_map` | pair map, legacy `Geo_Enriched` layout |
| `/mnt/mda-pipeline-refined/market_map_location_dim_history` | dim snapshot per `build_date` |
| `/mnt/mda-pipeline-refined/market_map_dq` | data-quality results of every run |
| `cdl_mda_prod.internal_material_data_asset.market_map_location_dim` | optional Delta copy (off by default) |

Inputs are read directly: locations and lanes from `/mnt/mda-pipeline-refined/ibl_consultant_fact`, the old map from
`/mnt/sharepoint/ibl/market_rate_long_lat_map/Geo Enriched.parquet`, UN/LOCODE from
`cdl_mda_prod.internal_material_data_asset.unlocode_dim` (read only - the notebook refuses to write to any input).

Modes: `incremental` (default) keeps every coordinate already accepted and only geocodes new or unresolved
locations; `full` recomputes everything. A failed ERROR check in `run_dq_checks` stops the job before anything is
written. `run_local.py` runs the same `# CORE` cells locally; `output/` holds a local full run.

## Outputs

| file | content |
|---|---|
| `output/market_map_location_dim.csv` | location dim: chosen `lat`/`lon`, `geo_src`, `geo_conf`, `status`, every candidate (`cand_*`), validation flags, legacy point and how far it moved |
| `output/Geo_Enriched_rebuilt.csv` | pair map in the legacy `Geo_Enriched` format (same rows and keys, new coordinates; `*_geo_src` holds the new source names) |
| `output/manual_review.csv` | locations with status REVIEW, the reason, a suggested action and all candidates |
| `output/validation_report.md` | before/after checks, source mix, largest corrections, overrides applied |
| `output/location_candidates.csv` | every candidate per location with validity, why it was rejected or outvoted |
| `output/manual_overrides.csv` | the overrides in the notebook cell *Manual overrides* |
| `output/dq_checks.csv` | data-quality checks of the run (ERROR = gate, WARN = report) |

## How a coordinate is chosen

Candidates per location, in priority order: `manual` > `unlocode` > port gazetteers (`wpi`, `searoute`,
`seaports`) > `legacy` > `postal` (GeoNames exact) > `postal_prefix` > `city` (GeoNames cities500, postal place
names, admin-1 fallback).

A candidate is rejected when it is outside its country polygon (Natural Earth admin-0; 10 km tolerance, 30 km for
ports) or, for legacy points, when the same point is used by 3+ differently named places (geocoder fallback such as
China TX, Mexico City, Washington DC). The highest-priority remaining candidate wins unless

- it has no support within 50 km and a stronger source contradicts it (this is what "validated legacy" means), or
- two other independent sources agree with each other more than 50 km away (it is outvoted).

`status` is REVIEW when the result is low-confidence, an unresolved > 50 km disagreement, a point shared by different
places, or an ocean port far from the coast that no other source confirms.

## Schedule

| what | when |
|---|---|
| `mode=incremental` job | after each consultant rate load (at least weekly); a few minutes, only new locations are geocoded |
| work through `manual_review.csv` | weekly, together with the incremental run |
| `mode=full, refresh_refs=true` | quarterly, and after a new UN/LOCODE release (twice a year) or a rule change; compare with the previous snapshot before accepting |

## Re-running

Databricks: run the notebook; reference data is downloaded once into the volume `.../market_map/reference/`.

Locally:

```bash
python market_map/run_local.py --consultant All_consultant_locations.csv --unlocode UNLOCODE_with_coordinates.csv --legacy Geo_Enriched.csv --refs <dir with GeoNames/Natural Earth/port files> --out market_map/output
```

To fix a REVIEW item, add a row to `MANUAL_OVERRIDES` in the notebook and re-run.

## Note on the World Port Index

NGA's download site (msi.nga.mil) returned 503 during this build, so the port cross-check used Eurostat searoute
ports (3,923 codes) and sea-ports (1,610 codes). Agreement with UN/LOCODE: median 3 km and 0.4 km; 5 of 195 and 1 of
192 differ by more than 50 km. The notebook downloads WPI (`UpdatedPub150.csv`) and uses it automatically when it
is available.
