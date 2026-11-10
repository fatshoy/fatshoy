# market_map rebuild (consultant rate location coordinates)

`../unlocode_dim.py` is the Databricks notebook that builds `cdl_mda_prod.internal_material_data_asset.unlocode_dim`
(one validated coordinate per location) and re-maps the pair-level market_map from it.
`run_local.py` runs the same `# CORE` cells locally; `output/` holds the result of that run.

## Outputs

| file | content |
|---|---|
| `output/unlocode_dim.csv` | location dim: chosen `lat`/`lon`, `geo_src`, `geo_conf`, `status`, every candidate (`cand_*`), validation flags, legacy point and how far it moved |
| `output/Geo_Enriched_rebuilt.csv` | pair map in the legacy `Geo_Enriched` format (same rows and keys, new coordinates; `*_geo_src` holds the new source names) |
| `output/manual_review.csv` | locations with status REVIEW, the reason, a suggested action and all candidates |
| `output/validation_report.md` | before/after checks, source mix, largest corrections, overrides applied |
| `output/location_candidates.csv` | every candidate per location with validity, why it was rejected or outvoted |
| `output/manual_overrides.csv` | the overrides in the notebook cell *Manual overrides* |

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

## Re-running

Databricks: upload the three inputs to `/Volumes/cdl_mda_prod/internal_material_data_asset/market_map/input/`
(or point the config at tables) and run the notebook. Reference data is downloaded once into `.../reference/`.

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
