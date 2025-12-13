# National BAN-Cadastre Alignment Report

Generated: 2025-12-13T20:43:15.131448600+00:00

## Definitions

- Accepted coverage (QA-aligned): best-per-parcel match is PreExisting, Inside, or distance_m <= 1500.
- Best-match coverage: best-per-parcel match exists (any match type except None), without threshold.
- Coverage delta: Best-match coverage minus Accepted coverage (higher delta implies higher risk of low-quality matches).

## Executive Summary

| Metric | Value |
|---|---:|
| Total parcels | 93451244 |
| Accepted matched parcels | 92643657 |
| Accepted coverage (%) | 99.14 |
| Mean confidence (accepted) | 61.64 |
| Best-match matched parcels | 92643657 |
| Best-match coverage (%) | 99.14 |
| Mean confidence (best-match) | 61.64 |
| Coverage delta (%) | 0.00 |

## Input Completeness

Manifest rows (total): 96

Expected departments (valid rows): 96

Invalid manifest rows: 0

Analyzed departments: 96

Skipped (missing matches): 0

Skipped (missing parcels): 0

## Match Type Distribution (best-per-parcel)

| Match type | Count |
|---|---:|
| FallbackNearest | 64934791 |
| BorderNear | 10313893 |
| PreExisting | 9628344 |
| Inside | 7766629 |

## By Region

| Region | Parcels | Accepted matched | Accepted % | Accepted conf | Best matched | Best % | Best conf |
|---|---:|---:|---:|---:|---:|---:|---:|
| Auvergne-Rhône-Alpes | 14852740 | 14696454 | 98.95% | 59.45 | 14696454 | 98.95% | 59.45 |
| Bourgogne-Franche-Comté | 6441935 | 6363480 | 98.78% | 59.22 | 6363480 | 98.78% | 59.22 |
| Bretagne | 5472211 | 5471587 | 99.99% | 65.93 | 5471587 | 99.99% | 65.93 |
| Centre-Val de Loire | 4569236 | 4560445 | 99.81% | 62.05 | 4560445 | 99.81% | 62.05 |
| Corse | 1036732 | 999452 | 96.40% | 57.41 | 999452 | 96.40% | 57.41 |
| Grand Est | 9395350 | 9217110 | 98.10% | 59.11 | 9217110 | 98.10% | 59.11 |
| Hauts-de-France | 6122662 | 6107244 | 99.75% | 65.83 | 6107244 | 99.75% | 65.83 |
| Normandie | 3883144 | 3881784 | 99.96% | 66.27 | 3881784 | 99.96% | 66.27 |
| Nouvelle-Aquitaine | 14523780 | 14487084 | 99.75% | 60.14 | 14487084 | 99.75% | 60.14 |
| Occitanie | 13022018 | 12880225 | 98.91% | 60.00 | 12880225 | 98.91% | 60.00 |
| Pays de la Loire | 5473830 | 5470271 | 99.93% | 64.06 | 5470271 | 99.93% | 64.06 |
| Provence-Alpes-Côte d'Azur | 4951249 | 4810127 | 97.15% | 63.84 | 4810127 | 97.15% | 63.84 |
| Île-de-France | 3706357 | 3698394 | 99.79% | 68.46 | 3698394 | 99.79% | 68.46 |

## Top 10 Departments (accepted coverage)

| Department | Accepted coverage |
|---|---:|
| 75 | 100.00% |
| 92 | 100.00% |
| 93 | 100.00% |
| 24 | 100.00% |
| 22 | 100.00% |
| 94 | 100.00% |
| 32 | 99.99% |
| 35 | 99.99% |
| 29 | 99.99% |
| 61 | 99.99% |

## Bottom 10 Departments (accepted coverage)

| Department | Accepted coverage |
|---|---:|
| 05 | 91.69% |
| 48 | 92.91% |
| 52 | 93.31% |
| 55 | 93.95% |
| 06 | 95.20% |
| 2A | 96.05% |
| 04 | 96.15% |
| 73 | 96.33% |
| 51 | 96.54% |
| 2B | 96.60% |

## Artifacts

- departments_summary.csv: data\ban_cadastre\departments_summary.csv
- national_summary.json: data\ban_cadastre\national_summary.json
- analysis_report.md: data\ban_cadastre\analysis_report.md
