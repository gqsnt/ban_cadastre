#!/usr/bin/env bash
set -euo pipefail

DEPT="69"
DATA_DIR="data/ban_cadastre"
DUCKDB_EXE="duckdb"

usage() {
  cat <<EOF
Usage: scripts/export_kepler.sh [--dept <DEP>] [--data-dir <DIR>] [--duckdb <PATH>]
EOF
}

sql_escape_path() {
  local p="$1"
  p="${p//\\/\/}"
  p="${p//\'/\'\'}"
  printf "%s" "$p"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dept|-d) DEPT="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --duckdb) DUCKDB_EXE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

matches_path="${DATA_DIR}/batch_results/matches_${DEPT}.parquet"
parcels_path="${DATA_DIR}/staging/parcelles_${DEPT}.parquet"
addresses_path="${DATA_DIR}/staging/adresses_${DEPT}.parquet"
kepler_dir="${DATA_DIR}/kepler"

[[ -f "$matches_path" ]]   || { echo "Missing input: $matches_path" >&2; exit 1; }
[[ -f "$parcels_path" ]]   || { echo "Missing input: $parcels_path" >&2; exit 1; }
[[ -f "$addresses_path" ]] || { echo "Missing input: $addresses_path" >&2; exit 1; }

mkdir -p "$kepler_dir"

matches_sql="$(sql_escape_path "$matches_path")"
parcels_sql="$(sql_escape_path "$parcels_path")"
addresses_sql="$(sql_escape_path "$addresses_path")"
kepler_sql="$(sql_escape_path "$kepler_dir")"

addr_out="${kepler_sql}/kepler_addresses_${DEPT}.csv"
parc_out="${kepler_sql}/kepler_parcels_${DEPT}.csv"
links_addr_out="${kepler_sql}/kepler_links_addr_${DEPT}.csv"
links_parcel_out="${kepler_sql}/kepler_links_parcel_${DEPT}.csv"

parcels_geom_type="$("$DUCKDB_EXE" ":memory:" -csv -noheader <<SQL
INSTALL spatial; LOAD spatial;
SELECT typeof(geom) FROM read_parquet('${parcels_sql}') LIMIT 1;
SQL
)"
addresses_geom_type="$("$DUCKDB_EXE" ":memory:" -csv -noheader <<SQL
INSTALL spatial; LOAD spatial;
SELECT typeof(geom) FROM read_parquet('${addresses_sql}') LIMIT 1;
SQL
)"

parcels_geom_expr="ST_GeomFromWKB(geom)"
addresses_geom_expr="ST_GeomFromWKB(geom)"
[[ "$parcels_geom_type" == *GEOMETRY* ]]   && parcels_geom_expr="geom"
[[ "$addresses_geom_type" == *GEOMETRY* ]] && addresses_geom_expr="geom"

"$DUCKDB_EXE" ":memory:" -batch <<SQL
INSTALL spatial; LOAD spatial;

CREATE OR REPLACE VIEW matches AS
SELECT * FROM read_parquet('${matches_sql}');

CREATE OR REPLACE VIEW parcels AS
SELECT id, code_insee, ${parcels_geom_expr} AS geom
FROM read_parquet('${parcels_sql}');

CREATE OR REPLACE VIEW addresses AS
SELECT id, code_insee, ${addresses_geom_expr} AS geom
FROM read_parquet('${addresses_sql}');

CREATE OR REPLACE MACRO match_prio(mt) AS (
  CASE mt
    WHEN 'PreExisting'     THEN 0
    WHEN 'Inside'          THEN 1
    WHEN 'BorderNear'      THEN 2
    WHEN 'FallbackNearest' THEN 3
    ELSE 100
  END
);

CREATE OR REPLACE MACRO addr_band(mt, d) AS (
  CASE
    WHEN mt IS NULL THEN 'UNMATCHED'
    WHEN mt IN ('PreExisting','Inside') THEN 'Inside'
    WHEN d <= 5  THEN '0-5'
    WHEN d <= 15 THEN '5-15'
    WHEN d <= 50 THEN '15-50'
    ELSE '>50'
  END
);

CREATE OR REPLACE MACRO parcel_band(mt, d) AS (
  CASE
    WHEN mt IS NULL THEN 'UNMATCHED'
    WHEN mt IN ('PreExisting','Inside') THEN 'Inside'
    WHEN d <= 100  THEN '0-100'
    WHEN d <= 250  THEN '100-250'
    WHEN d <= 500  THEN '250-500'
    WHEN d <= 1000 THEN '500-1000'
    WHEN d <= 1500 THEN '1000-1500'
    ELSE '>1500'
  END
);

CREATE OR REPLACE TABLE best_match_address AS
WITH ranked AS (
  SELECT
    id_ban, id_parcelle, match_type, distance_m, confidence,
    ROW_NUMBER() OVER (
      PARTITION BY id_ban
      ORDER BY match_prio(match_type) ASC, distance_m ASC, id_parcelle ASC
    ) AS rn
  FROM matches
  WHERE id_parcelle IS NOT NULL
    AND match_type IS NOT NULL
    AND match_type <> 'None'
)
SELECT * FROM ranked WHERE rn = 1;

CREATE OR REPLACE TABLE best_match_parcel AS
WITH ranked AS (
  SELECT
    id_parcelle, id_ban, match_type, distance_m, confidence,
    ROW_NUMBER() OVER (
      PARTITION BY id_parcelle
      ORDER BY match_prio(match_type) ASC, distance_m ASC, id_ban ASC
    ) AS rn
  FROM matches
  WHERE id_parcelle IS NOT NULL
    AND match_type IS NOT NULL
    AND match_type <> 'None'
)
SELECT * FROM ranked WHERE rn = 1;

COPY (
  SELECT
    a.id AS id_ban,
    a.code_insee,
    ST_X(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS lon,
    ST_Y(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS lat,
    b.id_parcelle,
    b.match_type,
    b.distance_m,
    b.confidence,
    addr_band(b.match_type, b.distance_m) AS class_match
  FROM addresses a
  LEFT JOIN best_match_address b ON a.id = b.id_ban
) TO '${addr_out}' (FORMAT 'CSV', HEADER);

COPY (
  SELECT
    p.id AS id_parcelle,
    p.code_insee,
    b.id_ban,
    b.match_type,
    b.distance_m,
    b.confidence,
    parcel_band(b.match_type, b.distance_m) AS parcel_class,
    ST_AsGeoJSON(ST_Transform(p.geom, 'EPSG:2154', 'OGC:CRS84')) AS geometry
  FROM parcels p
  LEFT JOIN best_match_parcel b ON p.id = b.id_parcelle
) TO '${parc_out}' (FORMAT 'CSV', HEADER);

COPY (
  SELECT
    b.id_ban,
    a.code_insee,
    b.id_parcelle,
    b.match_type,
    b.distance_m,
    b.confidence,
    addr_band(b.match_type, b.distance_m) AS class_match,
    ST_X(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lon,
    ST_Y(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lat,
    ST_X(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lon,
    ST_Y(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lat
  FROM best_match_address b
  JOIN addresses a ON a.id = b.id_ban
  JOIN parcels   p ON p.id = b.id_parcelle
) TO '${links_addr_out}' (FORMAT 'CSV', HEADER);

COPY (
  SELECT
    b.id_parcelle,
    p.code_insee,
    b.id_ban,
    b.match_type,
    b.distance_m,
    b.confidence,
    parcel_band(b.match_type, b.distance_m) AS class_match,
    ST_X(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lon,
    ST_Y(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lat,
    ST_X(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lon,
    ST_Y(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lat
  FROM best_match_parcel b
  JOIN parcels   p ON p.id = b.id_parcelle
  JOIN addresses a ON a.id = b.id_ban
) TO '${links_parcel_out}' (FORMAT 'CSV', HEADER);
SQL
