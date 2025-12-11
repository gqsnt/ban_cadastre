
param(
    [string]$Dept      = "69",
    [string]$DataDir   = "data/ban_cadastre",
    [string]$DuckdbExe = "duckdb"
)

# 1. Setup Paths & Validation
$baseDir       = $DataDir -replace '\\','/'
$matchesPath   = "$baseDir/batch_results/matches_${Dept}.parquet"
$parcelsPath   = "$baseDir/staging/parcelles_${Dept}.parquet"
$addressesPath = "$baseDir/staging/adresses_${Dept}.parquet"

$keplerDirWin = Join-Path $DataDir "kepler"
$keplerDir    = $keplerDirWin -replace '\\','/'

Write-Host "=== Exporting Kepler Data for Dept $Dept ==="
Write-Host "Inputs:"
Write-Host "  - Matches: $matchesPath"
Write-Host "  - Parcels: $parcelsPath"
Write-Host "  - Address: $addressesPath"

# Validate Inputs
if (!(Test-Path $matchesPath) -or !(Test-Path $parcelsPath)) {
    Write-Error "CRITICAL: Input Parquet files not found. Please run the pipeline (Link mode) first."
    exit 1
}

# Create Output Directory
if (!(Test-Path $keplerDirWin)) {
    New-Item -ItemType Directory -Force -Path $keplerDirWin | Out-Null
}

$addrOut         = "$keplerDir/kepler_addresses_${Dept}.csv"
$parcOut         = "$keplerDir/kepler_parcels_${Dept}.csv"
$linksAddrOut    = "$keplerDir/kepler_links_addr_${Dept}.csv"
$linksParcelOut  = "$keplerDir/kepler_links_parcel_${Dept}.csv"

# 2. DuckDB SQL Logic
$sql = @"
INSTALL spatial; LOAD spatial;

-- Load Data Views
CREATE VIEW matches AS SELECT * FROM read_parquet('$matchesPath');
CREATE VIEW parcels AS SELECT * FROM read_parquet('$parcelsPath');
CREATE VIEW addresses AS SELECT * FROM read_parquet('$addressesPath');

-- 1) Compute Best Match per ADDRESS (Priority: PreExisting < Inside < Border < Fallback)
CREATE OR REPLACE TABLE best_match_address AS
WITH m_ranked AS (
  SELECT
    id_ban,
    id_parcelle,
    match_type,
    distance_m,
    ROW_NUMBER() OVER (
      PARTITION BY id_ban
      ORDER BY
        CASE match_type
          WHEN 'PreExisting'     THEN 0
          WHEN 'Inside'          THEN 1
          WHEN 'BorderNear'      THEN 2
          WHEN 'FallbackNearest' THEN 3
          ELSE 100
        END ASC,
        distance_m ASC
    ) AS rn
  FROM matches
  WHERE id_parcelle IS NOT NULL
    AND match_type IS NOT NULL
    AND match_type <> 'None'
)
SELECT * FROM m_ranked WHERE rn = 1;

-- 2) Compute Best Match per PARCEL (Inverse view)
CREATE OR REPLACE TABLE best_match_parcel AS
WITH m_ranked AS (
  SELECT
    id_ban,
    id_parcelle,
    match_type,
    distance_m,
    ROW_NUMBER() OVER (
      PARTITION BY id_parcelle
      ORDER BY
        CASE match_type
          WHEN 'PreExisting'     THEN 0
          WHEN 'Inside'          THEN 1
          WHEN 'BorderNear'      THEN 2
          WHEN 'FallbackNearest' THEN 3
          ELSE 100
        END ASC,
        distance_m ASC
    ) AS rn
  FROM matches
  WHERE id_parcelle IS NOT NULL
    AND match_type IS NOT NULL
    AND match_type <> 'None'
)
SELECT * FROM m_ranked WHERE rn = 1;

-- 3) Aggregate Parcel Stats
CREATE OR REPLACE TABLE best_parcel_dist AS
SELECT
  id_parcelle,
  CASE
    WHEN match_type IN ('PreExisting','Inside') THEN 0.0
    ELSE distance_m
  END AS best_dist
FROM best_match_parcel;

----------------------------------------------------
-- 4) Export: Addresses (Points)
----------------------------------------------------
COPY (
  SELECT
    a.id         AS id_ban,
    a.code_insee,
    ST_X(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS lon,
    ST_Y(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS lat,
    b.id_parcelle,
    b.match_type,
    b.distance_m,
    CASE
      WHEN b.id_parcelle IS NULL THEN 'UNMATCHED'
      WHEN b.match_type IN ('PreExisting','Inside') THEN 'Inside'
      WHEN b.distance_m <= 5   THEN '0-5'
      WHEN b.distance_m <= 15  THEN '5-15'
      WHEN b.distance_m <= 50  THEN '15-50'
      ELSE '>50'
    END AS class_match
  FROM addresses a
  LEFT JOIN best_match_address b ON a.id = b.id_ban
) TO '$addrOut' (FORMAT 'CSV', HEADER);

----------------------------------------------------
-- 5) Export: Parcels (Polygons)
----------------------------------------------------
COPY (
  SELECT
    p.id         AS id_parcelle,
    p.code_insee,
    CASE
      WHEN b.best_dist IS NULL     THEN '>1500'
      WHEN b.best_dist <= 100      THEN '0-100'
      WHEN b.best_dist <= 250      THEN '100-250'
      WHEN b.best_dist <= 500      THEN '250-500'
      WHEN b.best_dist <= 1000     THEN '500-1000'
      WHEN b.best_dist <= 1500     THEN '1000-1500'
      ELSE '>1500'
    END AS parcel_class,
    ST_AsGeoJSON(ST_Transform(p.geom, 'EPSG:2154', 'OGC:CRS84')) AS geometry
  FROM parcels p
  LEFT JOIN best_parcel_dist b ON p.id = b.id_parcelle
) TO '$parcOut' (FORMAT 'CSV', HEADER);

----------------------------------------------------
-- 6) Export: Links (Address -> Parcel)
----------------------------------------------------
COPY (
  SELECT
    a.id         AS id_ban,
    a.code_insee,
    b.id_parcelle,
    b.match_type,
    b.distance_m,
    CASE
      WHEN b.id_parcelle IS NULL THEN 'UNMATCHED'
      WHEN b.match_type IN ('PreExisting','Inside') THEN 'Inside'
      WHEN b.distance_m <= 5   THEN '0-5'
      WHEN b.distance_m <= 15  THEN '5-15'
      WHEN b.distance_m <= 50  THEN '15-50'
      ELSE '>50'
    END AS class_match,
    ST_X(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lon,
    ST_Y(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lat,
    ST_X(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lon,
    ST_Y(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lat
  FROM addresses a
  LEFT JOIN best_match_address b ON a.id = b.id_ban
  LEFT JOIN parcels p           ON p.id = b.id_parcelle
  WHERE b.id_parcelle IS NOT NULL
) TO '$linksAddrOut' (FORMAT 'CSV', HEADER);

----------------------------------------------------
-- 7) Export: Links (Parcel -> Address)
----------------------------------------------------
COPY (
  SELECT
    p.id         AS id_parcelle,
    p.code_insee,
    b.id_ban,
    b.match_type,
    b.distance_m,
    CASE
      WHEN b.id_ban IS NULL THEN 'UNMATCHED'
      WHEN b.match_type IN ('PreExisting','Inside') THEN 'Inside'
      WHEN b.distance_m <= 5   THEN '0-5'
      WHEN b.distance_m <= 15  THEN '5-15'
      WHEN b.distance_m <= 50  THEN '15-50'
      ELSE '>50'
    END AS class_match,
    ST_X(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lon,
    ST_Y(ST_Transform(ST_Centroid(p.geom), 'EPSG:2154', 'OGC:CRS84')) AS parc_lat,
    ST_X(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lon,
    ST_Y(ST_Transform(a.geom, 'EPSG:2154', 'OGC:CRS84')) AS addr_lat
  FROM parcels p
  LEFT JOIN best_match_parcel b ON p.id = b.id_parcelle
  LEFT JOIN addresses a         ON a.id = b.id_ban
  WHERE b.id_ban IS NOT NULL
) TO '$linksParcelOut' (FORMAT 'CSV', HEADER);
"@

Write-Host "Running DuckDB Export..."
$sql | & $DuckdbExe ":memory:" -batch
Write-Host "Done. Files generated in $keplerDirWin"
