param(
    [string]$Dept      = "69",
    [string]$DataDir   = "data/ban_cadastre",
    [string]$DuckdbExe = "duckdb"
)

# Normalisation des chemins pour DuckDB (slashs)
$baseDir       = $DataDir -replace '\\','/'
$matchesPath   = "$baseDir/batch_results/matches_${Dept}.parquet"
$parcelsPath   = "$baseDir/staging/parcelles_${Dept}.parquet"
$addressesPath = "$baseDir/staging/adresses_${Dept}.parquet"

$keplerDirWin = Join-Path $DataDir "kepler"
$keplerDir    = $keplerDirWin -replace '\\','/'

if (!(Test-Path $keplerDirWin)) {
    New-Item -ItemType Directory -Force -Path $keplerDirWin | Out-Null
}

$addrOut         = "$keplerDir/kepler_addresses_${Dept}.csv"
$parcOut         = "$keplerDir/kepler_parcels_${Dept}.csv"
$linksAddrOut    = "$keplerDir/kepler_links_addr_${Dept}.csv"
$linksParcelOut  = "$keplerDir/kepler_links_parcel_${Dept}.csv"

$sql = @"
INSTALL spatial; LOAD spatial;

-- Vues de base
CREATE VIEW matches AS
  SELECT * FROM read_parquet('$matchesPath');

CREATE VIEW parcels AS
  SELECT * FROM read_parquet('$parcelsPath');

CREATE VIEW addresses AS
  SELECT * FROM read_parquet('$addressesPath');

-- 1) Meilleur match par ADRESSE (un lien par adresse)
CREATE OR REPLACE TABLE best_match_address AS
WITH m_ranked AS (
  SELECT
    id_ban,
    id_parcelle,
    match_type,
    distance_m,
    CASE match_type
      WHEN 'PreExisting'     THEN 0
      WHEN 'Inside'          THEN 1
      WHEN 'BorderNear'      THEN 2
      WHEN 'FallbackNearest' THEN 3
      ELSE 100
    END AS priority,
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

-- 2) Meilleur match par PARCELLE (un lien par parcelle)
CREATE OR REPLACE TABLE best_match_parcel AS
WITH m_ranked AS (
  SELECT
    id_ban,
    id_parcelle,
    match_type,
    distance_m,
    CASE match_type
      WHEN 'PreExisting'     THEN 0
      WHEN 'Inside'          THEN 1
      WHEN 'BorderNear'      THEN 2
      WHEN 'FallbackNearest' THEN 3
      ELSE 100
    END AS priority,
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

-- 3) Distance "best_dist" par parcelle (pour la classe 0–1500 / >1500)
CREATE OR REPLACE TABLE best_parcel_dist AS
SELECT
  id_parcelle,
  CASE
    WHEN match_type IN ('PreExisting','Inside') THEN 0.0
    ELSE distance_m
  END AS best_dist
FROM best_match_parcel;

----------------------------------------------------
-- 4) Adresses pour Kepler (points)
--    - class_match : Inside / 0-5 / 5-15 / 15-50 / >50 / UNMATCHED
----------------------------------------------------
COPY (
  SELECT
    a.id         AS id_ban,
    a.code_insee,
    ST_X(
      ST_Transform(
        a.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS lon,
    ST_Y(
      ST_Transform(
        a.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS lat,
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
-- 5) Parcelles pour Kepler (polygones)
--    - parcel_class : 0-100 / 100-250 / 250-500 / 500-1000 / 1000-1500 / >1500
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
    ST_AsGeoJSON(
      ST_Transform(
        p.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS geometry
  FROM parcels p
  LEFT JOIN best_parcel_dist b ON p.id = b.id_parcelle
) TO '$parcOut' (FORMAT 'CSV', HEADER);

----------------------------------------------------
-- 6) Liens ADRESSE -> PARCELLE (un lien par adresse)
--    colonnes : addr_lon, addr_lat, parc_lon, parc_lat
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
    ST_X(
      ST_Transform(
        a.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS addr_lon,
    ST_Y(
      ST_Transform(
        a.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS addr_lat,
    ST_X(
      ST_Transform(
        ST_Centroid(p.geom),
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS parc_lon,
    ST_Y(
      ST_Transform(
        ST_Centroid(p.geom),
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS parc_lat
  FROM addresses a
  LEFT JOIN best_match_address b ON a.id = b.id_ban
  LEFT JOIN parcels p           ON p.id = b.id_parcelle
  WHERE b.id_parcelle IS NOT NULL
) TO '$linksAddrOut' (FORMAT 'CSV', HEADER);

----------------------------------------------------
-- 7) Liens PARCELLE -> ADRESSE (un lien par parcelle)
--    colonnes : parc_lon, parc_lat, addr_lon, addr_lat
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
    ST_X(
      ST_Transform(
        ST_Centroid(p.geom),
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS parc_lon,
    ST_Y(
      ST_Transform(
        ST_Centroid(p.geom),
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS parc_lat,
    ST_X(
      ST_Transform(
        a.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS addr_lon,
    ST_Y(
      ST_Transform(
        a.geom,
        'EPSG:2154',
        'OGC:CRS84'
      )
    ) AS addr_lat
  FROM parcels p
  LEFT JOIN best_match_parcel b ON p.id = b.id_parcelle
  LEFT JOIN addresses a         ON a.id = b.id_ban
  WHERE b.id_ban IS NOT NULL
) TO '$linksParcelOut' (FORMAT 'CSV', HEADER);
"@

$sql | & $DuckdbExe ":memory:" -batch
