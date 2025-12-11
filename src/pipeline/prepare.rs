use anyhow::{anyhow, Context, Result};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub fn step_prepare_parcels(input_json: &Path, output_parquet: &Path) -> Result<()> {
    if output_parquet.exists() {
        return Ok(());
    }

    let input_str = input_json
        .to_str()
        .ok_or_else(|| anyhow!("Invalid input path"))?;
    let output_str = output_parquet
        .to_str()
        .ok_or_else(|| anyhow!("Invalid output path"))?;

    if let Some(parent) = output_parquet.parent() {
        fs::create_dir_all(parent)?;
    }

    // Script SQL équivalent à ton batch
    let sql = format!(
        r#"
INSTALL spatial; LOAD spatial;

-- 1. Lecture brute du GeoJSON
CREATE OR REPLACE TABLE parcelles_raw AS
SELECT * FROM ST_Read('{input}');

-- 2. Nettoyage + reprojection en GEOMETRY
CREATE OR REPLACE TABLE parcelles_clean AS
SELECT
    id AS id,
    CAST(commune AS VARCHAR) AS code_insee,
    ST_CollectionExtract(
        ST_MakeValid(
            ST_Transform(
                ST_Force2D(geom),
                'OGC:CRS84',
                'EPSG:2154'
            )
        ),
        3
    ) AS geom
FROM parcelles_raw
WHERE geom IS NOT NULL
  AND id IS NOT NULL
  AND commune IS NOT NULL;

-- 3. Filtrer les géométries vides (geom est bien un GEOMETRY ici)
DELETE FROM parcelles_clean
WHERE geom IS NULL OR ST_IsEmpty(geom);

-- 4. Export Parquet avec geom en WKB_BLOB pour ton loader Rust
COPY (
    SELECT
        id AS id,
        code_insee,
        ST_AsWKB(geom) AS geom
    FROM parcelles_clean
) TO '{output}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');

-- 5. Petit check
SELECT 'Parcels: ' || COUNT(*) FROM parcelles_clean;
"#,
        input = input_str.replace("'", "''"),
        output = output_str.replace("'", "''"),
    );

    // Lancer `duckdb` en process séparé
    let mut child = Command::new("duckdb")
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to start duckdb CLI")?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow!("Failed to open duckdb stdin"))?;
        stdin.write_all(sql.as_bytes())?;
    }

    let status = child.wait().context("Failed to wait for duckdb process")?;
    if !status.success() {
        return Err(anyhow!(
            "duckdb prepare parcels failed with status {}",
            status
        ));
    }

    Ok(())
}

pub fn step_prepare_addresses(input_csv: &Path, output_parquet: &Path) -> Result<()> {
    // Si le Parquet existe déjà, on ne refait pas le boulot
    if output_parquet.exists() {
        return Ok(());
    }

    let input_str = input_csv
        .to_str()
        .ok_or_else(|| anyhow!("Invalid input path"))?;
    let output_str = output_parquet
        .to_str()
        .ok_or_else(|| anyhow!("Invalid output path"))?;

    if let Some(parent) = output_parquet.parent() {
        fs::create_dir_all(parent)?;
    }

    // Script SQL équivalent à ton batch, paramétré avec les chemins
    let sql = format!(
        r#"
INSTALL spatial; LOAD spatial;

CREATE OR REPLACE TABLE adresses_raw AS
SELECT id, code_insee, x, y, lon, lat, cad_parcelles
FROM read_csv('{input}', auto_detect=true, header=true, ignore_errors=true);

CREATE OR REPLACE TABLE adresses_clean AS
SELECT
    CAST(id AS VARCHAR)         AS id,
    CAST(code_insee AS VARCHAR) AS code_insee,
    CASE
        WHEN x   IS NOT NULL AND y   IS NOT NULL THEN ST_Point(x, y)
        WHEN lon IS NOT NULL AND lat IS NOT NULL THEN ST_Transform(
            ST_Point(lon, lat),
            'OGC:CRS84',
            'EPSG:2154'
        )
        ELSE NULL
    END AS geom,
    CASE
        WHEN cad_parcelles IS NULL OR cad_parcelles = '' THEN NULL
        ELSE cad_parcelles
    END AS existing_link
FROM adresses_raw
WHERE (x IS NOT NULL OR lon IS NOT NULL)
  AND id IS NOT NULL
  AND code_insee IS NOT NULL;

COPY adresses_clean TO '{output}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
SELECT 'Adresses: ' || COUNT(*) FROM adresses_clean;
"#,
        input = input_str.replace('\'', "''"),
        output = output_str.replace('\'', "''"),
    );

    // Lancement de duckdb : `duckdb :memory:` avec le SQL sur stdin
    let mut child = Command::new("duckdb")
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to start duckdb CLI for addresses")?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow!("Failed to open duckdb stdin"))?;
        stdin.write_all(sql.as_bytes())?;
    }

    let status = child
        .wait()
        .context("Failed to wait for duckdb process (addresses)")?;
    if !status.success() {
        return Err(anyhow!(
            "duckdb prepare addresses failed with status {}",
            status
        ));
    }

    Ok(())
}
