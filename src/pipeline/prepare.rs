use anyhow::{anyhow, Context, Result};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn sql_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''")
}

pub fn step_prepare_parcels(input_json: &Path, output_parquet: &Path) -> Result<()> {
    if output_parquet.exists() {
        return Ok(());
    }
    if let Some(parent) = output_parquet.parent() {
        fs::create_dir_all(parent)?;
    }

    let sql = format!(
        r#"
INSTALL spatial; LOAD spatial;

CREATE OR REPLACE TABLE parcelles_raw AS
SELECT * FROM ST_Read('{input}');

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

DELETE FROM parcelles_clean
WHERE geom IS NULL OR ST_IsEmpty(geom);

COPY (
  SELECT
    id AS id,
    code_insee,
    ST_AsWKB(geom) AS geom
  FROM parcelles_clean
) TO '{output}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
"#,
        input = sql_path(input_json),
        output = sql_path(output_parquet),
    );

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
    if output_parquet.exists() {
        return Ok(());
    }
    if let Some(parent) = output_parquet.parent() {
        fs::create_dir_all(parent)?;
    }

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
    WHEN lon IS NOT NULL AND lat IS NOT NULL THEN ST_Transform(ST_Point(lon, lat), 'OGC:CRS84', 'EPSG:2154')
    ELSE NULL
  END AS geom,
  CASE
    WHEN cad_parcelles IS NULL OR cad_parcelles = '' THEN NULL
    ELSE cad_parcelles
  END AS existing_link
FROM adresses_raw
WHERE (
    (x IS NOT NULL AND y IS NOT NULL) OR
    (lon IS NOT NULL AND lat IS NOT NULL)
)
AND id IS NOT NULL
AND code_insee IS NOT NULL;

-- Export WKB to match Rust loader expectations.
COPY (
  SELECT
    id,
    code_insee,
    ST_AsWKB(geom) AS geom,
    existing_link
  FROM adresses_clean
  WHERE geom IS NOT NULL
) TO '{output}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
"#,
        input = sql_path(input_csv),
        output = sql_path(output_parquet),
    );

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
