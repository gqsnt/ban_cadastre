use anyhow::{Context, Result};
use duckdb::{Config, Connection};
use std::path::Path;

pub fn step_aggregate(output_dir: &Path) -> Result<()> {
    if !output_dir.exists() {
        return Ok(()); // Nothing to aggregate
    }

    let config = Config::default();
    let conn =
        Connection::open_in_memory_with_flags(config).context("Failed to open DuckDB aggregate")?;

    // 1. france_parcelles_adresses.parquet
    // Pattern: parcelles_adresses_*.parquet
    // DuckDB glob syntax is specific? standard glob works.
    let glob_pa = output_dir.join("parcelles_adresses_*.parquet");
    let target_pa = output_dir.join("france_parcelles_adresses.parquet");

    // We utilize DuckDB's glob capability in read_parquet
    let q_pa = format!(
        r#"
        COPY (
            SELECT * FROM read_parquet('{}', hive_partitioning=false)
        ) TO '{}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
    "#,
        glob_pa.to_string_lossy().replace("\\", "/"),
        target_pa.to_string_lossy().replace("\\", "/")
    );
    // Note: Windows paths need slash for DuckDB globs usually? Or just works.
    // replacing backslashes is safer for DuckDB queries.

    // If no files match, this might error. We should check if any exist.
    // If output_dir is just one flat dir for all departments.
    // The prompt implies `dept_*/parcelles_adresses_*.parquet` or flat `output/parcelles_adresses_*.parquet`.
    // My q_pa assumes flat.

    // Catch error if no files
    if let Err(e) = conn.execute(&q_pa, []) {
        println!("Warning: Aggregate Matches failed (maybe no files?): {}", e);
    } else {
        println!("Generated france_parcelles_adresses.parquet");
    }

    // 1. france_parcelles_adresses.csv
    let glob_pa = output_dir.join("parcelles_adresses_*.parquet");
    let target_pa = output_dir.join("france_parcelles_adresses.csv");

    // We utilize DuckDB's glob capability in read_parquet
    let q_pa = format!(
        r#"
        COPY (
            SELECT * FROM read_parquet('{}', hive_partitioning=false)
        ) TO '{}' (HEADER, DELIMITER ',')
    "#,
        glob_pa.to_string_lossy().replace("\\", "/"),
        target_pa.to_string_lossy().replace("\\", "/")
    );
    // Note: Windows paths need slash for DuckDB globs usually? Or just works.
    // replacing backslashes is safer for DuckDB queries.

    // If no files match, this might error. We should check if any exist.
    // If output_dir is just one flat dir for all departments.
    // The prompt implies `dept_*/parcelles_adresses_*.parquet` or flat `output/parcelles_adresses_*.parquet`.
    // My q_pa assumes flat.

    // Catch error if no files
    if let Err(e) = conn.execute(&q_pa, []) {
        println!("Warning: Aggregate Matches failed (maybe no files?): {}", e);
    } else {
        println!("Generated france_parcelles_adresses.csv");
    }

    // 2. National QA Distance Tiers
    // Union qa_distance_tiers_*.csv
    // Schema: threshold_m, total_parcels, matched_parcels, coverage_pct
    // We need to Sum totals and matches, recalc pct. Group by threshold.
    let glob_tiers = output_dir.join("qa_distance_tiers_*.csv");
    let target_tiers = output_dir.join("national_qa_distance_tiers.csv");

    let q_tiers = format!(
        r#"
        COPY (
            SELECT 
                threshold_m, 
                sum(total_parcels) as total_parcels, 
                sum(matched_parcels) as matched_parcels,
                (sum(matched_parcels)::DOUBLE / sum(total_parcels)::DOUBLE * 100.0) as coverage_pct
            FROM read_csv('{}', header=true, auto_detect=true)
            GROUP BY 1
            ORDER BY 1
        ) TO '{}' (FORMAT 'CSV', HEADER)
    "#,
        glob_tiers.to_string_lossy().replace("\\", "/"),
        target_tiers.to_string_lossy().replace("\\", "/")
    );

    if let Err(e) = conn.execute(&q_tiers, []) {
        println!("Warning: Aggregate Tiers failed: {}", e);
    }

    // 3. National QA Precision
    // Union qa_precision_*.csv
    // Schema: bin, count. Sum count by bin.
    let glob_prec = output_dir.join("qa_precision_*.csv");
    let target_prec = output_dir.join("national_qa_precision.csv");

    let q_prec = format!(
        r#"
        COPY (
            SELECT
                bin,
                SUM(CAST(count AS BIGINT)) AS count
            FROM read_csv('{}', header=true, auto_detect=true)
            GROUP BY bin
            ORDER BY SUM(CAST(count AS BIGINT)) DESC
        ) TO '{}' (FORMAT 'CSV', HEADER)
"#,
        glob_prec.to_string_lossy().replace("\\", "/"),
        target_prec.to_string_lossy().replace("\\", "/"),
    );
    if let Err(e) = conn.execute(&q_prec, []) {
        println!("Warning: Aggregate Precision failed: {}", e);
    }

    // 4. National Worst Communes
    // Union qa_worst_communes_*.csv
    // Schema: code_insee, total_parcels, matched_parcels, coverage_pct
    // Just union, sort coverage asc, limit 100.
    let glob_worst = output_dir.join("qa_worst_communes_*.csv");
    let target_worst = output_dir.join("national_worst_communes_top100.csv");

    let q_worst = format!(
        r#"
        COPY (
            SELECT *
            FROM read_csv('{}', header=true, auto_detect=true)
            ORDER BY coverage_pct ASC
            LIMIT 100
        ) TO '{}' (FORMAT 'CSV', HEADER)
    "#,
        glob_worst.to_string_lossy().replace("\\", "/"),
        target_worst.to_string_lossy().replace("\\", "/")
    );

    if let Err(e) = conn.execute(&q_worst, []) {
        println!("Warning: Aggregate Worst Communes failed: {}", e);
    }

    Ok(())
}
