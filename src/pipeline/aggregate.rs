use anyhow::{Context, Result};
use duckdb::{Config, Connection};
use std::path::{Path, PathBuf};
use tracing::{info, warn};
pub struct AggregateOutcome {
    pub generated: Vec<PathBuf>,
    pub missing_inputs: Vec<String>,
    pub partial: bool,
}

fn list_matching_files(dir: &Path, prefix: &str, suffix: &str) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for ent in std::fs::read_dir(dir).context("Failed to read output directory")? {
        let ent = ent?;
        let path = ent.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with(prefix) && name.ends_with(suffix) {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

pub fn step_aggregate(output_dir: &Path) -> Result<AggregateOutcome> {
    if !output_dir.exists() {
        return Ok(AggregateOutcome {
            generated: vec![],
            missing_inputs: vec!["output_dir_missing".into()],
            partial: true,
        });
    }

    let config = Config::default();
    let conn =
        Connection::open_in_memory_with_flags(config).context("Failed to open DuckDB aggregate")?;
    let mut generated: Vec<PathBuf> = Vec::new();
    let mut missing_inputs: Vec<String> = Vec::new();
    // 1. france_parcelles_adresses.parquet
    let pa_inputs = list_matching_files(output_dir, "parcelles_adresses_", ".parquet")?;
    if pa_inputs.is_empty() {
        warn!(output_dir=?output_dir, "aggregate: missing inputs parcelles_adresses_*.parquet (skipping france_parcelles_adresses.*)");
        missing_inputs.push("parcelles_adresses_*.parquet".into());
    } else {
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

        conn.execute(&q_pa, [])
            .context("Aggregate france_parcelles_adresses.parquet")?;
        info!(artifact=?target_pa, "artifact generated");
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
    conn.execute(&q_pa, [])
        .context("Aggregate france_parcelles_adresses.csv")?;
    info!(artifact=?target_pa, "artifact generated");

    // 2. National QA Distance Tiers
    // Union qa_distance_tiers_*.csv
    // Schema: threshold_m, total_parcels, matched_parcels, coverage_pct
    // We need to Sum totals and matches, recalc pct. Group by threshold.
    let tiers_inputs = list_matching_files(output_dir, "qa_distance_tiers_", ".csv")?;
    if tiers_inputs.is_empty() {
        warn!(output_dir=?output_dir, "aggregate: missing inputs qa_distance_tiers_*.csv (skipping national_qa_distance_tiers.csv)");
        missing_inputs.push("qa_distance_tiers_*.csv".into());
    } else {
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

        conn.execute(&q_tiers, [])
            .context("Aggregate national_qa_distance_tiers.csv")?;
        info!(artifact=?target_tiers, "artifact generated");
        generated.push(target_tiers);
    }
    // 3. National QA Precision
    // Union qa_precision_*.csv
    // Schema: bin, count. Sum count by bin.
    let prec_inputs = list_matching_files(output_dir, "qa_precision_", ".csv")?;
    if prec_inputs.is_empty() {
        warn!(output_dir=?output_dir, "aggregate: missing inputs qa_precision_*.csv (skipping national_qa_precision.csv)");
        missing_inputs.push("qa_precision_*.csv".into());
    } else {
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
        conn.execute(&q_prec, [])
            .context("Aggregate national_qa_precision.csv")?;
        info!(artifact=?target_prec, "artifact generated");
        generated.push(target_prec);
    }
    // 4. National Worst Communes
    // Union qa_worst_communes_*.csv
    // Schema: code_insee, total_parcels, matched_parcels, coverage_pct
    // Just union, sort coverage asc, limit 100.
    let worst_inputs = list_matching_files(output_dir, "qa_worst_communes_", ".csv")?;
    if worst_inputs.is_empty() {
        warn!(output_dir=?output_dir, "aggregate: missing inputs qa_worst_communes_*.csv (skipping national_worst_communes_top100.csv)");
        missing_inputs.push("qa_worst_communes_*.csv".into());
    } else {
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

        conn.execute(&q_worst, [])
            .context("Aggregate national_worst_communes_top100.csv")?;
        info!(artifact=?target_worst, "artifact generated");
        generated.push(target_worst);
    }
    let partial = !missing_inputs.is_empty();
    Ok(AggregateOutcome {
        generated,
        missing_inputs,
        partial,
    })
}
