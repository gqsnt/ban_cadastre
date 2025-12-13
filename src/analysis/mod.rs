use crate::cli::AnalyzeArgs;
use anyhow::{Context, Result};
use chrono::Utc;
use polars::lazy::dsl::{col, len, lit, when};
use polars::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use tracing::{info, instrument, warn};

const COVERAGE_THRESHOLD_M: f64 = 1500.0;

pub struct AnalyzeOutcome {
    pub output_dir: PathBuf,
    pub partial: bool,
    pub expected_departments: usize,
    pub analyzed_departments: usize,
    pub skipped_missing_matches: usize,
    pub skipped_missing_parcels: usize,
    pub invalid_manifest_rows: usize,
}

#[derive(Serialize)]
struct NationalSummary {
    total_parcels: i64,
    generated_at: String,
    coverage_threshold_m: f64,
    manifest_rows_total: usize,

    // QA-aligned headline coverage
    total_matched_parcels: i64,
    coverage_pct: f64,
    weighted_confidence_avg: f64,

    // additional metric (any best match)
    total_matched_parcels_any: i64,
    coverage_any_pct: f64,
    weighted_confidence_any_avg: f64,

    expected_departments: usize,
    analyzed_departments: usize,
    invalid_manifest_rows: usize,
    invalid_manifest_row_numbers: Vec<usize>,
    skipped_missing_matches: usize,
    skipped_missing_parcels: usize,
    missing_matches_departments: Vec<String>,
    missing_parcels_departments: Vec<String>,
    by_region: HashMap<String, RegionalStats>,
    match_type_distribution: HashMap<String, i64>,
}

#[derive(Serialize)]
struct RegionalStats {
    total_parcels: i64,

    matched_parcels: i64,
    coverage_pct: f64,
    weighted_confidence_avg: f64,

    matched_parcels_any: i64,
    coverage_any_pct: f64,
    weighted_confidence_any_avg: f64,

    match_type_distribution: HashMap<String, i64>,
}

pub type RegionAgg = HashMap<String, (i64, i64, f64, i64, f64, HashMap<String, i64>)>;

#[instrument(skip(args))]
pub fn run_analyze(args: AnalyzeArgs) -> Result<AnalyzeOutcome> {
    info!(results_dir=?args.results_dir, departments_file=?args.departments_file, output_dir=?args.output_dir, "starting analysis");
    let data_root = args.results_dir.clone();
    let matches_dir = data_root.join("batch_results");
    let staging_dir = data_root.join("staging");
    let qa_dir = data_root.join("output");
    let output_dir = args.output_dir.unwrap_or_else(|| data_root.clone());

    std::fs::create_dir_all(&output_dir)?;

    let depts_df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(args.departments_file.clone()))?
        .finish()
        .with_context(|| {
            format!(
                "Failed reading departments CSV: {:?}",
                args.departments_file
            )
        })?;

    let rows = depts_df.height();
    info!(departments_meta_rows = rows, "loaded departments metadata");

    let dept_col = depts_df.column("dept")?.str()?;
    let region_col = depts_df.column("region")?.str()?;
    let nom_col = depts_df.column("nom")?.str()?;
    let manifest_rows_total = rows;
    let mut summary_rows = Vec::new();

    let mut national_total_p = 0i64;

    // QA-aligned
    let mut national_matched_p = 0i64;
    let mut national_conf_sum = 0.0;

    // any
    let mut national_matched_any = 0i64;
    let mut national_conf_any_sum = 0.0;

    let mut national_match_dist: HashMap<String, i64> = HashMap::new();

    // region -> (tot, matched, confsum, matched_any, conf_any_sum, dist)
    let mut region_agg: RegionAgg = HashMap::new();

    let mut departement_details: Vec<(String, f64)> = Vec::new();
    let mut skipped_missing_matches = 0usize;
    let mut skipped_missing_parcels = 0usize;
    let mut analyzed = 0usize;
    let mut expected_departments = 0usize;
    let mut invalid_manifest_rows = 0usize;
    let mut invalid_manifest_row_numbers: Vec<usize> = Vec::new();
    let mut missing_matches_departments: Vec<String> = Vec::new();
    let mut missing_parcels_departments: Vec<String> = Vec::new();

    for i in 0..rows {
        let dept_code = match dept_col.get(i).map(str::trim).filter(|s| !s.is_empty()) {
            Some(v) => v,
            None => {
                invalid_manifest_rows += 1;
                invalid_manifest_row_numbers.push(i + 1);
                warn!(row = i + 1, "invalid manifest row: missing/empty dept");
                continue;
            }
        };
        let region = match region_col.get(i).map(str::trim).filter(|s| !s.is_empty()) {
            Some(v) => v,
            None => {
                invalid_manifest_rows += 1;
                invalid_manifest_row_numbers.push(i + 1);
                warn!(row = i + 1, dept=%dept_code, "invalid manifest row: missing/empty region");
                continue;
            }
        };
        let nom = match nom_col.get(i).map(str::trim).filter(|s| !s.is_empty()) {
            Some(v) => v,
            None => {
                invalid_manifest_rows += 1;
                invalid_manifest_row_numbers.push(i + 1);
                warn!(row = i + 1, dept=%dept_code, "invalid manifest row: missing/empty nom");
                continue;
            }
        };
        expected_departments += 1;
        let _span = tracing::info_span!("analyze_dept", dept = %dept_code).entered();
        let matches_file = matches_dir.join(format!("matches_{}.parquet", dept_code));
        let staging_parcels = staging_dir.join(format!("parcelles_{}.parquet", dept_code));

        if !matches_file.exists() {
            skipped_missing_matches += 1;
            missing_matches_departments.push(dept_code.to_string());
            warn!(dept=%dept_code, matches_file=?matches_file, "missing matches file; skipping department");
            continue;
        }

        // 1) total parcels
        let num_parcels: i64 = if staging_parcels.exists() {
            let p = staging_parcels.to_string_lossy();
            let lf = LazyFrame::scan_parquet(PlPath::from_str(&p), ScanArgsParquet::default())?;
            let df = lf
                .select([col("id").count().cast(DataType::Int64).alias("n_parcels")])
                .collect()?;
            df.column("n_parcels")?.i64()?.get(0).unwrap_or(0)
        } else {
            let qa_csv = qa_dir.join(format!("qa_distance_tiers_{}.csv", dept_code));
            if qa_csv.exists() {
                let df = CsvReadOptions::default()
                    .with_has_header(true)
                    .try_into_reader_with_file_path(Some(qa_csv.clone()))?
                    .finish()?;
                df.column("total_parcels")?
                    .cast(&DataType::Int64)?
                    .i64()?
                    .get(0)
                    .unwrap_or(0)
            } else {
                skipped_missing_parcels += 1;
                missing_parcels_departments.push(dept_code.to_string());
                warn!(dept=%dept_code, "missing parcels data (staging parquet and QA tiers missing); skipping department");
                continue;
            }
        };

        // 2) best match per parcel
        let mp = matches_file.to_string_lossy();
        let matches_lf =
            LazyFrame::scan_parquet(PlPath::from_str(&mp), ScanArgsParquet::default())?;
        let priority_expr = when(col("match_type").eq(lit("PreExisting")))
            .then(lit(0))
            .otherwise(
                when(col("match_type").eq(lit("Inside")))
                    .then(lit(1))
                    .otherwise(
                        when(col("match_type").eq(lit("BorderNear")))
                            .then(lit(2))
                            .otherwise(
                                when(col("match_type").eq(lit("FallbackNearest")))
                                    .then(lit(3))
                                    .otherwise(lit(100)),
                            ),
                    ),
            );

        let selector = col("id_parcelle").into_selector();

        let best_df = matches_lf
            .filter(col("id_parcelle").is_not_null())
            .select([
                col("id_parcelle"),
                col("id_ban"),
                col("match_type"),
                col("distance_m"),
                col("confidence"),
            ])
            .with_column(priority_expr.alias("prio"))
            .sort(
                ["id_parcelle", "prio", "distance_m", "id_ban"],
                SortMultipleOptions::default(),
            )
            .unique(selector, UniqueKeepStrategy::First)
            .collect()?;

        // distribution by match_type (best-per-parcel)
        let match_counts = best_df
            .clone()
            .lazy()
            .group_by([col("match_type")])
            .agg([len().cast(DataType::Int64).alias("n")])
            .collect()?;

        let mc_type = match_counts.column("match_type")?.str()?;
        let mc_cnt = match_counts.column("n")?.i64()?;

        let mut dept_dist: HashMap<String, i64> = HashMap::new();
        for j in 0..match_counts.height() {
            if let Some(t) = mc_type.get(j) {
                let c = mc_cnt.get(j).unwrap_or(0);
                if t != "None" {
                    dept_dist.insert(t.to_string(), c);
                }
            }
        }

        // any best match count
        let matched_any: i64 = dept_dist.values().sum();

        // QA-aligned accepted mask (on best-per-parcel rows)
        let accepted_mask = col("match_type")
            .eq(lit("PreExisting"))
            .or(col("match_type").eq(lit("Inside")))
            .or(col("distance_m")
                .cast(DataType::Float64)
                .lt_eq(lit(COVERAGE_THRESHOLD_M)));
        let matched_1500_df = best_df
            .clone()
            .lazy()
            .filter(accepted_mask.clone())
            .select([len().cast(DataType::Int64).alias("n")])
            .collect()?;
        let matched_1500: i64 = matched_1500_df.column("n")?.i64()?.get(0).unwrap_or(0);

        // avg confidence (any)
        let avg_any_df = best_df
            .clone()
            .lazy()
            .select([col("confidence").mean().alias("avg_conf_any")])
            .collect()?;
        let avg_conf_any = avg_any_df
            .column("avg_conf_any")?
            .f64()?
            .get(0)
            .unwrap_or(0.0);

        // avg confidence (accepted)
        let avg_acc_df = best_df
            .clone()
            .lazy()
            .filter(accepted_mask)
            .select([col("confidence").mean().alias("avg_conf_acc")])
            .collect()?;
        let avg_conf_acc = avg_acc_df
            .column("avg_conf_acc")?
            .f64()?
            .get(0)
            .unwrap_or(0.0);

        let coverage_any = if num_parcels > 0 {
            matched_any as f64 / num_parcels as f64 * 100.0
        } else {
            0.0
        };
        let coverage_1500 = if num_parcels > 0 {
            matched_1500 as f64 / num_parcels as f64 * 100.0
        } else {
            0.0
        };

        departement_details.push((dept_code.to_string(), coverage_1500));

        summary_rows.push(format!(
            "{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2}",
            dept_code,
            nom,
            region,
            num_parcels,
            matched_1500,
            coverage_1500,
            avg_conf_acc,
            coverage_any,
            avg_conf_any,
            coverage_any - coverage_1500
        ));

        national_total_p += num_parcels;

        national_matched_p += matched_1500;
        national_conf_sum += avg_conf_acc * (matched_1500 as f64);

        national_matched_any += matched_any;
        national_conf_any_sum += avg_conf_any * (matched_any as f64);

        for (k, v) in &dept_dist {
            *national_match_dist.entry(k.clone()).or_insert(0) += *v;
        }

        let r_entry =
            region_agg
                .entry(region.to_string())
                .or_insert((0, 0, 0.0, 0, 0.0, HashMap::new()));

        r_entry.0 += num_parcels;

        r_entry.1 += matched_1500;
        r_entry.2 += avg_conf_acc * (matched_1500 as f64);

        r_entry.3 += matched_any;
        r_entry.4 += avg_conf_any * (matched_any as f64);

        for (k, v) in dept_dist {
            *r_entry.5.entry(k).or_insert(0) += v;
        }

        analyzed += 1;
        info!(
            num_parcels,
            matched_accepted_1500 = matched_1500,
            coverage_accepted_1500_pct = coverage_1500,
            avg_conf_accepted_1500 = avg_conf_acc,
            matched_any_best = matched_any,
            coverage_any_best_pct = coverage_any,
            avg_conf_any_best = avg_conf_any,
            "dept analyzed"
        );
    }

    let national_cov = if national_total_p > 0 {
        national_matched_p as f64 / national_total_p as f64 * 100.0
    } else {
        0.0
    };

    let national_avg_conf = if national_matched_p > 0 {
        national_conf_sum / national_matched_p as f64
    } else {
        0.0
    };

    let national_cov_any = if national_total_p > 0 {
        national_matched_any as f64 / national_total_p as f64 * 100.0
    } else {
        0.0
    };

    let national_avg_conf_any = if national_matched_any > 0 {
        national_conf_any_sum / national_matched_any as f64
    } else {
        0.0
    };

    let mut reg_summaries = HashMap::new();
    for (name, (tot, mat, conf_sum, mat_any, conf_any_sum, dist)) in region_agg {
        let cov = if tot > 0 {
            mat as f64 / tot as f64 * 100.0
        } else {
            0.0
        };
        let avg = if mat > 0 { conf_sum / mat as f64 } else { 0.0 };

        let cov_any = if tot > 0 {
            mat_any as f64 / tot as f64 * 100.0
        } else {
            0.0
        };
        let avg_any = if mat_any > 0 {
            conf_any_sum / mat_any as f64
        } else {
            0.0
        };

        reg_summaries.insert(
            name,
            RegionalStats {
                total_parcels: tot,
                matched_parcels: mat,
                coverage_pct: cov,
                weighted_confidence_avg: avg,
                matched_parcels_any: mat_any,
                coverage_any_pct: cov_any,
                weighted_confidence_any_avg: avg_any,
                match_type_distribution: dist,
            },
        );
    }
    let partial = skipped_missing_matches > 0 || skipped_missing_parcels > 0;
    let summary = NationalSummary {
        generated_at: Utc::now().to_rfc3339(),
        coverage_threshold_m: COVERAGE_THRESHOLD_M,
        total_parcels: national_total_p,
        total_matched_parcels: national_matched_p,
        coverage_pct: national_cov,
        manifest_rows_total,
        weighted_confidence_avg: national_avg_conf,
        total_matched_parcels_any: national_matched_any,
        coverage_any_pct: national_cov_any,
        weighted_confidence_any_avg: national_avg_conf_any,
        analyzed_departments: analyzed,
        invalid_manifest_rows,
        invalid_manifest_row_numbers: invalid_manifest_row_numbers.clone(),
        expected_departments,
        skipped_missing_matches,
        skipped_missing_parcels,
        missing_matches_departments: missing_matches_departments.clone(),
        missing_parcels_departments: missing_parcels_departments.clone(),
        by_region: reg_summaries,
        match_type_distribution: national_match_dist,
    };
    info!(
        analyzed_departments = analyzed,
        skipped_missing_matches, skipped_missing_parcels, "analysis completed"
    );
    if invalid_manifest_rows > 0 {
        warn!(
            invalid_manifest_rows,
            "analysis: manifest contains invalid rows (missing dept/region/nom)"
        );
    }
    let csv_out = output_dir.join("departments_summary.csv");
    std::fs::write(
        &csv_out,
        format!(
            "dept,nom,region,num_parcels,accepted_matched_parcels,accepted_coverage_pct,accepted_avg_confidence,best_match_coverage_pct,best_match_avg_confidence,coverage_delta_pct\n{}",
            summary_rows.join("\n")
        ),
    )?;

    let json_out = output_dir.join("national_summary.json");
    let f = File::create(json_out)?;
    serde_json::to_writer_pretty(f, &summary)?;

    // Markdown
    let md_out = output_dir.join("analysis_report.md");
    let mut md = String::new();

    md.push_str("# National BAN-Cadastre Alignment Report\n\n");
    md.push_str(&format!("Generated: {}\n\n", summary.generated_at));

    md.push_str("## Definitions\n\n");
    md.push_str(&format!(
        "- Accepted coverage (QA-aligned): best-per-parcel match is PreExisting, Inside, or distance_m <= {}.\n",
        summary.coverage_threshold_m
    ));
    md.push_str("- Best-match coverage: best-per-parcel match exists (any match type except None), without threshold.\n");
    md.push_str("- Coverage delta: Best-match coverage minus Accepted coverage (higher delta implies higher risk of low-quality matches).\n\n");

    let delta = summary.coverage_any_pct - summary.coverage_pct;
    md.push_str("## Executive Summary\n\n");
    md.push_str("| Metric | Value |\n|---|---:|\n");
    md.push_str(&format!("| Total parcels | {} |\n", summary.total_parcels));
    md.push_str(&format!(
        "| Accepted matched parcels | {} |\n",
        summary.total_matched_parcels
    ));
    md.push_str(&format!(
        "| Accepted coverage (%) | {:.2} |\n",
        summary.coverage_pct
    ));
    md.push_str(&format!(
        "| Mean confidence (accepted) | {:.2} |\n",
        summary.weighted_confidence_avg
    ));
    md.push_str(&format!(
        "| Best-match matched parcels | {} |\n",
        summary.total_matched_parcels_any
    ));
    md.push_str(&format!(
        "| Best-match coverage (%) | {:.2} |\n",
        summary.coverage_any_pct
    ));
    md.push_str(&format!(
        "| Mean confidence (best-match) | {:.2} |\n",
        summary.weighted_confidence_any_avg
    ));
    md.push_str(&format!("| Coverage delta (%) | {:.2} |\n", delta));

    md.push_str("\n## Input Completeness\n\n");
    md.push_str(&format!(
        "Manifest rows (total): {}\n\n",
        summary.manifest_rows_total
    ));
    md.push_str(&format!(
        "Expected departments (valid rows): {}\n\n",
        summary.expected_departments
    ));
    md.push_str(&format!(
        "Invalid manifest rows: {}\n\n",
        summary.invalid_manifest_rows
    ));
    if !summary.invalid_manifest_row_numbers.is_empty() {
        let mut v = summary.invalid_manifest_row_numbers.clone();
        v.sort_unstable();
        md.push_str(&format!(
            "Invalid manifest row numbers: {}\n\n",
            v.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    md.push_str(&format!(
        "Analyzed departments: {}\n\n",
        summary.analyzed_departments
    ));
    md.push_str(&format!(
        "Skipped (missing matches): {}\n\n",
        summary.skipped_missing_matches
    ));
    if !summary.missing_matches_departments.is_empty() {
        let mut v = summary.missing_matches_departments.clone();
        v.sort();
        md.push_str(&format!(
            "Missing matches departments: {}\n\n",
            v.join(", ")
        ));
    }
    md.push_str(&format!(
        "Skipped (missing parcels): {}\n\n",
        summary.skipped_missing_parcels
    ));
    if !summary.missing_parcels_departments.is_empty() {
        let mut v = summary.missing_parcels_departments.clone();
        v.sort();
        md.push_str(&format!(
            "Missing parcels departments: {}\n\n",
            v.join(", ")
        ));
    }

    md.push_str("## Match Type Distribution (best-per-parcel)\n\n");
    md.push_str("| Match type | Count |\n|---|---:|\n");
    let mut dist: Vec<_> = summary.match_type_distribution.iter().collect();
    dist.sort_by(|(ka, va), (kb, vb)| vb.cmp(va).then_with(|| ka.cmp(kb)));
    for (k, v) in dist {
        md.push_str(&format!("| {} | {} |\n", k, v));
    }

    md.push_str("\n## By Region\n\n| Region | Parcels | Accepted matched | Accepted % | Accepted conf | Best matched | Best % | Best conf |\n|---|---:|---:|---:|---:|---:|---:|---:|\n");
    let mut regions: Vec<_> = summary.by_region.iter().collect();
    regions.sort_by_key(|(k, _)| *k);
    for (r, s) in regions {
        md.push_str(&format!(
            "| {} | {} | {} | {:.2}% | {:.2} | {} | {:.2}% | {:.2} |\n",
            r,
            s.total_parcels,
            s.matched_parcels,
            s.coverage_pct,
            s.weighted_confidence_avg,
            s.matched_parcels_any,
            s.coverage_any_pct,
            s.weighted_confidence_any_avg
        ));
    }

    departement_details.sort_by(|a, b| b.1.total_cmp(&a.1));

    md.push_str(
        "\n## Top 10 Departments (accepted coverage)\n\n| Department | Accepted coverage |\n|---|---:|\n",
    );
    for (code, cov) in departement_details.iter().take(10) {
        md.push_str(&format!("| {} | {:.2}% |\n", code, cov));
    }

    let mut bottom = departement_details.clone();
    bottom.sort_by(|a, b| a.1.total_cmp(&b.1));
    md.push_str(
        "\n## Bottom 10 Departments (accepted coverage)\n\n| Department | Accepted coverage |\n|---|---:|\n",
    );
    for (code, cov) in bottom.iter().take(10) {
        md.push_str(&format!("| {} | {:.2}% |\n", code, cov));
    }

    md.push_str("\n## Artifacts\n\n");
    md.push_str(&format!(
        "- departments_summary.csv: {}\n",
        csv_out.display()
    ));
    md.push_str(&format!(
        "- national_summary.json: {}\n",
        output_dir.join("national_summary.json").display()
    ));
    md.push_str(&format!("- analysis_report.md: {}\n", md_out.display()));

    std::fs::write(md_out, md)?;
    if partial {
        warn!(
            skipped_missing_matches,
            skipped_missing_parcels, "analysis produced partial results"
        );
    }

    Ok(AnalyzeOutcome {
        output_dir,
        partial,
        expected_departments,
        invalid_manifest_rows,
        analyzed_departments: analyzed,
        skipped_missing_matches,
        skipped_missing_parcels,
    })
}
