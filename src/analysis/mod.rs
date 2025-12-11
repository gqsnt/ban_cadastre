use crate::cli::AnalyzeArgs;
use anyhow::Result;
use polars::lazy::dsl::{col, lit, when};
use polars::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;

#[derive(Serialize)]
struct NationalSummary {
    total_parcels: i64,
    total_matched_parcels: i64,
    coverage_pct: f64,
    weighted_confidence_avg: f64,
    total_departments: usize,
    by_region: HashMap<String, RegionalStats>,
    match_type_distribution: HashMap<String, i64>,
}

#[derive(Serialize)]
struct RegionalStats {
    total_parcels: i64,
    matched_parcels: i64,
    coverage_pct: f64,
    weighted_confidence_avg: f64,
    match_type_distribution: HashMap<String, i64>,
}

pub fn run_analyze(args: AnalyzeArgs) -> Result<()> {
    println!("Starting Analysis...");

    let data_root = args.results_dir.clone();
    let matches_dir = data_root.join("batch_results");
    let staging_dir = data_root.join("staging");
    let qa_dir = data_root.join("output");

    let output_dir = args.output_dir.unwrap_or_else(|| data_root.clone());
    std::fs::create_dir_all(&output_dir)?;

    let depts_df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(args.departments_file))?
        .finish()?;
    let rows = depts_df.height();
    println!("Loaded metadata for {} departments", rows);

    let mut summary_rows = Vec::new();

    let mut national_total_p = 0i64;
    let mut national_matched_p = 0i64;
    let mut national_confidence_sum = 0.0;
    let mut national_match_dist: HashMap<String, i64> = HashMap::new();
    let mut region_agg: HashMap<String, (i64, i64, f64, HashMap<String, i64>)> = HashMap::new();

    let dept_col = depts_df.column("dept")?.str()?;
    let region_col = depts_df.column("region")?.str()?;
    let nom_col = depts_df.column("nom")?.str()?;
    let mut departement_details: Vec<(String, f64)> = Vec::new();

    for i in 0..rows {
        let dept_code = dept_col.get(i).unwrap();
        let region = region_col.get(i).unwrap();
        let nom = nom_col.get(i).unwrap();

        let matches_file = matches_dir.join(format!("matches_{}.parquet", dept_code));
        let staging_parcels = staging_dir.join(format!("parcelles_{}.parquet", dept_code));

        if !matches_file.exists() {
            continue;
        }

        // 1. Get total parcels count (aggregation only, pas de collect complet)
        let num_parcels: i64;
        if staging_parcels.exists() {
            let lf = LazyFrame::scan_parquet(
                PlPath::from_str(staging_parcels.to_string_lossy().as_ref()),
                ScanArgsParquet::default(),
            )?;
            let df = lf
                .select([col("id").count().alias("n_parcels")])
                .collect()?;
            let n = df.column("n_parcels")?.u32()?.get(0).unwrap_or(0);
            num_parcels = n as i64;
        } else {
            let qa_csv = qa_dir.join(format!("qa_distance_tiers_{}.csv", dept_code));
            if qa_csv.exists() {
                let df = CsvReadOptions::default()
                    .with_has_header(true)
                    .try_into_reader_with_file_path(Some(qa_csv.clone()))?
                    .finish()?;
                num_parcels = df
                    .column("total_parcels")?
                    .cast(&DataType::Int64)?
                    .i64()?
                    .get(0)
                    .unwrap();
            } else {
                println!("Missing parcelles data for {}, skipping.", dept_code);
                continue;
            }
        }

        // 2. Analyze Matches (colonnes prunées)
        let matches_lf = LazyFrame::scan_parquet(
            PlPath::from_str(matches_file.to_str().unwrap()),
            ScanArgsParquet::default(),
        )?;

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

        let best_matches_df = matches_lf
            .filter(col("id_parcelle").is_not_null())
            .select([
                col("id_parcelle"),
                col("match_type"),
                col("distance_m"),
                col("confidence"),
            ])
            .with_column(priority_expr.alias("prio"))
            .sort(
                ["id_parcelle", "prio", "distance_m"],
                SortMultipleOptions::default(),
            )
            .unique(
                Some(col("id_parcelle").into_selector().unwrap()),
                UniqueKeepStrategy::First,
            )
            .collect()?;

        // Distribution par type de match
        let mut dept_dist: HashMap<String, i64> = HashMap::new();

        // On recompte explicitement le nombre de lignes par match_type
        let match_counts = best_matches_df
            .clone()
            .lazy()
            .group_by([col("match_type")])
            .agg([len().alias("n")])
            .collect()?;

        let mc_type = match_counts.column("match_type")?.str()?;
        let mc_cnt = match_counts.column("n")?.u32()?;

        for j in 0..mc_type.len() {
            let t = mc_type.get(j).unwrap();
            let c = mc_cnt.get(j).unwrap() as i64;
            if t != "None" {
                dept_dist.insert(t.to_string(), c);
            }
        }

        let matched_parcels: i64 = dept_dist.values().sum();

        let conf_df = best_matches_df
            .lazy()
            .filter(col("match_type").neq(lit("None")))
            .collect()?;

        let avg_conf = if conf_df.height() > 0 {
            conf_df.column("confidence")?.u32()?.mean().unwrap_or(0.0)
        } else {
            0.0
        };

        let coverage = if num_parcels > 0 {
            matched_parcels as f64 / num_parcels as f64 * 100.0
        } else {
            0.0
        };

        departement_details.push((dept_code.to_string(), coverage));
        summary_rows.push(format!(
            "{},{},{},{},{},{:.2},{:.2}",
            dept_code, nom, region, num_parcels, matched_parcels, coverage, avg_conf
        ));

        national_total_p += num_parcels;
        national_matched_p += matched_parcels;
        national_confidence_sum += avg_conf * (matched_parcels as f64);
        for (k, v) in &dept_dist {
            *national_match_dist.entry(k.clone()).or_insert(0) += v;
        }

        let r_entry = region_agg
            .entry(region.to_string())
            .or_insert((0, 0, 0.0, HashMap::new()));
        r_entry.0 += num_parcels;
        r_entry.1 += matched_parcels;
        r_entry.2 += avg_conf * (matched_parcels as f64);
        for (k, v) in dept_dist {
            *r_entry.3.entry(k).or_insert(0) += v;
        }

        println!(
            "Analyzed {}: {:.1}% (Avg Conf: {:.1})",
            dept_code, coverage, avg_conf
        );
    }

    let national_cov = if national_total_p > 0 {
        national_matched_p as f64 / national_total_p as f64 * 100.0
    } else {
        0.0
    };
    let national_avg_conf = if national_matched_p > 0 {
        national_confidence_sum / national_matched_p as f64
    } else {
        0.0
    };

    let mut reg_summaries = HashMap::new();
    for (name, (tot, mat, conf_sum, dist)) in region_agg {
        let cov = if tot > 0 {
            mat as f64 / tot as f64 * 100.0
        } else {
            0.0
        };
        let avg = if mat > 0 { conf_sum / mat as f64 } else { 0.0 };
        reg_summaries.insert(
            name,
            RegionalStats {
                total_parcels: tot,
                matched_parcels: mat,
                coverage_pct: cov,
                weighted_confidence_avg: avg,
                match_type_distribution: dist,
            },
        );
    }

    let summary = NationalSummary {
        total_parcels: national_total_p,
        total_matched_parcels: national_matched_p,
        coverage_pct: national_cov,
        weighted_confidence_avg: national_avg_conf,
        total_departments: summary_rows.len(),
        by_region: reg_summaries,
        match_type_distribution: national_match_dist,
    };

    let csv_out = output_dir.join("departments_summary.csv");
    std::fs::write(
        &csv_out,
        format!(
            "dept,nom,region,num_parcels,matched_parcels,coverage_pct,avg_confidence\n{}",
            summary_rows.join("\n")
        ),
    )?;

    let json_out = output_dir.join("national_summary.json");
    let f = File::create(json_out)?;
    serde_json::to_writer_pretty(f, &summary)?;

    let md_out = output_dir.join("analysis_report.md");
    let mut md = String::new();
    md.push_str("# Rapport d'Analyse National\n\n");
    md.push_str(&format!("- **Total Parcelles**: {}\n", national_total_p));
    md.push_str(&format!("- **Total Matchées**: {}\n", national_matched_p));
    md.push_str(&format!(
        "- **Couverture Nationale**: {:.2}%\n",
        national_cov
    ));
    md.push_str(&format!(
        "- **Confiance Moyenne**: {:.2}\n",
        national_avg_conf
    ));
    md.push_str(&format!(
        "- **Départements Traités**: {}\n\n",
        summary_rows.len()
    ));
    md.push_str("### Distribution des types de match (National)\n");
    for (k, v) in &summary.match_type_distribution {
        md.push_str(&format!("- **{}**: {}\n", k, v));
    }
    md.push_str(
        "\n## Par Région\n\n| Région | Parcelles | Matchées | % | Confiance |\n|---|---|---|---|---|\n",
    );
    for (r, s) in &summary.by_region {
        md.push_str(&format!(
            "| {} | {} | {} | {:.2}% | {:.2} |\n",
            r, s.total_parcels, s.matched_parcels, s.coverage_pct, s.weighted_confidence_avg
        ));
    }

    departement_details.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    md.push_str(
        "\n## Top 10 Départements par Couverture\n\n| Département | Couverture |\n|---|---|\n",
    );
    for (code, cov) in departement_details.iter().take(10) {
        md.push_str(&format!("| {} | {:.2}% |\n", code, cov));
    }

    md.push_str("\n## Départements < 85% Couverture\n\n| Département | Couverture |\n|---|---|\n");
    for (code, cov) in departement_details.iter().rev() {
        if *cov >= 85.0 {
            break;
        }
        md.push_str(&format!("| {} | {:.2}% |\n", code, cov));
    }
    std::fs::write(md_out, md)?;
    Ok(())
}
