pub mod aggregate;
pub mod download;
pub mod match_step;
pub mod prepare;
pub mod qa;
pub mod state;

use crate::cli::PipelineArgs;
use crate::pipeline::state::BatchState;
use crate::structures::MatchConfig;
use anyhow::{Context, Result};
use colored::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use tracing::{error, info, instrument, warn};

#[instrument(skip(args))]
pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    info!("Starting Pipeline...");

    // 1. Setup Dirs
    let data_dir = args.data_dir;
    let raw_dir = data_dir.join("raw");
    let staging_dir = data_dir.join("staging");
    let batch_results_dir = data_dir.join("batch_results");
    let final_output = data_dir.join("output");

    std::fs::create_dir_all(&raw_dir)?;
    std::fs::create_dir_all(&staging_dir)?;
    std::fs::create_dir_all(&batch_results_dir)?;
    std::fs::create_dir_all(&final_output)?;

    // 2. Load State
    let state_path = data_dir.join("batch_state.json");
    let mut state = if args.resume {
        BatchState::load(&state_path)?
    } else {
        BatchState::new()
    };
    state.save(&state_path)?;

    // 3. Load Departments
    let mut depts = Vec::new();
    if let Some(list) = &args.departments {
        for d in list.split(',') {
            if !d.trim().is_empty() {
                depts.push(d.trim().to_string());
            }
        }
    } else {
        let file = File::open(&args.departments_file).context("Failed to open departments file")?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();
            if !parts.is_empty() {
                let d = parts[0].trim();
                if !d.is_empty() && d != "code_insee" && d != "dept" {
                    depts.push(d.to_string());
                }
            }
        }
    }

    info!("Found {} departments to process.", depts.len());

    let match_config = MatchConfig {
        num_neighbors: 5,
        address_max_distance_m: 50.0,
    };

    // 4. Loop
    for dept in depts {
        if args.resume && state.is_completed(&dept) {
            info!("Skipping {} (completed)", dept);
            continue;
        }

        let dept_curr = dept.clone();
        let _span = tracing::span!(tracing::Level::INFO, "process_dept", dept = %dept_curr).entered();
        info!("=== Processing Department {} ===", dept);

        let start_dept = Instant::now();

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<qa::QaSummary> {
            // Step A: Download
            let t0 = Instant::now();
            download::step_download(&dept, &raw_dir, args.force)?;
            info!("✨ Download step completed in {:.1}s", t0.elapsed().as_secs_f32());

            // Step B: Prepare
            let t1 = Instant::now();
            let p_in = raw_dir.join(format!("cadastre-{}-parcelles.json", dept));
            let p_out = staging_dir.join(format!("parcelles_{}.parquet", dept));
            if args.force || !p_out.exists() {
                prepare::step_prepare_parcels(&p_in, &p_out)?;
            }
            let a_in = raw_dir.join(format!("adresses-{}.csv", dept));
            let a_out = staging_dir.join(format!("adresses_{}.parquet", dept));
            if args.force || !a_out.exists() {
                prepare::step_prepare_addresses(&a_in, &a_out)?;
            }
            info!("✨ Prepare step completed in {:.1}s", t1.elapsed().as_secs_f32());

            // Step C: Match
            let t2 = Instant::now();
            match_step::step_match(
                &dept,
                &staging_dir,
                &batch_results_dir,
                &match_config,
                args.quick_qa,
                args.filter_commune.as_ref(),
                args.limit_addresses,
            )?;
            info!("✨ Match step completed in {:.1}s", t2.elapsed().as_secs_f32());

            // Step D: QA
            let t3 = Instant::now();
            let summary = qa::step_qa(&dept, &staging_dir, &batch_results_dir, &final_output)?;
            info!("✨ QA step completed in {:.1}s", t3.elapsed().as_secs_f32());
            
            Ok(summary)
        }));

        match res {
            Ok(Ok(summary)) => {
                let duration = start_dept.elapsed();
                state.mark_completed(&dept);
                state.save(&state_path)?;

                // ASCII Card
                let cov_color = if summary.coverage_pct >= 95.0 {
                    "🟢 Excellent".green()
                } else if summary.coverage_pct >= 80.0 {
                    "🟡 Good".yellow()
                } else {
                    "🔴 Low".red()
                };

                // Distances fmt
                // We have (threshold, pct).
                // Let's compute approx buckets: <5m, 5-50m, >50m.
                // We have cumulative pct for 5.0 and 50.0.
                let get_pct = |t: f64| -> f64 {
                    summary.dist_tier_pcts.iter().find(|(x, _)| *x == t).map(|(_, p)| *p).unwrap_or(0.0)
                };
                let pct_5 = get_pct(5.0);
                let pct_50 = get_pct(50.0);
                // The tiers in qa.rs are cumulative coverage of TOTAL parcels?
                // qa.rs: matched_parcels / total_parcels * 100.
                // But we want distribution of MATCHES usually?
                // The request says: "80% <5m | 15% <50m | 5% >50m"
                // If coverage is 98%, then these add up to 100% of matches? Or 100% of total?
                // "Distances: 80% <5m" usually implies % of matches.
                // Let's assume % of Total for consistency with Coverage.
                // OR we can re-normalize if we want breakdown of matches.
                // Let's stick to simple cumulative or buckets.
                // The Example: 80% <5m.
                // If I have pct_5 = 80.0, pct_50 = 95.0.
                // Then <5m = 80%.
                // 5-50m = 95 - 80 = 15%.
                // >50m = 100 - 95? (If we assume all others are >50).
                // But matched_parcels might be 98%.
                // Let's just display what we have:
                // "Distances: {:.1}% <5m | {:.1}% <50m", pct_5, pct_50
                // Or "Distances: <5m: {:.1}% | <50m: {:.1}%"
                
                let dist_msg = format!("📏 Distances: <5m: {:.1}% | <50m: {:.1}%", pct_5, pct_50);

                let msg = format!(
                    "\n✅ [Dept {}] Processed in {:.1}s\n   ├─ 📊 Coverage: {:.1}% ({})\n   ├─ 🎯 Avg Conf: {:.1}\n   └─ {}",
                    dept,
                    duration.as_secs_f32(),
                    summary.coverage_pct,
                    cov_color,
                    summary.avg_confidence,
                    dist_msg
                );
                // Use eprintln to ensure it shows up even if stdout is piped (though info! does stderr usually)
                // The user asked for eprintln OR tracing::info!. 
                // Since we are using tracing, `info!` is better.
                info!("{}", msg);
            }
            Ok(Err(e)) => {
                error!("Error processing {}: {:?}", dept, e);
                state.mark_failed(&dept, e.to_string());
                state.save(&state_path)?;
            }
            Err(_) => {
                error!("Panic processing {}", dept);
                state.mark_failed(&dept, "Panic".to_string());
                state.save(&state_path)?;
            }
        }
    }

    // 5. Aggregate
    info!("=== Aggregating Results ===");
    aggregate::step_aggregate(&final_output)?;

    info!("Pipeline Completed.");
    Ok(())
}
