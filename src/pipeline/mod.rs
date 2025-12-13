pub mod aggregate;
pub mod download;
pub mod match_step;
pub mod prepare;
pub mod qa;
pub mod state;
pub mod status;

use crate::cli::PipelineArgs;
use crate::pipeline::state::BatchState;
use crate::structures::MatchConfig;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;
use tracing::{error, info, instrument, warn};

pub struct PipelineOutcome {
    pub total_departments: usize,
    pub completed_departments: usize,
    pub failed_departments: usize,
    pub partial: bool,
    pub aggregate_partial: bool,
    pub state_path: PathBuf,
    pub output_dir: PathBuf,
}

#[instrument(skip(args))]
pub fn run_pipeline(args: PipelineArgs) -> Result<PipelineOutcome> {
    info!(
        data_dir=?args.data_dir,
        resume=args.resume,
        force=args.force,
        quick_qa=args.quick_qa,
        limit_addresses=?args.limit_addresses,
        filter_commune=?args.filter_commune,
        "starting pipeline"
    );
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

    let total = depts.len();
    info!(total_departments = total, "loaded departments");

    let match_config = MatchConfig::default();

    // 4. Loop
    for (idx, dept) in depts.into_iter().enumerate() {
        let dept_index = idx + 1;
        if args.resume && state.is_completed(&dept) {
            info!(dept=%dept, index=dept_index, total=total, "skipping department (completed)");
            continue;
        }

        let dept_curr = dept.clone();
        let _span =
            tracing::span!(tracing::Level::INFO, "process_dept", dept = %dept_curr).entered();
        info!(dept=%dept, index=dept_index, total=total, "processing department");

        let start_dept = Instant::now();

        let res =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<qa::QaSummary> {
                // Step A: Download
                let t0 = Instant::now();
                download::step_download(&dept, &raw_dir, args.force)?;
                info!(
                    dept=%dept,
                    step="download",
                    duration_s=t0.elapsed().as_secs_f32(),
                    "step completed"
                );

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
                info!(
                    dept=%dept,
                    step="prepare",
                    duration_s=t1.elapsed().as_secs_f32(),
                    "step completed"
                );

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
                info!(
                    dept=%dept,
                    step="match",
                    duration_s=t2.elapsed().as_secs_f32(),
                    "step completed"
                );

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

                let get_pct = |t: f64| -> f64 {
                    summary
                        .dist_tier_pcts
                        .iter()
                        .find(|(x, _)| *x == t)
                        .map(|(_, p)| *p)
                        .unwrap_or(0.0)
                };
                let pct_5 = get_pct(5.0);
                let pct_50 = get_pct(50.0);
                let coverage_band = if summary.coverage_pct >= 95.0 {
                    "excellent"
                } else if summary.coverage_pct >= 80.0 {
                    "good"
                } else {
                    "low"
                };

                info!(
                    dept=%dept,
                    status="ok",
                    index=dept_index,
                    total=total,
                    duration_s=duration.as_secs_f32(),
                    coverage_pct=summary.coverage_pct,
                    coverage_band=coverage_band,
                    avg_confidence=summary.avg_confidence,
                    coverage_lt_5m_pct=pct_5,
                    coverage_lt_50m_pct=pct_50,
                    "department processed"
                );
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
    info!(output_dir=?final_output, "aggregating results");
    let agg = aggregate::step_aggregate(&final_output)?;
    if agg.partial {
        warn!(
        missing_inputs=?agg.missing_inputs,
        "aggregate completed with missing inputs (partial artifacts)"
        );
    }

    let completed = state.completed.len();
    let failed = state.failed.len();
    let aggregate_partial = agg.partial;
    let partial = failed > 0 || aggregate_partial;
    if failed > 0 {
        warn!(
        completed_departments=completed,
        failed_departments=failed,
        state_path=?state_path,
        "pipeline completed with failed departments"
        );
        for f in &state.failed {
            warn!(dept=%f.dept, error=%f.error, "department failed");
        }
    } else {
        info!(
            completed_departments=completed,
            failed_departments=failed,
            state_path=?state_path,
            "pipeline completed"
        );
    }

    for p in &agg.generated {
        info!(artifact=?p, "artifact");
    }

    Ok(PipelineOutcome {
        total_departments: total,
        completed_departments: completed,
        failed_departments: failed,
        partial,
        aggregate_partial,
        state_path,
        output_dir: final_output,
    })
}
