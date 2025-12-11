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
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    println!("Starting Pipeline...");

    // 1. Setup Dirs
    let data_dir = args.data_dir;
    let raw_dir = data_dir.join("raw");
    let staging_dir = data_dir.join("staging");
    let _results_dir = data_dir.join("output"); // Using output as results dir based on prompt 2.3?
                                                // Prompt 2.3 says: data-dir containing raw/, staging/, batch_results/, output/
                                                // Let's stick to prompt.
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
    state.save(&state_path)?; // Initial save

    // 3. Load Departments
    let mut depts = Vec::new();
    if let Some(list) = &args.departments {
        for d in list.split(',') {
            if !d.trim().is_empty() {
                depts.push(d.trim().to_string());
            }
        }
    } else {
        // Load from file
        let file = File::open(&args.departments_file).context("Failed to open departments file")?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect(); // Assume CSV
            if !parts.is_empty() {
                let d = parts[0].trim();
                if !d.is_empty() && d != "code_insee" && d != "dept" {
                    // Skip header?
                    depts.push(d.to_string());
                }
            }
        }
    }

    println!("Found {} departments to process.", depts.len());

    let match_config = MatchConfig {
        num_neighbors: 5,
        address_max_distance_m: 50.0,
    };

    // 4. Loop
    for dept in depts {
        if args.resume && state.is_completed(&dept) {
            println!("Skipping {} (completed)", dept);
            continue;
        }

        println!("=== Processing Department {} ===", dept);

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            // Step A: Download
            download::step_download(&dept, &raw_dir, args.force)?;

            // Step B: Prepare
            // Parcels: raw/cadastre-*-parcelles.json -> staging/parcelles_*.parquet
            let p_in = raw_dir.join(format!("cadastre-{}-parcelles.json", dept));
            let p_out = staging_dir.join(format!("parcelles_{}.parquet", dept));
            if args.force || !p_out.exists() {
                prepare::step_prepare_parcels(&p_in, &p_out)?;
            }

            // Addresses: raw/adresses-*.csv -> staging/adresses_*.parquet
            let a_in = raw_dir.join(format!("adresses-{}.csv", dept));
            let a_out = staging_dir.join(format!("adresses_{}.parquet", dept));
            if args.force || !a_out.exists() {
                prepare::step_prepare_addresses(&a_in, &a_out)?;
            }

            // Step C: Match
            // staging -> batch_results/matches_*.parquet
            let _match_out = match_step::step_match(
                &dept,
                &staging_dir,
                &batch_results_dir,
                &match_config,
                args.quick_qa,
                args.filter_commune.as_ref(),
                args.limit_addresses,
            )?;

            // Step D: QA
            // batch_results -> final_output (CSVs)
            qa::step_qa(&dept, &staging_dir, &batch_results_dir, &final_output)?;

            Ok(())
        }));

        match res {
            Ok(Ok(())) => {
                println!("Department {} processed successfully.", dept);
                state.mark_completed(&dept);
                state.save(&state_path)?;
            }
            Ok(Err(e)) => {
                println!("Error processing {}: {:?}", dept, e);
                state.mark_failed(&dept, e.to_string());
                state.save(&state_path)?;
                // If not resume/force, maybe we want to stop?
                // Usually pipeline continues.
            }
            Err(_) => {
                println!("Panic processing {}", dept);
                state.mark_failed(&dept, "Panic".to_string());
                state.save(&state_path)?;
            }
        }
    }

    // 5. Aggregate
    println!("=== Aggregating Results ===");
    aggregate::step_aggregate(&final_output)?;

    println!("Pipeline Completed.");
    Ok(())
}
