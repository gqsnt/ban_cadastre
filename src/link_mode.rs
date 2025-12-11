use crate::cli::LinkArgs;
use crate::loader::{load_addresses, load_parcels};
use crate::matcher::match_parcels_and_addresses_3_steps;
use crate::structures::MatchConfig;
use crate::writer::MatchWriter;
use anyhow::Result;
use std::time::Instant;

pub fn run_link(args: LinkArgs) -> Result<()> {
    println!("Starting link mode...");
    println!("Input Addresses: {:?}", args.input_adresses);
    println!("Input Parcels: {:?}", args.input_parcelles);
    println!("Output: {:?}", args.output);

    let start_load = Instant::now();

    // 1. Load Data
    let mut parcels = load_parcels(&args.input_parcelles)?;
    let mut addresses = load_addresses(&args.input_adresses)?;

    println!(
        "Loaded {} parcels and {} addresses in {:?}",
        parcels.len(),
        addresses.len(),
        start_load.elapsed()
    );

    // 2. Filter (Optional CLI options)
    if let Some(code) = &args.filter_commune {
        println!("Filtering for commune: {}", code);
        parcels.retain(|p| p.code_insee == *code);
        addresses.retain(|a| a.code_insee == *code);
        println!(
            "Filtered: {} parcels, {} addresses",
            parcels.len(),
            addresses.len()
        );
    }

    if let Some(limit) = args.limit_addresses {
        if addresses.len() > limit {
            println!("Limiting addresses to {} (first ones)", limit);
            addresses.truncate(limit);
        }
    }

    if parcels.is_empty() || addresses.is_empty() {
        println!("Warning: valid input data is empty after loading/filtering. writing empty file.");
        let writer = MatchWriter::new(&args.output, args.batch_size)?;
        writer.close()?;
        return Ok(());
    }

    // 3. Match
    let config = MatchConfig {
        num_neighbors: args.num_neighbors,
        address_max_distance_m: args.distance_threshold, // Using the same threshold for rescue max distance
    };

    println!("Running matcher with config: {:?}", config);
    let start_match = Instant::now();
    let matches = match_parcels_and_addresses_3_steps(&parcels, &addresses, &config);
    println!(
        "Matching completed in {:?}. Generated {} matches.",
        start_match.elapsed(),
        matches.len()
    );

    // 4. Statistics
    let mut counts = std::collections::HashMap::new();
    for m in &matches {
        *counts.entry(m.match_type.to_string()).or_insert(0) += 1;
    }
    println!("Match Stats:");
    for (k, v) in counts {
        println!("  {}: {}", k, v);
    }

    // 5. Write Output
    let start_write = Instant::now();
    let mut writer = MatchWriter::new(&args.output, args.batch_size)?;
    for m in matches {
        writer.write(m)?;
    }
    writer.close()?;
    println!("Writing completed in {:?}", start_write.elapsed());

    Ok(())
}
