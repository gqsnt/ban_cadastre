use crate::cli::LinkArgs;
use crate::loader::{load_addresses, load_parcels};
use crate::matcher::match_parcels_and_addresses_3_steps;
use crate::structures::MatchConfig;
use crate::writer::MatchWriter;
use anyhow::Result;
use std::time::Instant;

use tracing::{info, warn};

fn log_crs_sanity_addresses(xs: &[f64], ys: &[f64]) {
    if xs.is_empty() || ys.is_empty() {
        return;
    }
    let max_abs_x = xs.iter().map(|v| v.abs()).fold(0.0, f64::max);
    let max_abs_y = ys.iter().map(|v| v.abs()).fold(0.0, f64::max);
    // Heuristic: lon/lat degrees are usually within ~[-180,180] and [-90,90].
    if max_abs_x <= 200.0 && max_abs_y <= 100.0 {
        warn!(
            max_abs_x,
            max_abs_y, "CRS sanity: coordinates look like degrees; expected EPSG:2154 meters"
        );
    }
}
pub fn run_link(args: LinkArgs) -> Result<()> {
    info!("starting link mode");
    info!(input_addresses=?args.input_adresses, "input addresses");
    info!(input_parcels=?args.input_parcelles, "input parcels");
    info!(output=?args.output, "output");

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let start_load = Instant::now();
    let mut parcels = load_parcels(&args.input_parcelles)?;
    let mut addresses = load_addresses(&args.input_adresses)?;

    info!(
        parcels = parcels.len(),
        addresses = addresses.len(),
        elapsed_ms = start_load.elapsed().as_millis(),
        "loaded inputs"
    );

    // CRS sanity (heuristic)
    {
        let mut ax = Vec::with_capacity(addresses.len());
        let mut ay = Vec::with_capacity(addresses.len());
        for a in &addresses {
            ax.push(a.geom.x());
            ay.push(a.geom.y());
        }
        log_crs_sanity_addresses(&ax, &ay);
    }

    if let Some(code) = &args.filter_commune {
        info!(commune=%code, "filtering by commune");
        parcels.retain(|p| p.code_insee == *code);
        addresses.retain(|a| a.code_insee == *code);
        info!(
            parcels = parcels.len(),
            addresses = addresses.len(),
            "after filter"
        );
    }

    if let Some(limit) = args.limit_addresses {
        if addresses.len() > limit {
            info!(limit, "limiting addresses (truncate)");
            addresses.truncate(limit);
        }
    }

    if parcels.is_empty() || addresses.is_empty() {
        warn!("input data is empty after loading/filtering; writing empty matches file");
        let writer = MatchWriter::new(&args.output, args.batch_size)?;
        writer.close()?;
        return Ok(());
    }

    let config = MatchConfig {
        address_max_distance_m: args.distance_threshold,

        // defaults for Step3
        fallback_max_distance_m: 1500.0,
        fallback_envelope_expand_m: 50.0,
    };

    info!(?config, "running matcher");
    let start_match = Instant::now();
    let matches = match_parcels_and_addresses_3_steps(&parcels, &addresses, &config);
    info!(
        elapsed_ms = start_match.elapsed().as_millis(),
        matches = matches.len(),
        "matching completed"
    );

    let mut counts = std::collections::HashMap::new();
    for m in &matches {
        *counts.entry(m.match_type.to_string()).or_insert(0) += 1;
    }
    info!("match stats:");
    for (k, v) in counts {
        info!(match_type=%k, count=v, "match_type count");
    }

    let start_write = Instant::now();
    let mut writer = MatchWriter::new(&args.output, args.batch_size)?;
    for m in matches {
        writer.write(m)?;
    }
    writer.close()?;
    info!(
        elapsed_ms = start_write.elapsed().as_millis(),
        "writing completed"
    );
    Ok(())
}
