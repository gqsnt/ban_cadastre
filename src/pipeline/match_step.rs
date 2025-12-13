use crate::loader::{load_addresses, load_parcels};
use crate::matcher::match_parcels_and_addresses_3_steps;
use crate::structures::MatchConfig;
use crate::writer::MatchWriter;
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{info, warn};

pub fn step_match(
    dept: &str,
    staging_dir: &Path,
    results_dir: &Path,
    config: &MatchConfig,
    quick_qa: bool,
    filter_commune: Option<&String>,
    limit_addresses: Option<usize>,
) -> Result<PathBuf> {
    let output_path = results_dir.join(format!("matches_{}.parquet", dept));

    if quick_qa && output_path.exists() {
        info!(dept=%dept, output_path=?output_path, "matches already exist; skipping (quick_qa=true)");
        return Ok(output_path);
    }

    let parcels_path = staging_dir.join(format!("parcelles_{}.parquet", dept));
    let addresses_path = staging_dir.join(format!("adresses_{}.parquet", dept));

    // Check files exist
    if !parcels_path.exists() || !addresses_path.exists() {
        return Err(anyhow::anyhow!(
            "Input parquet files not found in staging for {}",
            dept
        ));
    }

    let t_load = Instant::now();
    let mut parcels = load_parcels(&parcels_path)?;
    let mut addresses = load_addresses(&addresses_path)?;
    info!(
        dept=%dept,
        parcels=parcels.len(),
        addresses=addresses.len(),
        duration_s=t_load.elapsed().as_secs_f32(),
        "loaded match inputs"
    );
    // L
    // Limit/Filter
    if let Some(c) = filter_commune {
        parcels.retain(|p| p.code_insee == *c);
        addresses.retain(|a| a.code_insee == *c);
    }
    if let Some(l) = limit_addresses {
        if addresses.len() > l {
            addresses.truncate(l);
        }
    }

    if parcels.is_empty() || addresses.is_empty() {
        warn!(dept=%dept, "empty inputs after filtering/limiting; writing empty matches file");
        if let Some(p) = output_path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let writer = MatchWriter::new(&output_path, 10000)?;
        writer.close()?;
        return Ok(output_path);
    }

    let t_match = Instant::now();
    info!(
        dept=%dept,
        parcels=parcels.len(),
        addresses=addresses.len(),
        address_max_distance_m=config.address_max_distance_m,
        fallback_max_distance_m=config.fallback_max_distance_m,
        "matching started"
    );
    let matches = match_parcels_and_addresses_3_steps(&parcels, &addresses, config);
    info!(
        dept=%dept,
        matches=matches.len(),
        duration_s=t_match.elapsed().as_secs_f32(),
        "matching completed"
    );

    if let Some(p) = output_path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let t_write = Instant::now();
    let mut writer = MatchWriter::new(&output_path, 10000)?;
    for m in matches {
        writer.write(m)?;
    }
    writer.close()?;
    info!(
        dept=%dept,
        output_path=?output_path,
        duration_s=t_write.elapsed().as_secs_f32(),
        "matches written"
    );

    Ok(output_path)
}
