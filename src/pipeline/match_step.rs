use crate::loader::{load_addresses, load_parcels};
use crate::matcher::match_parcels_and_addresses_3_steps;
use crate::structures::MatchConfig;
use crate::writer::MatchWriter;
use anyhow::Result;
use std::path::{Path, PathBuf};

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
        println!(
            "Matches for {} already exist and quick-qa is on. Skipping.",
            dept
        );
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

    println!("Loading data for {}...", dept);
    let mut parcels = load_parcels(&parcels_path)?;
    let mut addresses = load_addresses(&addresses_path)?;

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
        println!("Warning: No data for {}. Writing empty matches file.", dept);
        if let Some(p) = output_path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let writer = MatchWriter::new(&output_path, 10000)?;
        writer.close()?;
        return Ok(output_path);
    }

    println!("Matching {}...", dept);
    let matches = match_parcels_and_addresses_3_steps(&parcels, &addresses, config);
    println!("Matches found: {}", matches.len());

    if let Some(p) = output_path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let mut writer = MatchWriter::new(&output_path, 10000)?;
    for m in matches {
        writer.write(m)?;
    }
    writer.close()?;

    Ok(output_path)
}
