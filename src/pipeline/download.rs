use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use std::fs::File;
use std::io::{copy, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn step_download(dept: &str, raw_dir: &Path, force: bool) -> Result<(PathBuf, PathBuf)> {
    // Define Paths
    let adresses_gz = raw_dir.join(format!("adresses-{}.csv.gz", dept));
    let adresses_csv = raw_dir.join(format!("adresses-{}.csv", dept));

    let parcelles_gz = raw_dir.join(format!("cadastre-{}-parcelles.json.gz", dept));
    let parcelles_json = raw_dir.join(format!("cadastre-{}-parcelles.json", dept));

    if !raw_dir.exists() {
        std::fs::create_dir_all(raw_dir)?;
    }

    // URLs
    let url_ban = format!(
        "https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-{}.csv.gz",
        dept
    );
    let url_cadastre = format!("https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/{}/cadastre-{}-parcelles.json.gz", dept, dept);

    // Download & Gunzip Addresses
    if force || !adresses_csv.exists() {
        println!("Downloading Addresses for {}...", dept);
        download_file(&url_ban, &adresses_gz)?;
        println!("Decompressing Addresses...");
        gunzip_file(&adresses_gz, &adresses_csv)?;
    } else {
        println!("Addresses for {} already exist, skipping download.", dept);
    }

    // Download & Gunzip Parcels
    if force || !parcelles_json.exists() {
        println!("Downloading Parcels for {}...", dept);
        download_file(&url_cadastre, &parcelles_gz)?;
        println!("Decompressing Parcels...");
        gunzip_file(&parcelles_gz, &parcelles_json)?;
    } else {
        println!("Parcels for {} already exist, skipping download.", dept);
    }

    Ok((adresses_csv, parcelles_json))
}

fn download_file(url: &str, target: &Path) -> Result<()> {
    // Retry logic could be added here
    let client = Client::builder()
        .timeout(Duration::from_secs(300))
        .build()?;

    let mut response = client.get(url).send()?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Failed to download {}: Status {}",
            url,
            response.status()
        ));
    }

    let mut dest = File::create(target).context("Failed to create download file")?;
    copy(&mut response, &mut dest)?;
    Ok(())
}

fn gunzip_file(input: &Path, output: &Path) -> Result<()> {
    let input_file = File::open(input).context("Failed to open gz file")?;
    let mut decoder = GzDecoder::new(BufReader::new(input_file));
    let output_file =
        File::create(output).context("Failed to create output file for decompression")?;
    let mut encoder = BufWriter::new(output_file);

    copy(&mut decoder, &mut encoder)?;
    Ok(())
}
