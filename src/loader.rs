use crate::structures::{AddressInput, ParcelData, ParcelGeometry};
use anyhow::{anyhow, Context, Result};
use geo::Geometry;
use geozero::wkb::Wkb;
use geozero::ToGeo;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use std::fs::File;
use std::path::Path;

fn get_string_or_long(row: &Row, idx: usize) -> Option<String> {
    if let Ok(s) = row.get_string(idx) {
        Some(s.clone())
    } else if let Ok(v) = row.get_long(idx) {
        Some(v.to_string())
    } else {
        None
    }
}

pub fn load_parcels(path: &Path) -> Result<Vec<ParcelData>> {
    let file =
        File::open(path).with_context(|| format!("Failed to open parcel file: {:?}", path))?;
    let reader = SerializedFileReader::new(file).context("Failed to create parquet reader")?;

    let num_rows = reader.metadata().file_metadata().num_rows() as usize;
    let mut parcels = Vec::with_capacity(num_rows);

    for row in reader.get_row_iter(None)? {
        let row = row?;

        let id = get_string_or_long(&row, 0)
            .ok_or_else(|| anyhow!("Parcel id column is neither string nor long"))?;
        let code_insee = get_string_or_long(&row, 1)
            .ok_or_else(|| anyhow!("Parcel code_insee column is neither string nor long"))?;

        let wkb_data = row.get_bytes(2)?;
        let geom_geo = Wkb(wkb_data.data().to_vec())
            .to_geo()
            .map_err(|e| anyhow!("Failed to parse WKB for parcel {}: {}", id, e))?;

        let geom = match geom_geo {
            Geometry::Polygon(p) => ParcelGeometry::Polygon(p),
            Geometry::MultiPolygon(mp) => ParcelGeometry::MultiPolygon(mp),
            _ => continue,
        };

        // Critical: skip empty/invalid geometry (avoid AABB (0,0) pollution).
        let envelope = match geom.envelope_opt() {
            Some(e) => e,
            None => continue,
        };

        parcels.push(ParcelData {
            id,
            code_insee,
            geom,
            envelope,
        });
    }

    Ok(parcels)
}

pub fn load_addresses(path: &Path) -> Result<Vec<AddressInput>> {
    let file =
        File::open(path).with_context(|| format!("Failed to open address file: {:?}", path))?;
    let reader = SerializedFileReader::new(file).context("Failed to create parquet reader")?;

    let num_rows = reader.metadata().file_metadata().num_rows() as usize;
    let mut addresses = Vec::with_capacity(num_rows);

    for row in reader.get_row_iter(None)? {
        let row = row?;

        let id = get_string_or_long(&row, 0)
            .ok_or_else(|| anyhow!("Address id column is neither string nor long"))?;
        let code_insee = get_string_or_long(&row, 1)
            .ok_or_else(|| anyhow!("Address code_insee column is neither string nor long"))?;

        let wkb_data = row.get_bytes(2)?;
        let geom_geo = Wkb(wkb_data.data().to_vec())
            .to_geo()
            .map_err(|e| anyhow!("Failed to parse WKB for address {}: {}", id, e))?;
        let geom = match geom_geo {
            Geometry::Point(p) => p,
            _ => continue,
        };

        let existing_link_raw = get_string_or_long(&row, 3);
        let existing_link = existing_link_raw.and_then(|s| {
            let s_trim = s.trim().to_string();
            if s_trim.is_empty() || s_trim.eq_ignore_ascii_case("null") {
                None
            } else {
                Some(s_trim)
            }
        });

        addresses.push(AddressInput {
            id,
            code_insee,
            geom,
            existing_link,
        });
    }

    Ok(addresses)
}
