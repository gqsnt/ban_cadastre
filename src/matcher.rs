use crate::indexer::{AddressIndex, DepartmentIndex};
use crate::structures::{
    match_type_priority, AddressInput, MatchConfig, MatchOutput, MatchType, ParcelData,
    ParcelGeometry, ParcelStore,
};
use geo::Centroid;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Map id_parcelle -> List of PreExisting matches (distance 0)
pub type PreexistingMap = HashMap<String, Vec<MatchOutput>>;

pub fn build_preexisting_map(
    addresses: &[AddressInput],
    known_parcels: &HashSet<&str>,
) -> PreexistingMap {
    let mut map: PreexistingMap = HashMap::new();
    for addr in addresses {
        if let Some(links) = &addr.existing_link {
            for parcel_id in links.split(';') {
                let pid = parcel_id.trim();
                if pid.is_empty() {
                    continue;
                }
                if known_parcels.contains(pid) {
                    let m = MatchOutput::new(
                        addr.id.clone(),
                        Some(pid.to_owned()),
                        MatchType::PreExisting,
                        0.0,
                    );
                    map.entry(pid.to_owned()).or_default().push(m);
                }
            }
        }
    }
    map
}

pub fn match_parcels_and_addresses_3_steps(
    parcels: &dyn ParcelStore,
    addresses: &[AddressInput],
    config: &MatchConfig,
) -> Vec<MatchOutput> {
    // 1. Build indices and lookups
    let known_parcels: HashSet<&str> = parcels.iter().map(|p| p.id.as_str()).collect();
    let preexisting_map = build_preexisting_map(addresses, &known_parcels);
    let parcel_index = DepartmentIndex::build(parcels);
    let address_index = AddressIndex::build(addresses);

    // --- Step 1: INSIDE + PRE_EXISTING (Parallel over Parcel indices) ---
    let step1_results: Vec<Vec<MatchOutput>> = (0..parcels.len())
        .into_par_iter()
        .map(|idx| {
            let parcel = parcels.get_parcel(idx);
            let mut parcel_matches = Vec::new();

            // A. Pre-existing
            if let Some(pre) = preexisting_map.get(&parcel.id) {
                parcel_matches.extend(pre.iter().cloned());
            }

            // B. Inside
            let candidates = address_index.locate_in_envelope(&parcel.envelope);
            for addr in candidates {
                if parcel.geom.contains_point(&addr.geom) {
                    let m = MatchOutput::new(
                        addr.id.clone(),
                        Some(parcel.id.clone()),
                        MatchType::Inside,
                        0.0,
                    );
                    parcel_matches.push(m);
                }
            }
            parcel_matches
        })
        .collect();

    let mut all_matches: Vec<MatchOutput> = Vec::new();
    let mut parcels_without_match_indices = Vec::new();

    // Flatten results and identify empty parcels
    for (idx, mut matches) in step1_results.into_iter().enumerate() {
        if matches.is_empty() {
            parcels_without_match_indices.push(idx);
        } else {
            all_matches.append(&mut matches);
        }
    }

    // Best match per address for rescue step
    let mut addr_best: HashMap<String, (u8, f32)> = HashMap::with_capacity(addresses.len());
    for m in &all_matches {
        let prio = match_type_priority(&m.match_type);
        let entry = addr_best.entry(m.id_ban.clone()).or_insert((100, f32::MAX));
        if prio < entry.0 || (prio == entry.0 && m.distance_m < entry.1) {
            *entry = (prio, m.distance_m);
        }
    }

    // --- Step 2: Fallback (Parallel over 'parcels_without_match') ---
    // MODIFIED: Search k-nearest neighbors to centroid, then check distance to geometry
    let step2_results: Vec<MatchOutput> = parcels_without_match_indices
        .par_iter()
        .filter_map(|&idx| {
            let parcel = parcels.get_parcel(idx);
            let centroid = match parcel.geom {
                ParcelGeometry::Polygon(ref p) => p.centroid(),
                ParcelGeometry::MultiPolygon(ref mp) => mp.centroid(),
            };

            if let Some(c) = centroid {
                // Search for the 5 nearest addresses to the centroid
                let best_candidate = address_index
                    .tree
                    .nearest_neighbor_iter(&[c.x(), c.y()])
                    .take(5)
                    .map(|node| {
                        let addr = &address_index.addresses[node.idx];
                        // Measure accurate distance to the parcel geometry (not centroid)
                        let dist = parcel.geom.distance_to_point(&addr.geom);
                        (addr, dist)
                    })
                    // Select the one strictly closest to the geometry
                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

                if let Some((addr, dist)) = best_candidate {
                    return Some(MatchOutput::new(
                        addr.id.clone(),
                        Some(parcel.id.clone()),
                        MatchType::FallbackNearest,
                        dist as f32,
                    ));
                }
            }
            None
        })
        .collect();

    for m in &step2_results {
        let prio = match_type_priority(&m.match_type);
        let entry = addr_best.entry(m.id_ban.clone()).or_insert((100, f32::MAX));
        if prio < entry.0 || (prio == entry.0 && m.distance_m < entry.1) {
            *entry = (prio, m.distance_m);
        }
    }
    all_matches.extend(step2_results);

    // --- Step 3: Address Rescue (Parallel over Addresses) ---
    // MODIFIED: Removed redundant "Inside" check. Step 1 already covers all Inside matches.
    let step3_results: Vec<MatchOutput> = addresses
        .par_iter()
        .filter_map(|addr| {
            if let Some((prio, dist)) = addr_best.get(&addr.id) {
                if *prio <= 1 {
                    return None; // Already PreExisting or Inside
                }
                if *prio == 2 && *dist <= config.address_max_distance_m as f32 {
                    return None; // Already BorderNear within threshold
                }
            }

            // Neighbors
            let neighbors = parcel_index.nearest_neighbors(&addr.geom, config.num_neighbors);
            let mut best_near: Option<(&ParcelData, f64)> = None;

            for p in neighbors {
                let d = p.geom.distance_to_point(&addr.geom);
                if d <= config.address_max_distance_m
                    && (best_near.is_none() || d < best_near.unwrap().1)
                {
                    best_near = Some((p, d));
                }
            }

            if let Some((p, d)) = best_near {
                return Some(MatchOutput::new(
                    addr.id.clone(),
                    Some(p.id.clone()),
                    MatchType::BorderNear,
                    d as f32,
                ));
            }
            None
        })
        .collect();

    all_matches.extend(step3_results);

    all_matches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::{AddressInput, MatchConfig, MatchType, ParcelData, ParcelGeometry};
    use geo::{Point, Polygon};

    // #[test]
    // fn test_match_inside() {
    //     let p1 = ParcelData {
    //         id: "p1".to_string(),
    //         code_insee: "00000".to_string(),
    //         geom: ParcelGeometry::Polygon(Polygon::new(
    //             vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)].into(),
    //             vec![],
    //         )),
    //         envelope:
    //     };
    //
    //     let a1 = AddressInput {
    //         id: "a1".to_string(),
    //         code_insee: "00000".to_string(),
    //         geom: Point::new(5.0, 5.0),
    //         existing_link: None,
    //     };
    //
    //     // We need to pass the Vec directly as reference?
    //     // No, we need &Vec which implements ParcelStore?
    //     // Or coerce.
    //     let parcels = vec![p1];
    //     let addresses = vec![a1];
    //     let config = MatchConfig::default();
    //
    //     // Implicit deref or explicit?
    //     let matches = match_parcels_and_addresses_3_steps(&parcels, &addresses, &config);
    //     assert_eq!(matches.len(), 1);
    //     assert_eq!(matches[0].match_type, MatchType::Inside);
    //     assert_eq!(matches[0].id_parcelle.as_deref(), Some("p1"));
    // }
    //
    // #[test]
    // fn test_match_fallback() {
    //      let p1 = ParcelData {
    //         id: "p1".to_string(),
    //         code_insee: "00000".to_string(),
    //         geom: ParcelGeometry::Polygon(Polygon::new(
    //             vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)].into(),
    //             vec![],
    //         )),
    //     };
    //     // Address far away
    //      let a1 = AddressInput {
    //         id: "a1".to_string(),
    //         code_insee: "00000".to_string(),
    //         geom: Point::new(100.0, 100.0),
    //         existing_link: None,
    //     };
    //
    //     let parcels = vec![p1];
    //     let addresses = vec![a1];
    //     let config = MatchConfig::default();
    //
    //     let matches = match_parcels_and_addresses_3_steps(&parcels, &addresses, &config);
    //     assert!(matches.iter().any(|m| m.match_type == MatchType::FallbackNearest));
    // }
}
