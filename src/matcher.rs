use crate::indexer::{AddressIndex, DepartmentIndex};
use crate::structures::{AddressInput, MatchConfig, MatchOutput, MatchType, ParcelData, ParcelStore};
use geo::{Point};
use rayon::prelude::*;
use rstar::{ PointDistance, AABB};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};


pub type PreexistingMap = HashMap<String, Vec<MatchOutput>>;

pub fn build_preexisting_map(
    addresses: &[AddressInput],
    known_parcels: &HashSet<&str>,
) -> PreexistingMap {
    let mut map: PreexistingMap = HashMap::new();

    for addr in addresses {
        if let Some(links) = &addr.existing_link {
            for parcel_id in links.split(|c| c == ';' || c == '|' || c == ',') {
                let pid = parcel_id.trim();
                if pid.is_empty() {
                    continue;
                }
                if known_parcels.contains(pid) {
                    let m = MatchOutput::new(
                        addr.id.clone(),
                        Some(pid.to_owned()),
                        0.0,
                        MatchType::PreExisting,
                    );
                    map.entry(pid.to_owned()).or_default().push(m);
                }
            }
        }
    }

    map
}

fn expand_aabb(env: &AABB<[f64; 2]>, margin: f64) -> AABB<[f64; 2]> {
    let lo = env.lower();
    let hi = env.upper();
    AABB::from_corners(
        [lo[0] - margin, lo[1] - margin],
        [hi[0] + margin, hi[1] + margin],
    )
}

const INSIDE_EPS_M: f64 = 0.01;

fn is_inside_or_on_border(parcel: &ParcelData, p: &Point<f64>) -> bool {
    // 0 quand inside OU sur la frontière
    let d = parcel.geom.distance_to_point(p);
    d.is_finite() && d <= INSIDE_EPS_M
}




pub fn match_parcels_and_addresses_3_steps(
    parcels: &dyn ParcelStore,
    addresses: &[AddressInput],
    config: &MatchConfig,
) -> Vec<MatchOutput> {
    let known_parcels: HashSet<&str> = parcels.iter().map(|p| p.id.as_str()).collect();
    let preexisting_map = build_preexisting_map(addresses, &known_parcels);

    let parcel_index = DepartmentIndex::build(parcels);
    let address_index = AddressIndex::build(addresses);

    // --- Step 1: INSIDE + PRE_EXISTING ---
    let step1_results: Vec<Vec<MatchOutput>> = (0..parcels.len())
        .into_par_iter()
        .map(|idx| {
            let parcel = parcels.get_parcel(idx);
            let mut out = Vec::new();
            let mut strict_addr_ids: HashSet<String> = HashSet::new();

            if let Some(pre) = preexisting_map.get(&parcel.id) {
                for m in pre {
                    strict_addr_ids.insert(m.id_ban.clone());
                    out.push(m.clone());
                }
            }

            for addr in address_index.locate_in_envelope(&parcel.envelope) {
                if strict_addr_ids.contains(&addr.id) {
                    continue;
                }
                if is_inside_or_on_border(parcel, &addr.geom) {
                    out.push(MatchOutput::new(
                        addr.id.clone(),
                        Some(parcel.id.clone()),
                        0.0,
                        MatchType::Inside,
                    ));
                }
            }

            out
        })
        .collect();

    let mut all_matches: Vec<MatchOutput> = Vec::new();
    all_matches.reserve(step1_results.iter().map(|v| v.len()).sum::<usize>());
    for mut v in step1_results {
        all_matches.append(&mut v);
    }

    // Track which parcels already have at least one match (any type) after step1/step2
    let mut parcel_has_match: Vec<bool> = vec![false; parcels.len()];
    // Map parcel_id -> idx for fast marking from Step2 results
    let mut parcel_idx_by_id: HashMap<String, usize> = HashMap::with_capacity(parcels.len());
    for (idx, p) in parcels.iter().enumerate() {
        parcel_idx_by_id.insert(p.id.clone(), idx);
    }
    // Mark parcels matched from Step1
    for m in &all_matches {
        if let Some(pid) = &m.id_parcelle {
            if let Some(&idx) = parcel_idx_by_id.get(pid) {
                parcel_has_match[idx] = true;
            }
        }
    }

    // STEP 2 (address-centric): BORDER_NEAR, 0 < d <= address_max_distance_m
    let step2_results: Vec<MatchOutput> = addresses
        .par_iter()
        .filter_map(|addr| {

            let mut best: Option<(&ParcelData, f64)> = None;
            let point_coords = [addr.geom.x(), addr.geom.y()];
            let thr = config.address_max_distance_m;
            let thr2 = thr * thr;

            for node in parcel_index.tree.nearest_neighbor_iter(&point_coords) {
                if node.distance_2(&point_coords) > thr2 {
                    break;
                }
                let p = parcel_index.get_parcel(node.idx);
                let d = p.geom.distance_to_point(&addr.geom);
                if !d.is_finite() {
                    continue;
                }
                // 0 < d <= 50  (exclude distance==0 which is "Inside")
                if d <= INSIDE_EPS_M || d > thr {
                    continue;
                }
                if best
                    .map(|(_, bd)| d.total_cmp(&bd) == Ordering::Less)
                    .unwrap_or(true)
                {
                    best = Some((p, d));
                }
            }

            let (p, d) = best?;
            Some(MatchOutput::new(
                addr.id.clone(),
                Some(p.id.clone()),
                d as f32,
                MatchType::BorderNear,
            ))
        })
        .collect();

    // Add step2 matches + mark parcels matched
    for m in &step2_results {
        if let Some(pid) = &m.id_parcelle {
            if let Some(&idx) = parcel_idx_by_id.get(pid) {
                parcel_has_match[idx] = true;
            }
        }
    }
    all_matches.extend(step2_results);

    // STEP 3 (parcel-centric): FALLBACK_NEAREST for parcels still without any match, d <= fallback_max_distance_m
    let parcels_without_match_indices: Vec<usize> = parcel_has_match
        .iter()
        .enumerate()
        .filter_map(|(idx, has)| if *has { None } else { Some(idx) })
        .collect();

    let step3_results: Vec<MatchOutput> = parcels_without_match_indices
        .par_iter()
        .filter_map(|&idx| {
            let parcel = parcels.get_parcel(idx);

            // Step 3 correct/robuste:
            // - on élargit progressivement l'AABB de la parcelle
            // - on évalue TOUTES les adresses dans la fenêtre (une fois)
            // - on s'arrête quand r >= best_dist (garantit le plus proche)

            let dmax = config.fallback_max_distance_m;
            let mut r = config.fallback_envelope_expand_m.max(5.0);

            let mut seen: HashSet<usize> = HashSet::with_capacity(256);
            let mut best_idx: Option<usize> = None;
            let mut best_dist: f64 = f64::INFINITY;
            let mut best_addr_id: Option<String> = None;

            while r <= dmax {
                let env = expand_aabb(&parcel.envelope, r);
                let mut any_new = false;

                for a_idx in address_index.locate_in_envelope_indices(&env) {

                    if !seen.insert(a_idx) {
                        continue;
                    }
                    any_new = true;

                    let addr = address_index.get(a_idx);
                    // Pruning (borne inférieure): distance(point, AABB(parcel)) <= distance(point, polygon)
                    // Si la borne inférieure ne peut pas battre best_dist, inutile de calculer la distance au polygone.
                    if best_idx.is_some() && best_dist.is_finite() {
                        let pxy = [addr.geom.x(), addr.geom.y()];
                        let lb2 = parcel.envelope.distance_2(&pxy);
                        let bd2 = best_dist * best_dist;
                        if lb2 >= bd2 {
                            continue;
                        }
                    }
                    let d = parcel.geom.distance_to_point(&addr.geom);
                    if !d.is_finite() || d > dmax {
                        continue;
                    }

                    let better = if best_idx.is_none() {
                        true
                    } else {
                        match d.total_cmp(&best_dist) {
                            Ordering::Less => true,
                            Ordering::Equal => {
                                // tie-break déterministe
                                match best_addr_id.as_ref() {
                                    Some(id) => addr.id < *id,
                                    None => true,
                                }
                            }
                            Ordering::Greater => false,
                        }
                    };

                    if better {
                        best_dist = d;
                        best_idx = Some(a_idx);
                        best_addr_id = Some(addr.id.clone());
                    }
                }

                // condition d'arrêt: si best_dist <= r, aucune adresse hors env(r) ne peut battre best_dist
                if best_idx.is_some() && best_dist <= r {
                    break;
                }

                if !any_new && r >= dmax {
                    break;
                }

                // croissance:
                // - par défaut: double
                // - si on a déjà un best_dist, faire un "closing pass" direct à r = best_dist
                //   (plus rapide que de continuer à doubler jusqu'à le dépasser).
                let mut next_r = (r * 2.0).min(dmax);
                if best_idx.is_some() && best_dist.is_finite() && best_dist > r {
                    next_r = best_dist.min(dmax);
                }
                if next_r <= r {
                    break;
                }
                r = next_r;
            }

            let a_idx = best_idx?;

            let addr = address_index.get(a_idx);
            // Si Step 3 découvre un point "Inside", on le sort comme Inside (au lieu de FallbackNearest).
            let (match_type, out_dist) = if best_dist <= INSIDE_EPS_M {
                (MatchType::Inside, 0.0_f32)
            } else {
                (MatchType::FallbackNearest, best_dist as f32)
            };


            Some(MatchOutput::new(
                addr.id.clone(),
                Some(parcel.id.clone()),
                out_dist,
                match_type,
            ))
        })
        .collect();

    all_matches.extend(step3_results);
    all_matches
}



