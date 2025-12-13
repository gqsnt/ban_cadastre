use geo::prelude::*;
use geo::{MultiPolygon, Point, Polygon};
use rstar::AABB;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Data representation of a Cadastral Parcel.
#[derive(Debug, Clone)]
pub struct ParcelData {
    pub id: String,
    pub code_insee: String,
    pub geom: ParcelGeometry,
    /// Precomputed bounding box in the working CRS (EPSG:2154).
    pub envelope: AABB<[f64; 2]>,
}

#[derive(Debug, Clone)]
pub enum ParcelGeometry {
    Polygon(Polygon<f64>),
    MultiPolygon(MultiPolygon<f64>),
}

impl ParcelGeometry {
    pub fn distance_to_point(&self, p: &Point<f64>) -> f64 {
        match self {
            ParcelGeometry::Polygon(poly) => geo::Euclidean.distance(poly, p),
            ParcelGeometry::MultiPolygon(mpoly) => geo::Euclidean.distance(mpoly, p),
        }
    }


    /// Returns None if geometry has no bounding rect (empty/invalid).
    pub fn envelope_opt(&self) -> Option<AABB<[f64; 2]>> {
        use geo::BoundingRect;

        let rect = match self {
            ParcelGeometry::Polygon(poly) => poly.bounding_rect(),
            ParcelGeometry::MultiPolygon(mpoly) => mpoly.bounding_rect(),
        }?;

        Some(AABB::from_corners(
            [rect.min().x, rect.min().y],
            [rect.max().x, rect.max().y],
        ))
    }
}

pub trait ParcelStore: Sync + Send {
    fn get_parcel(&self, idx: usize) -> &ParcelData;
    fn len(&self) -> usize;
    fn iter(&self) -> Box<dyn Iterator<Item = &ParcelData> + '_>;
}

impl ParcelStore for Vec<ParcelData> {
    fn get_parcel(&self, idx: usize) -> &ParcelData {
        &self[idx]
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn iter(&self) -> Box<dyn Iterator<Item = &ParcelData> + '_> {
        Box::new(self.as_slice().iter())
    }
}

#[derive(Debug, Clone)]
pub struct AddressInput {
    pub id: String,
    pub code_insee: String,
    pub geom: Point<f64>,
    pub existing_link: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MatchType {
    PreExisting,
    Inside,
    BorderNear,
    FallbackNearest,
    None,
}

impl MatchType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            MatchType::PreExisting => "PreExisting",
            MatchType::Inside => "Inside",
            MatchType::BorderNear => "BorderNear",
            MatchType::FallbackNearest => "FallbackNearest",
            MatchType::None => "None",
        }
    }
}

impl fmt::Display for MatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchOutput {
    pub id_ban: String,
    pub id_parcelle: Option<String>,
    pub match_type: MatchType,
    pub distance_m: f32,
    pub confidence: u32,
}

impl MatchOutput {
    pub fn new(
        id_ban: String,
        id_parcelle: Option<String>,
        distance_m: f32,
        match_type: MatchType,
    ) -> Self {
        let confidence = match match_type {
            MatchType::PreExisting => 100,
            MatchType::Inside => 90,
            MatchType::BorderNear => {
                if distance_m < 5.0 {
                    80
                } else {
                    70
                }
            }
            MatchType::FallbackNearest => 50,
            MatchType::None => 0,
        };

        Self {
            id_ban,
            id_parcelle,
            match_type,
            distance_m,
            confidence,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MatchConfig {
    pub address_max_distance_m: f64,
    // Step3 tuning
    pub fallback_max_distance_m: f64,
    pub fallback_envelope_expand_m: f64,

}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            address_max_distance_m: 50.0,
            fallback_max_distance_m: 1500.0,
            fallback_envelope_expand_m: 50.0,
        }
    }
}
