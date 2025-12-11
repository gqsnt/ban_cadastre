use geo::prelude::*;
use geo::{MultiPolygon, Point, Polygon};
use rstar::AABB;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

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
            ParcelGeometry::Polygon(poly) => Euclidean.distance(poly, p),
            ParcelGeometry::MultiPolygon(mpoly) => Euclidean.distance(mpoly, p),
        }
    }

    pub fn contains_point(&self, p: &Point<f64>) -> bool {
        match self {
            ParcelGeometry::Polygon(poly) => poly.contains(p),
            ParcelGeometry::MultiPolygon(mpoly) => mpoly.contains(p),
        }
    }

    pub fn envelope(&self) -> rstar::AABB<[f64; 2]> {
        use geo::BoundingRect;
        let rect = match self {
            ParcelGeometry::Polygon(poly) => poly.bounding_rect(),
            ParcelGeometry::MultiPolygon(mpoly) => mpoly.bounding_rect(),
        };

        if let Some(r) = rect {
            rstar::AABB::from_corners([r.min().x, r.min().y], [r.max().x, r.max().y])
        } else {
            rstar::AABB::from_point([0.0, 0.0])
        }
    }
}

/// Abstraction for accessing Parcels.
/// Allows future optimizations like memory mapping or compressed storage.
pub trait ParcelStore: Sync + Send {
    fn get_parcel(&self, idx: usize) -> &ParcelData;
    fn len(&self) -> usize;
    fn iter(&self) -> Box<dyn Iterator<Item = &ParcelData> + '_>;
}

// Basic implementation for Vec<ParcelData>
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

impl Display for MatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
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
        match_type: MatchType,
        distance_m: f32,
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
    pub num_neighbors: usize,
    pub address_max_distance_m: f64,
}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            num_neighbors: 5,
            address_max_distance_m: 50.0,
        }
    }
}

pub fn match_type_priority(mt: &MatchType) -> u8 {
    match mt {
        MatchType::PreExisting => 0,
        MatchType::Inside => 1,
        MatchType::BorderNear => 2,
        MatchType::FallbackNearest => 3,
        MatchType::None => 100,
    }
}
