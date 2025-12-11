use crate::structures::{AddressInput, ParcelData, ParcelGeometry, ParcelStore};
use geo::Point;
use rstar::{Envelope, PointDistance, RTree, RTreeObject, AABB};

pub struct ParcelNode<'a> {
    pub idx: usize,
    pub geom: &'a ParcelGeometry,
    pub envelope: AABB<[f64; 2]>,
}

impl<'a> RTreeObject for ParcelNode<'a> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl<'a> PointDistance for ParcelNode<'a> {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let p = Point::new(point[0], point[1]);
        let d = self.geom.distance_to_point(&p);
        d * d
    }
}

pub struct DepartmentIndex<'a> {
    // We hold a reference to the store
    pub store: &'a dyn ParcelStore,
    pub tree: RTree<ParcelNode<'a>>,
}

impl<'a> DepartmentIndex<'a> {
    pub fn build(store: &'a dyn ParcelStore) -> Self {
        let mut nodes = Vec::with_capacity(store.len());
        for (idx, p) in store.iter().enumerate() {
            nodes.push(ParcelNode {
                idx,
                geom: &p.geom,
                envelope: p.envelope,
            });
        }
        let tree = RTree::bulk_load(nodes);
        Self { store, tree }
    }

    pub fn find_containing(&self, point: &Point<f64>) -> Vec<&'a ParcelData> {
        let point_coords = [point.x(), point.y()];
        self.tree
            .locate_all_at_point(&point_coords)
            .filter(|node| node.geom.contains_point(point))
            .map(|node| self.store.get_parcel(node.idx))
            .collect()
    }

    pub fn nearest_neighbors(&self, point: &Point<f64>, max_count: usize) -> Vec<&'a ParcelData> {
        let point_coords = [point.x(), point.y()];
        self.tree
            .nearest_neighbor_iter(&point_coords)
            .take(max_count)
            .map(|node| self.store.get_parcel(node.idx))
            .collect()
    }
}

pub struct AddressNode {
    pub idx: usize,
    pub envelope: AABB<[f64; 2]>,
}

impl RTreeObject for AddressNode {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl PointDistance for AddressNode {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let p = self.envelope.center();
        let dx = p[0] - point[0];
        let dy = p[1] - point[1];
        dx * dx + dy * dy
    }
}

pub struct AddressIndex<'a> {
    pub addresses: &'a [AddressInput],
    pub tree: RTree<AddressNode>,
}

impl<'a> AddressIndex<'a> {
    pub fn build(addresses: &'a [AddressInput]) -> Self {
        let mut nodes = Vec::with_capacity(addresses.len());
        for (idx, a) in addresses.iter().enumerate() {
            nodes.push(AddressNode {
                idx,
                envelope: AABB::from_point([a.geom.x(), a.geom.y()]),
            });
        }
        let tree = RTree::bulk_load(nodes);
        Self { addresses, tree }
    }

    pub fn locate_in_envelope(
        &self,
        envelope: &AABB<[f64; 2]>,
    ) -> impl Iterator<Item = &'a AddressInput> + use<'a, '_> {
        let addresses = self.addresses;
        self.tree
            .locate_in_envelope(envelope)
            .map(move |node| &addresses[node.idx])
    }

    pub fn nearest_neighbor(&self, point: &Point<f64>) -> Option<&'a AddressInput> {
        let point_coords = [point.x(), point.y()];
        self.tree
            .nearest_neighbor(&point_coords)
            .map(|node| &self.addresses[node.idx])
    }
}
