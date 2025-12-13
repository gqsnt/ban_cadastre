use crate::structures::{AddressInput, ParcelData, ParcelStore};
use rstar::{PointDistance, RTree, RTreeObject, AABB};

pub struct ParcelNode {
    pub idx: usize,
    pub envelope: AABB<[f64; 2]>,
}

impl RTreeObject for ParcelNode {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl PointDistance for ParcelNode {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        self.envelope.distance_2(point)
    }
}

pub struct DepartmentIndex<'a> {
    pub store: &'a dyn ParcelStore,
    pub tree: RTree<ParcelNode>,
}

impl<'a> DepartmentIndex<'a> {
    pub fn build(store: &'a dyn ParcelStore) -> Self {
        let mut nodes = Vec::with_capacity(store.len());
        for (idx, p) in store.iter().enumerate() {
            nodes.push(ParcelNode {
                idx,
                envelope: p.envelope,
            });
        }
        let tree = RTree::bulk_load(nodes);
        Self { store, tree }
    }

    pub fn get_parcel(&self, idx: usize) -> &'a ParcelData {
        self.store.get_parcel(idx)
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
        self.envelope.distance_2(point)
    }
}

pub struct AddressIndex<'a> {
    addresses: &'a [AddressInput],
    tree: RTree<AddressNode>,
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

    pub fn get(&self, idx: usize) -> &'a AddressInput {
        &self.addresses[idx]
    }

    pub fn locate_in_envelope<'s>(
        &'s self,
        envelope: &'s AABB<[f64; 2]>,
    ) -> impl Iterator<Item = &'a AddressInput> + 's {
        let addresses = self.addresses;
        self.tree
            .locate_in_envelope(envelope)
            .map(move |node| &addresses[node.idx])
    }

    pub fn locate_in_envelope_indices<'s>(
        &'s self,
        envelope: &'s AABB<[f64; 2]>,
    ) -> impl Iterator<Item = usize> + 's {
        self.tree.locate_in_envelope(envelope).map(|node| node.idx)
    }

}
