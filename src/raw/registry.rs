use bumpalo::collections::Vec as BumpVec;
use bumpalo::Bump;

use crate::raw::build::BuilderNode;
use crate::raw::{CompiledAddr, FstOutput, NONE_ADDRESS};

#[derive(Debug)]
pub struct Registry<'bump, V: FstOutput = u64> {
    table: BumpVec<'bump, RegistryCell<'bump, V>>,
    table_size: usize, // number of rows
    mru_size: usize,   // number of columns
}

#[derive(Debug)]
struct RegistryCache<'bump, 'a, V: FstOutput = u64> {
    cells: &'a mut [RegistryCell<'bump, V>],
}

#[derive(Debug)]
pub struct RegistryCell<'bump, V: FstOutput = u64> {
    addr: CompiledAddr,
    node: BuilderNode<'bump, V>,
}

#[derive(Debug)]
pub enum RegistryEntry<'bump, 'a, V: FstOutput = u64> {
    Found(CompiledAddr),
    NotFound(&'a mut RegistryCell<'bump, V>),
    Rejected,
}

impl<'bump, V: FstOutput> Registry<'bump, V> {
    pub fn new(
        table_size: usize,
        mru_size: usize,
        bump: &'bump Bump,
    ) -> Registry<'bump, V> {
        let ncells = table_size.checked_mul(mru_size).unwrap();
        let mut table = BumpVec::with_capacity_in(ncells, bump);
        for _ in 0..ncells {
            table.push(RegistryCell::none(bump));
        }
        Registry { table, table_size, mru_size }
    }

    pub fn entry<'a>(
        &'a mut self,
        node: &BuilderNode<'_, V>,
    ) -> RegistryEntry<'bump, 'a, V> {
        if self.table.is_empty() {
            return RegistryEntry::Rejected;
        }
        let bucket = self.hash(node);
        let start = self.mru_size * bucket;
        let end = start + self.mru_size;
        RegistryCache { cells: &mut self.table[start..end] }.entry(node)
    }

    fn hash(&self, node: &BuilderNode<'_, V>) -> usize {
        // Basic FNV-1a hash as described:
        // https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
        //
        // In unscientific experiments, this provides the same compression
        // as `std::hash::SipHasher` but is much much faster.
        const FNV_PRIME: u64 = 1099511628211;
        let mut h = 14695981039346656037;
        h = (h ^ (node.is_final as u64)).wrapping_mul(FNV_PRIME);
        h = (h ^ node.final_output.value().to_u64()).wrapping_mul(FNV_PRIME);
        for t in &node.trans {
            h = (h ^ (t.inp as u64)).wrapping_mul(FNV_PRIME);
            h = (h ^ t.out.value().to_u64()).wrapping_mul(FNV_PRIME);
            h = (h ^ (t.addr as u64)).wrapping_mul(FNV_PRIME);
        }
        (h as usize) % self.table_size
    }
}

impl<'bump, 'a, V: FstOutput> RegistryCache<'bump, 'a, V> {
    fn entry(
        mut self,
        node: &BuilderNode<'_, V>,
    ) -> RegistryEntry<'bump, 'a, V> {
        if self.cells.len() == 1 {
            let cell = &mut self.cells[0];
            if !cell.is_none() && &cell.node == node {
                RegistryEntry::Found(cell.addr)
            } else {
                cell.node.clone_from(node);
                RegistryEntry::NotFound(cell)
            }
        } else if self.cells.len() == 2 {
            let cell1 = &mut self.cells[0];
            if !cell1.is_none() && &cell1.node == node {
                return RegistryEntry::Found(cell1.addr);
            }

            let cell2 = &mut self.cells[1];
            if !cell2.is_none() && &cell2.node == node {
                let addr = cell2.addr;
                self.cells.swap(0, 1);
                return RegistryEntry::Found(addr);
            }

            self.cells[1].node.clone_from(node);
            self.cells.swap(0, 1);
            RegistryEntry::NotFound(&mut self.cells[0])
        } else {
            let find =
                |c: &RegistryCell<'bump, V>| !c.is_none() && &c.node == node;
            if let Some(i) = self.cells.iter().position(find) {
                let addr = self.cells[i].addr;
                self.promote(i); // most recently used
                RegistryEntry::Found(addr)
            } else {
                let last = self.cells.len() - 1;
                self.cells[last].node.clone_from(node); // discard LRU
                self.promote(last);
                RegistryEntry::NotFound(&mut self.cells[0])
            }
        }
    }

    fn promote(&mut self, mut i: usize) {
        assert!(i < self.cells.len());
        while i > 0 {
            self.cells.swap(i - 1, i);
            i -= 1;
        }
    }
}

impl<'bump, V: FstOutput> RegistryCell<'bump, V> {
    fn none(bump: &'bump Bump) -> RegistryCell<'bump, V> {
        RegistryCell { addr: NONE_ADDRESS, node: BuilderNode::new(bump) }
    }

    fn is_none(&self) -> bool {
        self.addr == NONE_ADDRESS
    }

    pub fn insert(&mut self, addr: CompiledAddr) {
        self.addr = addr;
    }
}

#[cfg(test)]
mod tests {
    use bumpalo::Bump;

    use super::{Registry, RegistryCache, RegistryCell, RegistryEntry};
    use crate::raw::build::BuilderNode;
    use crate::raw::{Output, Transition};

    fn assert_rejected(entry: RegistryEntry<'_, '_>) {
        match entry {
            RegistryEntry::Rejected => {}
            entry => panic!("expected rejected entry, got: {:?}", entry),
        }
    }

    fn assert_not_found(entry: RegistryEntry<'_, '_>) {
        match entry {
            RegistryEntry::NotFound(_) => {}
            entry => panic!("expected nout found entry, got: {:?}", entry),
        }
    }

    fn assert_insert_and_found(
        reg: &mut Registry<'_>,
        bnode: &BuilderNode<'_>,
    ) {
        match reg.entry(&bnode) {
            RegistryEntry::NotFound(cell) => cell.insert(1234),
            entry => panic!("unexpected not found entry, got: {:?}", entry),
        }
        match reg.entry(&bnode) {
            RegistryEntry::Found(addr) => assert_eq!(addr, 1234),
            entry => panic!("unexpected found entry, got: {:?}", entry),
        }
    }

    fn bnode_new<'bump>(
        bump: &'bump Bump,
        is_final: bool,
        final_output: Output,
        trans: &[Transition],
    ) -> BuilderNode<'bump> {
        let mut node = BuilderNode::new(bump);
        node.is_final = is_final;
        node.final_output = final_output;
        node.trans.extend_from_slice(trans);
        node
    }

    #[test]
    fn empty_is_ok() {
        let bump = Bump::new();
        let mut reg = Registry::new(0, 0, &bump);
        let bnode = bnode_new(&bump, false, Output::zero(), &[]);
        assert_rejected(reg.entry(&bnode));
    }

    #[test]
    fn one_final_is_ok() {
        let bump = Bump::new();
        let mut reg = Registry::new(1, 1, &bump);
        let bnode = bnode_new(&bump, true, Output::zero(), &[]);
        assert_insert_and_found(&mut reg, &bnode);
    }

    #[test]
    fn one_with_trans_is_ok() {
        let bump = Bump::new();
        let mut reg = Registry::new(1, 1, &bump);
        let bnode = bnode_new(
            &bump,
            false,
            Output::zero(),
            &[Transition { addr: 0, inp: b'a', out: Output::zero() }],
        );
        assert_insert_and_found(&mut reg, &bnode);
        assert_not_found(reg.entry(&bnode_new(
            &bump,
            true,
            Output::zero(),
            &[Transition { addr: 0, inp: b'a', out: Output::zero() }],
        )));
        assert_not_found(reg.entry(&bnode_new(
            &bump,
            false,
            Output::zero(),
            &[Transition { addr: 0, inp: b'b', out: Output::zero() }],
        )));
        assert_not_found(reg.entry(&bnode_new(
            &bump,
            false,
            Output::zero(),
            &[Transition { addr: 0, inp: b'a', out: Output::new(1) }],
        )));
    }

    #[test]
    fn cache_works() {
        let bump = Bump::new();
        let mut reg = Registry::new(1, 1, &bump);

        let bnode1 = bnode_new(&bump, true, Output::zero(), &[]);
        assert_insert_and_found(&mut reg, &bnode1);

        let bnode2 = bnode_new(&bump, true, Output::new(1), &[]);
        assert_insert_and_found(&mut reg, &bnode2);
        assert_not_found(reg.entry(&bnode1));
    }

    #[test]
    fn promote() {
        let bump = Bump::new();
        let mut bnodes = vec![
            RegistryCell { addr: 1, node: bnode_new(&bump, false, Output::zero(), &[]) },
            RegistryCell { addr: 2, node: bnode_new(&bump, false, Output::zero(), &[]) },
            RegistryCell { addr: 3, node: bnode_new(&bump, false, Output::zero(), &[]) },
            RegistryCell { addr: 4, node: bnode_new(&bump, false, Output::zero(), &[]) },
        ];
        let mut cache = RegistryCache { cells: &mut bnodes };

        cache.promote(0);
        assert_eq!(cache.cells[0].addr, 1);
        assert_eq!(cache.cells[1].addr, 2);
        assert_eq!(cache.cells[2].addr, 3);
        assert_eq!(cache.cells[3].addr, 4);

        cache.promote(1);
        assert_eq!(cache.cells[0].addr, 2);
        assert_eq!(cache.cells[1].addr, 1);
        assert_eq!(cache.cells[2].addr, 3);
        assert_eq!(cache.cells[3].addr, 4);

        cache.promote(3);
        assert_eq!(cache.cells[0].addr, 4);
        assert_eq!(cache.cells[1].addr, 2);
        assert_eq!(cache.cells[2].addr, 1);
        assert_eq!(cache.cells[3].addr, 3);

        cache.promote(2);
        assert_eq!(cache.cells[0].addr, 1);
        assert_eq!(cache.cells[1].addr, 4);
        assert_eq!(cache.cells[2].addr, 2);
        assert_eq!(cache.cells[3].addr, 3);
    }
}
