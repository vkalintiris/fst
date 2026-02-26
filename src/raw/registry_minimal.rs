// This module is a drop-in but inefficient replacement of the LRU registry.
// In particular, this registry will never forget a node. In other words, if
// this registry is used during construction, then you're guaranteed a minimal
// FST.
//
// This is really only meant to be used for debugging and experiments. It is
// a memory/CPU hog.
//
// One "easy" improvement here is to use an FNV hash instead of the super
// expensive SipHasher.

#![allow(dead_code)]

use std::collections::hash_map::{Entry, HashMap};

use bumpalo::Bump;

use crate::raw::build::BuilderNode;
use crate::raw::CompiledAddr;

#[derive(Debug)]
pub struct Registry<'bump> {
    bump: &'bump Bump,
    table: HashMap<OwnedBuilderNode, RegistryCell>,
}

/// An owned version of BuilderNode for use as HashMap keys.
/// This uses std Vec since HashMap needs owned keys.
#[derive(Debug, Hash, Eq, PartialEq)]
struct OwnedBuilderNode {
    is_final: bool,
    final_output: crate::raw::Output,
    trans: Vec<crate::raw::Transition>,
}

impl OwnedBuilderNode {
    fn from_builder_node(node: &BuilderNode<'_>) -> Self {
        OwnedBuilderNode {
            is_final: node.is_final,
            final_output: node.final_output,
            trans: node.trans.to_vec(),
        }
    }
}

#[derive(Debug)]
pub enum RegistryEntry<'a> {
    Found(CompiledAddr),
    NotFound(&'a mut RegistryCell),
    Rejected,
}

#[derive(Clone, Copy, Debug)]
pub struct RegistryCell(CompiledAddr);

impl<'bump> Registry<'bump> {
    pub fn new(
        table_size: usize,
        _lru_size: usize,
        bump: &'bump Bump,
    ) -> Registry<'bump> {
        Registry {
            bump,
            table: HashMap::with_capacity(table_size),
        }
    }

    pub fn entry<'a>(
        &'a mut self,
        bnode: &BuilderNode<'_>,
    ) -> RegistryEntry<'a> {
        let key = OwnedBuilderNode::from_builder_node(bnode);
        match self.table.entry(key) {
            Entry::Occupied(v) => RegistryEntry::Found(v.get().0),
            Entry::Vacant(v) => {
                RegistryEntry::NotFound(v.insert(RegistryCell(0)))
            }
        }
    }
}

impl RegistryCell {
    pub fn insert(&mut self, addr: CompiledAddr) {
        self.0 = addr;
    }
}
