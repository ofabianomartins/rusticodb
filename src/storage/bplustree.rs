use crate::storage::Pager;
use crate::storage::Tuple;

pub struct BPlusTree {
    pub pager: Pager
}


pub fn bplustree_new(pager: Pager) -> BPlusTree {
    BPlusTree { pager }
}

pub fn bplustree_insert(tree: &BPlusTree, tuple: Tuple) {
}

pub fn bplustree_remove(tree: &BPlusTree, tuple: Tuple) {
}
