
use crate::storage::cell::Cell;


#[derive(Debug)]
pub struct Tuple {
    pub size: u64,
    pub cells: Vec<Cell>
}
