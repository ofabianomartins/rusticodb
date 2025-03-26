
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;

use crate::storage::ResultSet;
use crate::storage::Tuple;

pub fn product_cartesian(machine: &mut Machine, tables: Vec<Table>) -> ResultSet {
    let mut result_set = ResultSet::new_empty();
    
    for (_dx, table) in tables.iter().enumerate() {
        let columns1 = get_columns(machine, &table);
        let tuples1: Vec<Tuple> = read_tuples(machine, &table);
        let result_set1 = ResultSet::new_select(columns1, tuples1);

        result_set = result_set.cartesian_product(&result_set1);
    }

    return result_set;
}
