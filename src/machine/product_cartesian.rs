
use crate::machine::Table;
use crate::machine::Column;
use crate::machine::Machine;
use crate::machine::get_columns;
use crate::machine::read_tuples;
use crate::machine::get_rowid_column_for_table;

use crate::storage::ResultSet;
use crate::storage::Tuple;

pub fn product_cartesian(machine: &mut Machine, tables: Vec<Table>) -> ResultSet {
    let mut result_set = ResultSet::new_empty();
    
    for (_dx, table) in tables.iter().enumerate() {
        let mut table_columns = get_columns(machine, &table);

        let mut has_primary_key: bool = false;

        for item in table_columns.iter() {
            if item.primary_key {
                has_primary_key = true;
            }
        }

        let mut columns: Vec<Column> = vec![];

        if has_primary_key == false {
            columns.push(get_rowid_column_for_table(table));
        }

        columns.append(&mut table_columns);

        let tuples: Vec<Tuple> = read_tuples(machine, &table);
        let result_set1 = ResultSet::new_select(columns, tuples);

        result_set = result_set.cartesian_product(&result_set1);
    }

    return result_set;
}
