use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::get_tables;

pub fn check_table_exists(machine: &mut Machine, table: &Table) -> bool {
    let tables: Vec<Table> = get_tables(machine, &table.database_name)
        .into_iter()
        .filter(|tuple| tuple.name == table.name)
        .collect();

    return tables.len() > 0;
}
