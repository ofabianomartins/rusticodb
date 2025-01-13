use crate::machine::Index;
use crate::machine::Machine;
use crate::machine::get_indexes;

pub fn check_index_exists(machine: &mut Machine, database_name: &String, name: &String) -> bool {
    let indexes: Vec<Index> = get_indexes(machine, &database_name)
        .into_iter()
        .filter(|tuple| tuple.name == *name)
        .collect();

    return indexes.len() > 0;
}
