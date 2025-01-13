use crate::machine::Sequence;
use crate::machine::Machine;
use crate::machine::get_sequences;

pub fn check_sequence_exists(machine: &mut Machine, database_name: &String, name: &String) -> bool {
    let sequences: Vec<Sequence> = get_sequences(machine, &database_name)
        .into_iter()
        .filter(|tuple| tuple.name == *name)
        .collect();

    return sequences.len() > 0;
}
