use crate::machine::Machine;
use crate::machine::Column;
use crate::machine::insert_tuples;
use crate::machine::create_file;
use crate::machine::get_databases_table_definition;
use crate::machine::get_tables_table_definition;
use crate::machine::get_columns_table_definition;
use crate::machine::get_indexes_table_definition;
use crate::machine::get_sequences_table_definition;

use crate::storage::Tuple;
use crate::storage::get_tuple_sequence;
use crate::storage::get_tuple_column;

use crate::config::SysDb;

use crate::utils::Logger;

pub fn setup_base(machine: &mut Machine) {
    Logger::info("setup sequences table");

    let mut tuples: Vec<Tuple> = Vec::new();
    tuples.push(get_tuple_sequence(1u64, &SysDb::dbname(), &SysDb::tblname_databases(), &String::from("id"), &String::from("rusticodb_databases_id"), 1u64));
    tuples.push(get_tuple_sequence(2u64, &SysDb::dbname(), &SysDb::tblname_tables()   , &String::from("id"), &String::from("rusticodb_tables_id"), 1u64));
    tuples.push(get_tuple_sequence(3u64, &SysDb::dbname(), &SysDb::tblname_columns()  , &String::from("id"), &String::from("rusticodb_columns_id"), 29u64));
    tuples.push(get_tuple_sequence(4u64, &SysDb::dbname(), &SysDb::tblname_sequences(), &String::from("id"), &String::from("rusticodb_sequences_id"), 6u64));
    tuples.push(get_tuple_sequence(5u64, &SysDb::dbname(), &SysDb::tblname_indexes()  , &String::from("id"), &String::from("rusticodb_indexes_id"), 1u64));

    create_file(machine, &SysDb::table_sequences());
    insert_tuples(machine, &SysDb::table_sequences(), &mut tuples);

    Logger::info("setup columns table");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut columns: Vec<Column> = Vec::new();

    columns.append(&mut get_databases_table_definition());
    columns.append(&mut get_tables_table_definition());
    columns.append(&mut get_columns_table_definition());
    columns.append(&mut get_indexes_table_definition());
    columns.append(&mut get_sequences_table_definition());

    for column in columns.iter() {
        tuples.push(
            get_tuple_column(
                column.id,
                &column.database_name.clone(),
                &column.table_name.clone(), 
                &column.name.clone(),
                &column.clone().get_type_column(),
                column.not_null,
                column.unique,
                column.primary_key,
                &column.default
            )
        );
    }

    create_file(machine, &SysDb::table_columns());
    let _ = insert_tuples(machine, &SysDb::table_columns(), &mut tuples);
}
