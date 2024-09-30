use crate::config::Config;
use crate::storage::machine::Machine;
use crate::storage::tuple::Tuple;
use crate::storage::cell::Cell;
use crate::storage::os_interface::OsInterface;

pub fn setup_system(machine: &mut Machine) {
    OsInterface::create_folder_if_not_exists(&Config::data_folder());

    let main_db_name = String::from("rusticodb");

    machine.create_database(&main_db_name);
    setup_databases_table(machine);
    setup_tables_table(machine);
    setup_columns_table(machine);
}

pub fn setup_databases_table(machine: &mut Machine) {
    let main_db_name = String::from("rusticodb");
    let table_name_databases = String::from("databases");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();
    let mut cell: Cell = Cell::new();
    
    cell.insert_string(&String::from("rusticodb"));

    tuple.append_cell(cell);

    tuples.push(tuple);

    machine.create_table(&main_db_name, &table_name_databases);
    machine.insert_tuples(&main_db_name, &table_name_databases, &mut tuples);
}

pub fn setup_tables_table(machine: &mut Machine) {
    let main_db_name = String::from("rusticodb");
    let table_name_tables = String::from("tables");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();

    let mut cell_rustico: Cell = Cell::new();
    cell_rustico.insert_string(&String::from("rusticodb"));

    let mut cell_database: Cell = Cell::new();
    cell_database.insert_string(&String::from("databases"));

    tuple.append_cell(cell_rustico);
    tuple.append_cell(cell_database);

    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();

    let mut cell_rustico: Cell = Cell::new();
    cell_rustico.insert_string(&String::from("rusticodb"));

    let mut cell_database: Cell = Cell::new();
    cell_database.insert_string(&String::from("tables"));

    tuple.append_cell(cell_rustico);
    tuple.append_cell(cell_database);

    tuples.push(tuple);

    let mut tuple: Tuple = Tuple::new();

    let mut cell_rustico: Cell = Cell::new();
    cell_rustico.insert_string(&String::from("rusticodb"));

    let mut cell_database: Cell = Cell::new();
    cell_database.insert_string(&String::from("colums"));

    tuple.append_cell(cell_rustico);
    tuple.append_cell(cell_database);

    tuples.push(tuple);

    machine.create_table(&main_db_name, &table_name_tables);
    machine.insert_tuples(&main_db_name, &table_name_tables, &mut tuples);
}

pub fn setup_columns_table(machine: &mut Machine) {
    let main_db_name = String::from("rusticodb");
    let table_name_tables = String::from("columns");

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut tuple: Tuple = Tuple::new();

    let mut cell_database: Cell = Cell::new();
    cell_database.insert_string(&String::from("rusticodb"));

    let mut cell_table: Cell = Cell::new();
    cell_table.insert_string(&String::from("databases"));

    let mut cell_column: Cell = Cell::new();
    cell_column.insert_string(&String::from("name"));

    let mut cell_column_type: Cell = Cell::new();
    cell_column_type.insert_string(&String::from("VARCHAR"));

    tuple.append_cell(cell_database);
    tuple.append_cell(cell_table);
    tuple.append_cell(cell_column);
    tuple.append_cell(cell_column_type);

    tuples.push(tuple);

    machine.create_table(&main_db_name, &table_name_tables);
    machine.insert_tuples(&main_db_name, &table_name_tables, &mut tuples);
}
