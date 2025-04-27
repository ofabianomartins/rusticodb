use rusticodb::setup::setup_system;

use rusticodb::machine::Machine;
use rusticodb::machine::create_database;
use rusticodb::machine::create_table;
use rusticodb::machine::insert_row;
use rusticodb::machine::Table;
use rusticodb::machine::Column;
use rusticodb::machine::ColumnType;
use rusticodb::machine::get_columns;
use rusticodb::machine::product_cartesian;

use rusticodb::storage::Pager;
use rusticodb::storage::Data;
use rusticodb::storage::Tuple;

use crate::test_utils::create_tmp_test_folder;

#[test]
pub fn test_in_one_column_without_primary_key() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, "database1".to_string(), false);

    let _ = machine.set_actual_database("USE database1".to_string());

    let table = Table::new("database1".to_string(), "table1".to_string());
    let columns = vec![
        Column::new(
            0u64,
            "database1".to_string(),
            "table1".to_string(),
            "name1".to_string(),
            ColumnType::Varchar("".to_string()),
            false,
            false,
            false,
            "".to_string()
        )
    ];

    let _ = create_table(&mut machine, &table, columns);

    let columns = get_columns(&mut machine, &table);

    let mut tuple = Tuple::new();

    tuple.push(Data::Varchar("string1".to_string()));

    let result_set = insert_row(&mut machine, &table, &columns, &columns, &mut vec![tuple], false);
    assert!(matches!(result_set, Ok(_result_set)));

    let result_set = product_cartesian(&mut machine, vec![table]);

    assert_eq!(result_set.tuples.len(), 1);
    assert_eq!(result_set.tuples[0].len(), 2);
    assert_eq!(result_set.column_count(), 2);
}

#[test]
pub fn test_in_one_column_with_primary_key() {
    let pager = Pager::new();
    let mut machine = Machine::new(pager);

    create_tmp_test_folder();

    setup_system(&mut machine);

    let _ = create_database(&mut machine, "database1".to_string(), false);

    let _ = machine.set_actual_database("USE database1".to_string());

    let table = Table::new("database1".to_string(), "table1".to_string());
    let table_columns = vec![
        Column::new(
            0u64,
            "database1".to_string(),
            "table1".to_string(),
            "id".to_string(),
            ColumnType::UnsignedBigint(0),
            true,
            true,
            true,
            "".to_string()
        ),
        Column::new(
            0u64,
            "database1".to_string(),
            "table1".to_string(),
            "name1".to_string(),
            ColumnType::Varchar("".to_string()),
            false,
            false,
            false,
            "".to_string()
        )
    ];

    let _ = create_table(&mut machine, &table, table_columns.clone());

    let mut tuple = Tuple::new();

    tuple.push(Data::Varchar("string1".to_string()));

    let insert_columns = vec![
        Column::new(
            0u64,
            "database1".to_string(),
            "table1".to_string(),
            "name1".to_string(),
            ColumnType::Varchar("".to_string()),
            false,
            false,
            false,
            "".to_string()
        )
    ];

    let result_set = insert_row(&mut machine, &table, &table_columns, &insert_columns, &mut vec![tuple], false);
    assert!(matches!(result_set, Ok(_result_set)));

    let result_set = product_cartesian(&mut machine, vec![table]);

    assert_eq!(result_set.tuples.len(), 1);
    assert_eq!(result_set.column_count(), 2);
}
