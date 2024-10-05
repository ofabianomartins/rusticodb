use rusticodb::machine::context::Context;

#[test]
pub fn test_if_actual_database_exists_on_system_to_context() {
    let database1 = String::from("database1");
    let mut context = Context::new();

    assert_eq!(context.set_actual_database(database1.clone()), false);
    assert_eq!(context.check_database_exists(&database1), false);
    assert!(matches!(context.actual_database, None))
}

#[test]
pub fn test_if_actual_database_is_set_to_context() {
    let database1 = String::from("database1");
    let mut context = Context::new();

    context.add_database(database1.clone());

    assert!(context.set_actual_database(database1.clone()));
    assert!(context.check_database_exists(&database1));
    assert!(matches!(context.actual_database, Some(database1)))
}

#[test]
pub fn test_if_database_is_add_to_context() {
    let database1 = String::from("database1");
    let mut context = Context::new();

    assert!(context.add_database(database1.clone()));
    assert!(context.check_database_exists(&database1));
}

#[test]
pub fn test_if_database_is_add_twice_failed_on_second() {
    let database1 = String::from("database1");
    let mut context = Context::new();

    assert!(context.add_database(database1.clone()));
    assert!(context.check_database_exists(&database1));
    assert_eq!(context.add_database(database1), false);
}

#[test]
pub fn test_if_table_is_add_to_context() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let mut context = Context::new();

    assert!(context.add_table(database1.clone(), table1.clone()));
    assert!(context.check_table_exists(&database1, &table1));
}

#[test]
pub fn test_if_table_of_the_same_database_is_add_twice_failed_on_second() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let mut context = Context::new();

    assert!(context.add_table(database1.clone(), table1.clone()));
    assert!(context.check_table_exists(&database1, &table1));
    assert_eq!(context.add_table(database1, table1), false);
}

#[test]
pub fn test_if_column_is_add_to_context() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let column1 = String::from("column1");
    let mut context = Context::new();

    assert!(context.add_column(database1.clone(), table1.clone(), column1.clone()));
    assert!(context.check_column_exists(&database1, &table1, &column1));
}

#[test]
pub fn test_if_column_of_the_same_table_is_add_twice_failed_on_second() {
    let database1 = String::from("database1");
    let table1 = String::from("table1");
    let column1 = String::from("column1");
    let mut context = Context::new();

    assert!(context.add_column(database1.clone(), table1.clone(), column1.clone()));
    assert!(context.check_column_exists(&database1, &table1, &column1));
    assert_eq!(context.add_column(database1, table1, column1), false);
}
