use rusticodb::parser::parser;

#[test]
fn test_parser_create_table_users() {
    let sql = "CREATE TABLE users(id INTEGER, name VARCHAR)";

    let commands = parser(sql);

    assert_eq!(commands.create_tables.len(), 1);
    assert_eq!(commands.create_tables.get(0).unwrap().name.to_string(), "users");
    assert_eq!(commands.create_tables.get(0).unwrap().columns.len(), 2);
}

#[test]
fn test_parser_create_table_messages() {
    let sql = "CREATE TABLE messages(id INTEGER, content VARCHAR, description VARCHAR)";

    let commands = parser(sql);

    assert_eq!(commands.create_tables.len(), 1);
    assert_eq!(commands.create_tables.get(0).unwrap().name.to_string(), "messages");
    assert_eq!(commands.create_tables.get(0).unwrap().columns.len(), 3);
}
