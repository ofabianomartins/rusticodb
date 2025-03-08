use crate::machine::Table;

#[derive(Debug)]
pub struct AuthDb {}

impl AuthDb {

    pub fn db() -> String {
        return String::from("auth");
    }

    pub fn tbl_name_databases() -> String {
        return String::from("databases");
    }

    pub fn tbl_name_tables() -> String {
        return String::from("tables");
    }

    pub fn tbl_name_columns() -> String {
        return String::from("columns");
    }

    pub fn tbl_name_sequences() -> String {
        return String::from("sequences");
    }

    pub fn tbl_name_indexes() -> String {
        return String::from("indexes");
    }

    pub fn table_databases() -> Table {
        return Table::new(AuthDb::db(), AuthDb::tbl_name_databases());
    }

    pub fn table_tables() -> Table {
        return Table::new(AuthDb::db(), AuthDb::tbl_name_tables());
    }

    pub fn table_columns() -> Table {
        return Table::new(AuthDb::db(), AuthDb::tbl_name_columns());
    }

    pub fn table_sequences() -> Table {
        return Table::new(AuthDb::db(), AuthDb::tbl_name_sequences());
    }

    pub fn table_indexes() -> Table {
        return Table::new(AuthDb::db(), AuthDb::tbl_name_indexes());
    }

}
