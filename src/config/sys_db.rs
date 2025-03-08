use crate::machine::Table;

#[derive(Debug)]
pub struct SysDb {}

impl SysDb {

    pub fn dbname() -> String {
        return String::from("rusticodb");
    }

    pub fn tblname_databases() -> String {
        return String::from("databases");
    }

    pub fn tblname_tables() -> String {
        return String::from("tables");
    }

    pub fn tblname_columns() -> String {
        return String::from("columns");
    }

    pub fn tblname_sequences() -> String {
        return String::from("sequences");
    }

    pub fn tblname_indexes() -> String {
        return String::from("indexes");
    }

    pub fn table_databases() -> Table {
        return Table::new(SysDb::dbname(), SysDb::tblname_databases());
    }

    pub fn table_tables() -> Table {
        return Table::new(SysDb::dbname(), SysDb::tblname_tables());
    }

    pub fn table_columns() -> Table {
        return Table::new(SysDb::dbname(), SysDb::tblname_columns());
    }

    pub fn table_sequences() -> Table {
        return Table::new(SysDb::dbname(), SysDb::tblname_sequences());
    }

    pub fn table_indexes() -> Table {
        return Table::new(SysDb::dbname(), SysDb::tblname_indexes());
    }

}
