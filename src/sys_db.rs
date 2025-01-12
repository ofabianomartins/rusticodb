use crate::config::Config;

use crate::machine::Table;

#[derive(Debug)]
pub struct SysDb {}

impl SysDb {

    pub fn table_databases() -> Table {
        return Table::new(Config::sysdb(), Config::sysdb_table_databases());
    }

    pub fn table_tables() -> Table {
        return Table::new(Config::sysdb(), Config::sysdb_table_tables());
    }

    pub fn table_columns() -> Table {
        return Table::new(Config::sysdb(), Config::sysdb_table_columns());
    }

    pub fn table_sequences() -> Table {
        return Table::new(Config::sysdb(), Config::sysdb_table_sequences());
    }

}
