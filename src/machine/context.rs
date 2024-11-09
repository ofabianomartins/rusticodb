use crate::machine::database::Database;
use crate::machine::table::Table;
use crate::machine::column::Column;
use crate::machine::column::ColumnType;

#[derive(Debug)]
pub struct Context {
    pub actual_database: Option<String>,

    databases: Vec<Database>,
    tables: Vec<Table>,
    columns: Vec<Column>
}

impl Context {
    pub fn new() -> Self {
        Context {
            actual_database: None,
            databases: Vec::new(),
            tables: Vec::new(),
            columns: Vec::new()
        } 
    }

    pub fn set_actual_database(&mut self, name: String) {
        if self.check_database_exists(&name) == false {
            self.actual_database = None;
        } else {
            self.actual_database = Some(name);
        }
    }

    pub fn add_database(&mut self, name: String) -> bool {
        if self.check_database_exists(&name) == true {
            return false
        }
        self.databases.push(Database::new(name));
        return true;
    }

    pub fn remove_database(&mut self, name: String) -> bool {
        if let Some(index) = self.databases.iter().position(|x| x.check_name(&name)) {
            self.databases.remove(index);
            return true;
        }
        return false;
    }

    pub fn check_database_exists(&self, name: &String) -> bool {
        let mut found = false;
        for elem in &self.databases {
            if elem.check_name(name) == true {
                found = true;
                break;
            }
        }
        return found;
    }

    pub fn add_table(&mut self, database_name: String, name: String) -> bool {
        if self.check_table_exists(&database_name, &name) == true {
            return false
        }
        self.tables.push(Table::new(database_name, name));
        return true;
    }

    pub fn check_table_exists(&self, database_name: &String, name: &String) -> bool {
        let mut found = false;
        for elem in &self.tables {
            if elem.check_name(database_name, name) == true {
                found = true;
                break;
            }
        }
        return found;
    }

    pub fn add_column(&mut self, database_name: String, table_name: String, name: String, column_type: ColumnType) -> bool {
        if self.check_column_exists(&database_name, &table_name, &name) == true {
            return false
        }
        self.columns.push(Column::new(database_name, table_name, name, column_type));
        return true;
    }

    pub fn check_column_exists(&self, database_name: &String, table_name: &String, name: &String) -> bool {
        let mut found = false;
        for elem in &self.columns {
            if elem.check_name(database_name, table_name, name) == true {
                found = true;
                break;
            }
        }
        return found;
    }

}
