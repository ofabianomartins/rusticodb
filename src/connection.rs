use crate::column::Column;
use crate::database::Database;

use crate::table::Table;
use crate::parser::parser;
use crate::parser::ExecutionCommands;

#[derive(Clone)]
pub struct Connection {
    pub filepath: String,
    pub database: Database
}

impl Connection {

    pub fn load(filepath: &String) -> Self {
        let mut database = Database::read_database(filepath);
        // let _file = File::create(filepath);

        Connection { filepath: filepath.clone(), database }
    }

    pub fn execute(&mut self, sql: &String) {
        let commands: ExecutionCommands = parser(sql);

        for create_table_statement in commands.create_tables {
            // create_table::create_table(filepath, create_table_statement)
            //

            let mut table_columns: Vec<Column> = Vec::new();
            
            for column in create_table_statement.columns {
                table_columns.push(
                    Column {
                        name: column.name.to_string(),
                        data_type: column.data_type.to_string()
                    }
                );

            }

            self.database.tables.push(
                Table {
                    name: create_table_statement.name.to_string(),
                    columns: table_columns,
                    indexes: Vec::new()
                }
            )
        }

        self.database.write_database(&self.filepath)
    }

    pub fn close(self) {

    }
}
