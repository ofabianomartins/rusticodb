use crate::database::Database;

use crate::parser::parser;
use crate::parser::ExecutionCommands;

#[derive(Clone)]
pub struct Connection {
    pub filepath: String,
    pub database: Database
}

impl Connection {
    pub fn load(filepath: &String) -> Self {
        let database = Database::read_database(filepath);

        Connection { filepath: filepath.clone(), database }
    }

    pub fn execute(&mut self, sql: &String) {
        let commands: ExecutionCommands = parser(sql);

        for create_table_statement in commands.create_tables {
            self.database.append_table(create_table_statement);
        }

        for insert_statement in commands.inserts {
            self.database.insert_row(insert_statement);
        }

        self.database.write_database(&self.filepath)
    }

    pub fn close(self) {

    }
}
