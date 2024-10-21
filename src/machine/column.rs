
#[derive(Debug)]
pub struct Column {
    database_name: String,
    table_name: String,
    name: String
}

impl Column {

    pub fn new(database_name: String, table_name: String, name: String) -> Self {
        Column { database_name, table_name, name }
    }

    pub fn check_name(&self, database_name: &String, table_name: &String, other_name: &String) -> bool {
        return self.database_name == *database_name && 
            self.table_name == *table_name && 
            self.name == *other_name; 
    }


}
