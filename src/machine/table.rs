
#[derive(Debug)]
pub struct Table {
    database_name: String,
    name: String
}

impl Table {

    pub fn new(database_name: String, name: String) -> Self {
        Table { database_name, name }
    }

    pub fn check_name(&self, database_name: &String, other_name: &String) -> bool {
        return self.database_name == *database_name && self.name == *other_name; 
    }

}
