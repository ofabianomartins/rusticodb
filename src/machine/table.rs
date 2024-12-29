use std::fmt;

#[derive(Debug, Clone)]
pub struct Table {
    pub database_name: String,
    pub database_alias: String,
    pub name: String,
    pub alias: String
}

impl Table {

    pub fn new(database_name: String, name: String) -> Self {
        Table { 
            database_name: database_name.clone(),
            database_alias: database_name,
            name: name.clone(),
            alias: name
        }
    }

    pub fn new_with_alias(
        database_name: String,
        database_alias: String,
        name: String,
        alias: String
    ) -> Self {
        Table { database_name, database_alias, name, alias }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.database_name == other.database_name
    }
}
impl Eq for Table {}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} - {} - {}", self.database_name, self.name, self.alias)
    }
}
