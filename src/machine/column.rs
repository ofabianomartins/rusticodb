use std::fmt;

#[derive(Debug, Clone)]
pub struct Column {
    pub database_name: String,
    pub database_alias: String,
    pub table_name: String,
    pub table_alias: String,
    pub name: String,
    pub alias: String,
    pub column_type: ColumnType
}

#[derive(Debug, Clone)]
pub enum ColumnType {
    Null = 1,
    Boolean = 2,
    UnsignedTinyint = 3,
    UnsignedSmallint = 4,
    UnsignedInt = 5,
    UnsignedBigint = 6,
    SignedTinyint = 7,
    SignedSmallint = 8,
    SignedInt = 9,
    SignedBigint = 10,
    Varchar = 11,
    Text = 12
}

impl Column {
    pub fn new(
        database_name: String,
        table_name: String,
        name: String,
        column_type: ColumnType
    ) -> Self {
        Column { 
            database_name: database_name.clone(), 
            database_alias: database_name,
            table_name: table_name.clone(), 
            table_alias: table_name, 
            name: name.clone(), 
            alias: name, 
            column_type 
        }
    }

    pub fn new_with_alias(
        database_name: String,
        database_alias: String,
        table_name: String,
        table_alias: String,
        name: String,
        alias: String,
        column_type: ColumnType
    ) -> Self {
        Column { 
            database_name, 
            database_alias, 
            table_name, 
            table_alias, 
            name, 
            alias, 
            column_type 
        }
    }

    pub fn check_name(&self, database_name: &String, table_name: &String, other_name: &String) -> bool {
        return self.database_name == *database_name && 
            self.table_name == *table_name && 
            self.name == *other_name; 
    }

    pub fn check_column_name(&self, other_name: &String) -> bool {
        return self.name == *other_name; 
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.table_alias == other.table_alias
    }
}
impl Eq for Column {}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {} {}", self.table_alias, self.table_name, self.name, self.alias)
    }
}

