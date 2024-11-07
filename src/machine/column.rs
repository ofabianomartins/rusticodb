use std::fmt;

#[derive(Debug)]
pub struct Column {
    database_name: String,
    table_name: String,
    name: String,
    pub column_type: ColumnType
}

#[derive(Debug)]
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
    pub fn new(database_name: String, table_name: String, name: String, column_type: ColumnType) -> Self {
        Column { database_name, table_name, name, column_type }
    }

    pub fn new_column(name: String, column_type: ColumnType) -> Self {
        Column {
            database_name: String::from(""), 
            table_name: String::from(""), 
            name,
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

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.name)
    }
}

