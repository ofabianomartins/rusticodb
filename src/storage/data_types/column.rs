
#[derive(Debug)]
pub enum DataTypes {
    VARCHAR,
    INT,
    FLOAT
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataTypes
}
