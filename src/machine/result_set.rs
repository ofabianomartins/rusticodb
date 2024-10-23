use std::fmt;

use crate::storage::tuple::Tuple;
use crate::machine::column::Column;

#[derive(Debug)]
pub struct ResultSet {
    pub set_type: ResultSetType,
    pub message: String,
    pub tuples: Vec<Tuple>,
    pub columns: Vec<Column>
}

impl ResultSet {
    pub fn new_select(columns: Vec<Column>, tuples: Vec<Tuple>) -> Self {
        ResultSet { 
            set_type: ResultSetType::Select, 
            message: String::from(""), 
            columns, 
            tuples 
        }
    }

    pub fn new_command(set_type: ResultSetType, message: String) -> Self {
        ResultSet { 
            set_type, 
            message,
            columns: Vec::new(), 
            tuples: Vec::new()
        }
    }
}

impl fmt::Display for ResultSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.set_type {
            ResultSetType::Change => {
                write!(f, "{}", self.message)
            },
            ResultSetType::Select => {
                let _ = write!(f, "--------------------");
                let _ = write!(f, "{:?}", self.columns);
                for tuple_item in &self.tuples {
                    let _ = write!(f, "--------------------");
                    let _ = write!(f, "{:?}", tuple_item);
                }
                write!(f, "--------------------")
            }
        }
    }
}

#[derive(Debug)]
pub enum ResultSetType {
    Change,
    Select,
}

#[derive(Debug)]
pub enum ExecutionError {
    ParserError(String),
    TokenizerError(String),
    RecursionLimitExceeded,
    DatabaseNotExists(String),
    DatabaseExists(String),
    DatabaseNotSetted
}
