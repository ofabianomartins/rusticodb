use std::fmt;

use crate::storage::tuple::Tuple;
use crate::storage::cell::ParserError;
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

    fn get_column_position(&self, column_name: &String) -> Option<usize> {
        return self.columns.iter().position(|elem| elem.check_column_name(column_name) )
    }

    pub fn get_string(&mut self, index: usize, column_name: &String) -> Result<String, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_string(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_text(&mut self, index: usize, column_name: &String) -> Result<String, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_text(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_tinyint(&mut self, index: usize, column_name: &String) -> Result<u8, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_tinyint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_smallint(&mut self, index: usize, column_name: &String) -> Result<u16, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_smallint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_int(&mut self, index: usize, column_name: &String) -> Result<u32, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_int(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_bigint(&mut self, index: usize, column_name: &String) -> Result<u64, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_bigint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_tinyint(&mut self, index: usize, column_name: &String) -> Result<i8, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_tinyint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_smallint(&mut self, index: usize, column_name: &String) -> Result<i16, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_smallint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_int(&mut self, index: usize, column_name: &String) -> Result<i32, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_int(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_bigint(&mut self, index: usize, column_name: &String) -> Result<i64, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_bigint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn line_count(&mut self) -> usize {
        return self.tuples.len(); 
    }

    pub fn column_count(&mut self) -> usize {
        return self.columns.len(); 
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
