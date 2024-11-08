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

    pub fn get_string(&self, index: usize, column_name: &String) -> Result<String, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_string(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_text(&self, index: usize, column_name: &String) -> Result<String, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_text(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_tinyint(&self, index: usize, column_name: &String) -> Result<u8, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_tinyint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_smallint(&self, index: usize, column_name: &String) -> Result<u16, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_smallint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_int(&self, index: usize, column_name: &String) -> Result<u32, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_int(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_unsigned_bigint(&self, index: usize, column_name: &String) -> Result<u64, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_unsigned_bigint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_tinyint(&self, index: usize, column_name: &String) -> Result<i8, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_tinyint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_smallint(&self, index: usize, column_name: &String) -> Result<i16, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_smallint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_int(&self, index: usize, column_name: &String) -> Result<i32, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_int(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn get_signed_bigint(&self, index: usize, column_name: &String) -> Result<i64, ParserError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => tuple.get_signed_bigint(*position as u16),
                None => Err(ParserError::NoneExists)
            },
            None => Err(ParserError::NoneExists)
        }
    }

    pub fn line_count(&self) -> usize {
        return self.tuples.len(); 
    }

    pub fn column_count(&self) -> usize {
        return self.columns.len(); 
    }

    fn count_column_size(&self) -> Vec<u64> {
        let mut column_length: Vec<u64> = Vec::new();
        for column in &self.columns {
            column_length.push(column.name.len() as u64);
        }
        for tuple_item in &self.tuples {
            let cell_count = tuple_item.cell_count() as u64;
            let mut cell_index: u64 = 0;

            while cell_index < cell_count {
                let cell_length = tuple_item.get_cell(cell_index as u16).to_string().len() as u64;

                let old_version = column_length.get_mut(cell_index as usize).unwrap();

                if *old_version < cell_length {
                    column_length[cell_index as usize] = cell_length;
                }

                cell_index += 1;
            }
        }
        return column_length;
    }

}

fn print_line_result(f: &mut fmt::Formatter, column_size_count: u64) {
    let mut cell_index: u64 = 1;
    let _ = write!(f, "+");
    while cell_index < (column_size_count - 1){
        let _ = write!(f, "-");
        cell_index += 1;
    }
    let _ = write!(f, "+\n");
}

fn print_complete_cell(f: &mut fmt::Formatter, column_size_count: u64) {
    let mut cell_index: u64 = 0;
    while cell_index < column_size_count{
        let _ = write!(f, " ");
        cell_index += 1;
    }
}

impl fmt::Display for ResultSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.set_type {
            ResultSetType::Change => {
                write!(f, "{}", self.message)
            },
            ResultSetType::Select => {
                let column_length: Vec<u64> = self.count_column_size();

                let column_sum_count: u64 = column_length.iter().sum();
                let column_size_count: u64 = column_sum_count + (column_length.len() as u64) * 3u64 + 1u64;
                let _ = print_line_result(f, column_size_count);

                let _ = write!(f, "|");
                for (cell_index, column) in self.columns.iter().enumerate() {
                    let _ = write!(f, " {}", column);

                    let adjust_column_size = column_length.get(cell_index as usize).unwrap() - (column.name.len() as u64);
                    print_complete_cell(f, adjust_column_size);
                    let _ = write!(f, " |");
                }
                let _ = write!(f, "\n");

                let _ = print_line_result(f, column_size_count);

                for tuple_item in &self.tuples {
                    let cell_count = tuple_item.cell_count() as u64;
                    let mut cell_index: u64 = 0;
                    while cell_index < cell_count {
                        let cell_value = tuple_item.get_cell(cell_index as u16).to_string();
                        let _ = write!(f, "| {} ", cell_value);

                        let adjust_column_size = column_length.get(cell_index as usize).unwrap() - (cell_value.len() as u64);
                        print_complete_cell(f, adjust_column_size);
                        cell_index += 1;
                    }
                    let _ = write!(f, "|\n");
                }

                let _ = print_line_result(f, column_size_count);
                write!(f, "")
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
    DatabaseNotSetted, 
    NotImplementedYet
}
