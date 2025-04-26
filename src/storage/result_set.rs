use std::fmt;
use std::vec::Vec;

use crate::machine::Column;

use crate::storage::Expression;
use crate::storage::Tuple;
use crate::storage::Data;
use crate::storage::tuple_new;

use crate::utils::ExecutionError;

#[derive(Debug)]
pub struct ResultSet {
    pub set_type: ResultSetType,
    pub message: String,
    pub tuples: Vec<Tuple>,
    pub columns: Vec<Column>
}

/*
 * This object sholud implement the relational algebra operator
 * and acts like a dataframe. On futher situation, will be implement to use
 * Hard Disk to increase the data limit. But now only use the main memory. 
 *
 * The basic operator to be implement will be: 
 *   - selection
 *   - projection
 *   - cartesian product
 *   - union
 *   - diff 
 * 
 */

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

    pub fn new_empty() -> Self {
        ResultSet { 
            set_type: ResultSetType::Select, 
            message: String::from(""), 
            columns: Vec::new(), 
            tuples: Vec::new() 
        }
    }

    fn get_column_position(&self, column_name: &String) -> Option<usize> {
        return self.columns.iter().position(|elem| elem.check_column_name(column_name) )
    }

    pub fn get_value(&self, index: usize, column_name: &String) -> Result<Data, ExecutionError> {
        match &mut self.get_column_position(column_name) {
            Some(position) => match self.tuples.get(index) {
                Some(tuple) => match tuple.get(*position) {
                   Some(pos) => Ok(pos.clone()),
                   None => Err(ExecutionError::PositionNotExists(position.clone() as usize))
                },
                None => Err(ExecutionError::TupleNotExists(index))
            },
            None => Err(ExecutionError::ColumnNotExists(column_name.clone()))
        }
    }

    pub fn line_count(&self) -> usize {
        return self.tuples.len(); 
    }

    pub fn column_count(&self) -> usize {
        return self.columns.len(); 
    }

    pub fn full(&self) -> bool {
        return self.tuples.len() > 0; 
    }

    pub fn empty(&self) -> bool {
        return self.tuples.len() == 0; 
    }

    fn count_column_size(&self) -> Vec<u64> {
        let mut column_length: Vec<u64> = Vec::new();
        for column in &self.columns {
            column_length.push(column.alias.len() as u64);
        }
        for tuple_item in &self.tuples {
            let mut cell_index: usize = 0;

            while cell_index < column_length.len() {
                let cell_length = tuple_item.get(cell_index).unwrap().to_string().len() as u64;

                let old_version = column_length.get_mut(cell_index).unwrap();

                if *old_version < cell_length {
                    column_length[cell_index] = cell_length;
                }

                cell_index += 1;
            }
        }
        return column_length;
    }

    pub fn projection(&self, projection_columns: Vec<Column>) -> Result<ResultSet, ExecutionError> {
        let mut column_indexes: Vec<u16> = Vec::new();

        for (_idxr, partial) in projection_columns.iter().enumerate() {
            for (main_index, partial2) in self.columns.iter().enumerate() {
                if partial2 == partial {
                    column_indexes.push(main_index as u16)
                }
            }
        }
        let mut new_set: ResultSet = ResultSet::new_select(projection_columns, vec![]);

        for (_idxr, partial) in self.tuples.iter().enumerate() {
            let mut new_tuple: Tuple = tuple_new();

            for (_idxr, cell_index) in column_indexes.iter().enumerate() {
                new_tuple.push(partial.get(*cell_index as usize).unwrap().clone());
            }
            
            new_set.tuples.push(new_tuple);
        }

        return Ok(new_set);
    }

    pub fn selection(&self, condition: Expression) -> Result<ResultSet, ExecutionError> {
        let mut columns: Vec<Column> = Vec::new();
        let mut tuples: Vec<Tuple> = Vec::new();

        for column in &self.columns {
            columns.push(column.clone());
        }

        let column_names = self.columns.iter().map(|e| e.name.clone()).collect();

        for tuple in &self.tuples {
            if condition.result(tuple, &column_names).is_true() {
                tuples.push(tuple.clone());
            }
        }

        Ok(ResultSet::new_select(columns, tuples))
    }

    pub fn limit(&self, size: usize) -> ResultSet {
        let mut columns: Vec<Column> = Vec::new();
        let mut tuples: Vec<Tuple> = Vec::new();

        for column in &self.columns {
            columns.push(column.clone());
        }

        if size > self.tuples.len() {
            for tuple in &self.tuples {
                tuples.push(tuple.clone());
            }
        } else {
            for index in 0..size {
                let tuple: &Tuple = self.tuples.get(index).unwrap();
                tuples.push(tuple.clone());
            }
        }

        ResultSet::new_select(columns, tuples)
    }

    pub fn offset(&self, size: usize) -> ResultSet {
        let mut columns: Vec<Column> = Vec::new();
        let mut tuples: Vec<Tuple> = Vec::new();

        for column in &self.columns {
            columns.push(column.clone());
        }

        for index in size..self.tuples.len() {
            let tuple: &Tuple = self.tuples.get(index).unwrap();
            tuples.push(tuple.clone());
        }

        ResultSet::new_select(columns, tuples)
    }

    pub fn cartesian_product(&self, other_set: &ResultSet) -> ResultSet {
        let new_columns: Vec<Column> = vec![self.columns.clone(), other_set.columns.clone()].concat();
        let mut new_set: ResultSet = ResultSet::new_select(new_columns, vec![]);

        if self.empty() && other_set.full() {
            for (_idx2, element) in other_set.tuples.iter().enumerate() {
                let mut new_tuple: Tuple = tuple_new();

                let mut cell_index = 0;
                while cell_index < element.len() {
                    new_tuple.push(element.get(cell_index).unwrap().clone());
                    cell_index += 1;
                }
                
                new_set.tuples.push(new_tuple);
            }
        } else if self.full() && other_set.empty() {
            for (_idxr, partial) in self.tuples.iter().enumerate() {
                let new_tuple: Tuple = partial.clone();

                new_set.tuples.push(new_tuple);
            }
        } else {
            for (_idxr, partial) in self.tuples.iter().enumerate() {
                for (_idx2, element) in other_set.tuples.iter().enumerate() {
                    let mut new_tuple: Tuple = partial.clone();

                    let mut cell_index = 0;
                    while cell_index < element.len() {
                        new_tuple.push(element.get(cell_index).unwrap().clone());
                        cell_index += 1;
                    }
                    
                    new_set.tuples.push(new_tuple);
                }
            }
        }

        return new_set;
    }

    pub fn union(&self, other_set: &ResultSet) -> Result<ResultSet, ExecutionError> {
        let mut new_set: ResultSet = ResultSet::new_select(self.columns.clone(), vec![]);

        for (_idxr, partial) in self.tuples.iter().enumerate() {
            new_set.tuples.push(partial.clone());
        }

        for (_idxr, partial) in other_set.tuples.iter().enumerate() {
            new_set.tuples.push(partial.clone());
        }

        return Ok(new_set);
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
        let _ = write!(f, "\n\n");
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
                    let _ = write!(f, " {}", column.alias);

                    let adjust_column_size = column_length.get(cell_index as usize).unwrap() - (column.name.len() as u64);
                    print_complete_cell(f, adjust_column_size);
                    let _ = write!(f, " |");
                }
                let _ = write!(f, "\n");

                let _ = print_line_result(f, column_size_count);

                for tuple_item in &self.tuples {
                    let mut cell_index: usize = 0;
                    while cell_index < column_length.len() {
                        let cell_value = tuple_item.get(cell_index).unwrap();
                        let _ = write!(f, "| {} ", cell_value);

                        let adjust_column_size = column_length.get(cell_index).unwrap() - (cell_value.to_string().len() as u64);
                        print_complete_cell(f, adjust_column_size);
                        cell_index += 1;
                    }
                    let _ = write!(f, "|\n");
                }

                let _ = print_line_result(f, column_size_count);
                write!(f, "Result size: {} ", self.tuples.len())
            }
        }
    }
}

#[derive(Debug)]
pub enum ResultSetType {
    Change,
    Select,
}

