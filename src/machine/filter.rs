use crate::machine::column::Column;
use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub struct Filter {
    pub conditions: Vec<Condition>,
}

impl Filter {
    pub fn new(conditions: Vec<Condition>) -> Self {
        Filter { conditions }
    }
    
    pub fn filter(&self, tuple: &Tuple, columns: &Vec<Column>) -> bool {
        let mut result: bool = true;
        for condition in &self.conditions {
            result = result && condition.filter(tuple, columns);
        }
        return result;
    }
}

#[derive(Debug)]
pub struct Condition {
    pub column: String,
    pub operator: ConditionType,
    pub value: Vec<u8>
}

impl Condition {
    pub fn new(column: String, operator: ConditionType, value: Vec<u8>) -> Self {
        Condition { column, operator, value }
    }

    pub fn filter(&self, tuple: &Tuple, columns: &Vec<Column>) -> bool {
        let mut value: Vec<u8> = Vec::new();

        for (idx, column) in columns.iter().enumerate() {
            if column.check_column_name(&self.column) {
                value = tuple.get_vec_u8(idx as u16).unwrap();
            }
        }
        
        match self.operator {
            ConditionType::Equal => value == self.value,
            _ => false
        }
    }
}

#[derive(Debug)]
pub enum ConditionType {
    And,
    Or,
    In,
    Not,
    Like,
    Equal,
    Greather,
    GreatherThan,
    Less,
    LessThan
}
