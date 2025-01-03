use std::fmt;

use crate::machine::column::Column;
use crate::machine::raw_val::RawVal;
use crate::storage::tuple::Tuple;

#[derive(Debug)]
pub enum Condition {
    ColName(String),
    Const(RawVal),
    Func1(Condition1Type, Box<Condition>),
    Func2(Condition2Type, Box<Condition>, Box<Condition>)
}

#[derive(Debug)]
pub enum Condition1Type {
    Not,
    Negate
}

#[derive(Debug)]
pub enum Condition2Type {
    And,
    Or,
    In,
    Like,
    Equal,
    NotEqual,
    Greather,
    GreatherThan,
    Less,
    LessThan
}

impl Condition {
    pub fn evaluate(&self, tuple: &Tuple, columns: &Vec<Column>) -> bool {
//        return self.evaluate_value(tuple, columns)[0] == 1u8
        match self {
            Condition::Func1(operator, opr1) => { 
                match operator {
                    Condition1Type::Not => {
                        let value_opr1 = opr1.evaluate_value(tuple, columns);
                        return value_opr1 == vec![0];
                    },
                    _ => false
                }
            },
            Condition::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.evaluate_value(tuple, columns);
                let value_opr2 = opr2.evaluate_value(tuple, columns);
                match operator {
                    Condition2Type::And => value_opr1 == vec![1] && value_opr2 == vec![1],
                    Condition2Type::Or => value_opr1 == vec![1] || value_opr2 == vec![1],
                    Condition2Type::Equal => value_opr1 == value_opr2,
                    Condition2Type::NotEqual => value_opr1 != value_opr2,
                    _ => false
                }
            },
            _ => false
        }
    }

    fn evaluate_value(&self, tuple: &Tuple, columns: &Vec<Column>) -> Vec<u8> {
        match self {
            Condition::ColName(colname)=> {
                let mut value: Vec<u8> = Vec::new();

                for (idx, column) in columns.iter().enumerate() {
                    if column.name == *colname {
                        value = tuple.get_vec_u8(idx as u16).unwrap();
                    }
                }
                value
            },
            Condition::Const(value) => {
                return value.to_vec_u8();
            },
            Condition::Func1(operator, opr1) => { 
                match operator {
                    Condition1Type::Not => {
                        let value_opr1 = opr1.evaluate_value(tuple, columns);
                        return if value_opr1 == vec![0] { vec![0] } else { vec![1] };
                    },
                    _ => vec![0u8]
                }
            },
            Condition::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.evaluate_value(tuple, columns);
                let value_opr2 = opr2.evaluate_value(tuple, columns);
                match operator {
                    Condition2Type::Equal => {
                        return if value_opr1 == value_opr2 { vec![1] } else { vec![0] };
                    },
                    _ => vec![0u8]
                }
            }
        }
    }

    fn to_str(&self) -> String {
        match self {
            Condition::ColName(colname) => colname.to_string(),
            Condition::Const(value) => value.to_string(),
            Condition::Func1(operator, opr1) => { 
                match operator {
                    Condition1Type::Not => format!("NOT {}", opr1),
                    _ => String::from("")
                }
            },
            Condition::Func2(operator, opr1, opr2) => {
                match operator {
                    Condition2Type::Equal => format!("{} AND {}", opr1, opr2),
                    _ => String::from("")
                }
            }
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
