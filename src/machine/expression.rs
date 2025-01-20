use std::fmt;

use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::RawVal;

use crate::storage::Tuple;
use crate::storage::Cell;
use crate::storage::CellType;

#[derive(Debug)]
pub enum Expression {
    ColName(String),
    Const(RawVal),
    Func1(Expression1Type, Box<Expression>),
    Func2(Expression2Type, Box<Expression>, Box<Expression>),
    Empty
}

#[derive(Debug)]
pub enum Expression1Type {
    Not,
    Negate
}

#[derive(Debug)]
pub enum Expression2Type {
    // Logic operators
    And,
    Or,

    // Comparable operators
    // In,
    // Like,
    Equal,
    NotEqual,
    GreatherOrEqual,
    GreatherThan,
    LessOrEqual,
    LessThan,

    // Aritmetic operators
    Add,
    Sub,
    Mul,
    Div
}

fn get_type(column_type: ColumnType) -> CellType {
   match column_type {
       ColumnType::Varchar => CellType::String,
       _ => CellType::Null
   }
}


fn compare_func2(operator: &Expression2Type, opr1: Cell, opr2: Cell) -> Cell {
    return match operator {
        Expression2Type::And => {
            return if opr1 == Cell::new_true() && opr2 == Cell::new_true() { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::Or => {
            return if opr1 == Cell::new_true() || opr2 == Cell::new_true() { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::GreatherOrEqual => {
            return if opr1 >= opr2 { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::GreatherThan => {
            return if opr1 > opr2 { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::LessOrEqual => {
            return if opr1 <= opr2 { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::LessThan => {
            return if opr1 < opr2 { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::Equal => {
            return if opr1 == opr2 { Cell::new_true() } else { Cell::new_false() };
        },
        Expression2Type::Add => {
            return opr1 + opr2;
        },
        Expression2Type::Sub => {
            return opr1 - opr2;
        },
        Expression2Type::Mul => {
            return opr1 * opr2;
        },
        Expression2Type::Div => {
            return opr1 / opr2;
        },
        Expression2Type::NotEqual => {
            return if opr1 != opr2 { Cell::new_true() } else { Cell::new_false() };
        },
        _ => Cell::new_null()
    };
}

impl Expression {
    pub fn evaluate(&self, tuple: &Tuple, columns: &Vec<Column>) -> bool {
        match self {
            Expression::Empty => true,
            Expression::Func1(operator, opr1) => { 
                match operator {
                    Expression1Type::Not => {
                        let value_opr1 = opr1.evaluate_value(tuple, columns);
                        return value_opr1 == vec![0];
                    },
                    _ => false
                }
            },
            Expression::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.evaluate_value(tuple, columns);
                let value_opr2 = opr2.evaluate_value(tuple, columns);
                match operator {
                    Expression2Type::And => value_opr1 == vec![1] && value_opr2 == vec![1],
                    Expression2Type::Or => value_opr1 == vec![1] || value_opr2 == vec![1],
                    Expression2Type::Equal => value_opr1 == value_opr2,
                    Expression2Type::NotEqual => value_opr1 != value_opr2,
                    _ => false
                }
            },
            _ => false
        }
    }

    fn evaluate_value(&self, tuple: &Tuple, columns: &Vec<Column>) -> Vec<u8> {
        match self {
            Expression::Empty => vec![1],
            Expression::ColName(colname)=> {
                let mut value: Vec<u8> = Vec::new();

                for (idx, column) in columns.iter().enumerate() {
                    if column.name == *colname {
                        value = tuple.get_vec_u8(idx as u16).unwrap();
                    }
                }
                value
            },
            Expression::Const(value) => {
                return value.to_vec_u8();
            },
            Expression::Func1(operator, opr1) => { 
                match operator {
                    Expression1Type::Not => {
                        let value_opr1 = opr1.evaluate_value(tuple, columns);
                        return if value_opr1 == vec![0] { vec![0] } else { vec![1] };
                    },
                    _ => vec![0u8]
                }
            },
            Expression::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.evaluate_value(tuple, columns);
                let value_opr2 = opr2.evaluate_value(tuple, columns);
                match operator {
                    Expression2Type::And => {
                        return if value_opr1 == vec![1] && value_opr2 == vec![1] { vec![1] } else { vec![0] };
                    },
                    Expression2Type::Equal => {
                        return if value_opr1 == value_opr2 { vec![1] } else { vec![0] };
                    },
                    _ => vec![0u8]
                }
            }
        }
    }

    pub fn result(&self, tuple: &Tuple, columns: &Vec<Column>) -> Cell {
        match self {
            Expression::Empty => Cell::new_null(),
            Expression::ColName(colname)=> {
                for (idx, column) in columns.iter().enumerate() {
                    if column.name == *colname {
                        return tuple.get_cell(idx as u16);
                    }
                }
                return Cell::new_null();
            },
            Expression::Const(value) => {
                match value {
                    &RawVal::Int(number) => {
                        return Cell::new_type(CellType::UnsignedBigint, number.to_be_bytes().to_vec())
                    },
                    &RawVal::Float(_) | &RawVal::Str(_) | &RawVal::Null => return Cell::new_null()
                };
            },
            Expression::Func1(operator, opr1) => { 
                return match operator {
                    Expression1Type::Not => {
                        let value_opr1 = opr1.result(tuple, columns);
                        return if value_opr1 == Cell::new_false() { Cell::new_false() } else { Cell::new_true() };
                    },
                    _ => Cell::new_null()
                };
            },
            Expression::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.result(tuple, columns);
                let value_opr2 = opr2.result(tuple, columns);
                return compare_func2(operator, value_opr1, value_opr2);
            }
        }
    }

    fn to_str(&self) -> String {
        match self {
            Expression::Empty => String::from(""),
            Expression::ColName(colname) => colname.to_string(),
            Expression::Const(value) => value.to_string(),
            Expression::Func1(operator, opr1) => { 
                match operator {
                    Expression1Type::Not => format!("NOT {}", opr1),
                    _ => String::from("")
                }
            },
            Expression::Func2(operator, opr1, opr2) => {
                match operator {
                    Expression2Type::Equal => format!("{} == {}", opr1, opr2),
                    Expression2Type::And => format!("{} && ({})", opr1, opr2),
                    _ => String::from("")
                }
            }
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
