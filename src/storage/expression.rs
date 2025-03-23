use std::fmt;

use crate::storage::RawVal;
use crate::storage::Tuple;
use crate::storage::tuple_get_cell;
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

fn logic_result(result: bool) -> Cell {
    return Cell { data: vec![CellType::Boolean as u8, result as u8] };
}

fn compare_func1(operator: &Expression1Type, opr1: Cell) -> Vec<u8> {
    match operator {
        // Logic implementation
        Expression1Type::Not => !opr1,
        Expression1Type::Negate => -opr1,
    }.data
}

fn compare_func2(operator: &Expression2Type, opr1: Cell, opr2: Cell) -> Vec<u8> {
    match operator {
        // Logic implementation
        Expression2Type::And => logic_result(opr1.is_true() && opr2.is_true()),
        Expression2Type::Or => logic_result(opr1.is_true() || opr2.is_true()),

        // Comparison implementation
        Expression2Type::GreatherOrEqual => logic_result(opr1 >= opr2),
        Expression2Type::GreatherThan => logic_result(opr1 > opr2),
        Expression2Type::LessOrEqual => logic_result(opr1 <= opr2),
        Expression2Type::LessThan => logic_result(opr1 < opr2),
        Expression2Type::Equal => logic_result(opr1 == opr2),
        Expression2Type::NotEqual => logic_result(opr1 != opr2),

        // Aritmetic implementation
        Expression2Type::Add => opr1 + opr2, 
        Expression2Type::Sub => opr1 - opr2,
        Expression2Type::Mul => opr1 * opr2,
        Expression2Type::Div => opr1 / opr2
    }.data
}

impl Expression {
    pub fn result(&self, tuple: &Tuple, columns: &Vec<String>) -> Vec<u8> {
        match self {
            Expression::Empty => vec![CellType::Null as u8],
            Expression::ColName(colname)=> {
                for (idx, column) in columns.iter().enumerate() {
                    if *column == *colname {
                        return tuple_get_cell(tuple, idx as u16);
                    }
                }
                return vec![CellType::Null as u8];
            },
            Expression::Const(value) => {
                match value {
                    &RawVal::Int(number) => {
                        let mut data = vec![CellType::UnsignedBigint as u8];
                        data.append(&mut number.to_be_bytes().to_vec());
                        return data
                    },
                    RawVal::Str(value) => { 
                        let mut bytes_array = value.clone().into_bytes();
                        let mut data = vec![CellType::Varchar as u8];
                        data.append(&mut (bytes_array.len() as u16).to_be_bytes().to_vec());
                        data.append(&mut bytes_array);

                        return data 
                    },
                    &RawVal::Float(_) | &RawVal::Null => return vec![CellType::Null as u8],
                };
            },
            Expression::Func1(operator, opr1) => { 
                return compare_func1(operator, Cell { data: opr1.result(tuple, columns) });
            },
            Expression::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.result(tuple, columns);
                let value_opr2 = opr2.result(tuple, columns);
                return compare_func2(operator, Cell { data: value_opr1 }, Cell { data: value_opr2 });
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
