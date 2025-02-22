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

fn _get_type(column_type: ColumnType) -> CellType {
   match column_type {
       ColumnType::Varchar => CellType::String,
       _ => CellType::Null
   }
}

fn logic_result(result: bool) -> Cell {
    return if result { Cell::new_true() } else { Cell::new_false() };
}

fn compare_func1(operator: &Expression1Type, opr1: Cell) -> Cell {
    match operator {
        // Logic implementation
        Expression1Type::Not => !opr1,
        Expression1Type::Negate => -opr1,
    }
}

fn compare_func2(operator: &Expression2Type, opr1: Cell, opr2: Cell) -> Cell {
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
    }
}

impl Expression {
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
                    RawVal::Str(value) => { return Cell::new_string(value) },
                    &RawVal::Float(_) | &RawVal::Null => return Cell::new_null()
                };
            },
            Expression::Func1(operator, opr1) => { 
                return compare_func1(operator, opr1.result(tuple, columns));
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
