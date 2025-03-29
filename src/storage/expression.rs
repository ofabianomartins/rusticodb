use std::fmt;

use crate::storage::Data;
use crate::storage::Tuple;

#[derive(Debug)]
pub enum Expression {
    ColName(String),
    Const(Data),
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

impl Expression {
    pub fn result(&self, tuple: &Tuple, columns: &Vec<String>) -> Data {
        match self {
            Expression::Empty => Data::Null,
            Expression::ColName(colname)=> {
                for (idx, column) in columns.iter().enumerate() {
                    if *column == *colname {
                        return tuple.get(idx).unwrap().clone();
                    }
                }
                return Data::Null;
            },
            Expression::Const(value) => value.clone(),
            Expression::Func1(operator, opr1) => { 
                let value_opr1 = opr1.result(tuple, columns);
                return match operator {
                    // Logic implementation
                    Expression1Type::Not => !value_opr1,
                    Expression1Type::Negate => -value_opr1
                }
            },
            Expression::Func2(operator, opr1, opr2) => {
                let value_opr1 = opr1.result(tuple, columns);
                let value_opr2 = opr2.result(tuple, columns);
                return match operator {
                    // Logic implementation
                    Expression2Type::And => Data::Boolean(value_opr1.and(&value_opr2)),
                    Expression2Type::Or => Data::Boolean(value_opr1.or(&value_opr2)),

                    // Comparison implementation
                    Expression2Type::GreatherOrEqual => Data::Boolean(value_opr1 >= value_opr2),
                    Expression2Type::GreatherThan => Data::Boolean(value_opr1 > value_opr2),
                    Expression2Type::LessOrEqual => Data::Boolean(value_opr1 <= value_opr2),
                    Expression2Type::LessThan => Data::Boolean(value_opr1 < value_opr2),
                    Expression2Type::Equal => Data::Boolean(value_opr1 == value_opr2),
                    Expression2Type::NotEqual => Data::Boolean(value_opr1 != value_opr2),

                    // Aritmetic implementation
                    Expression2Type::Add => value_opr1 + value_opr2, 
                    Expression2Type::Sub => value_opr1 - value_opr2,
                    Expression2Type::Mul => value_opr1 * value_opr2,
                    Expression2Type::Div => value_opr1 / value_opr2
                }
            }
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str_ref = match self {
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
        };
        write!(f, "{}", str_ref)
    }
}
