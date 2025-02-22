
use crate::machine::Column;
use crate::machine::Expression;

#[derive(Debug)]
pub struct Attribution {
    pub target: Column,
    pub expr: Expression
}

impl Attribution {

    pub fn new(target: Column, expr: Expression) -> Self {
        Self { target, expr }
    }

}
