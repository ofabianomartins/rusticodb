
use sqlparser::ast::Query;

use crate::machine::machine::Machine;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ExecutionError;

use crate::parser::parse_query::parse_query;

pub fn query(machine: &mut Machine, query: Box<Query>) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.context.actual_database.clone() {
        let query_data = parse_query(query).unwrap();
        let tuples = machine.read_tuples(&db_name, &query_data.table);
        return Ok(ResultSet::new_select(query_data.select, tuples))
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

