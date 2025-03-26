use sqlparser::ast::Expr;
use sqlparser::ast::Assignment;
use sqlparser::ast::TableWithJoins;
use sqlparser::ast::SelectItem;
use sqlparser::ast::AssignmentTarget;

use crate::machine::Machine;
use crate::machine::Attribution;
use crate::machine::Column;
use crate::machine::ColumnType;
use crate::machine::Table;
use crate::machine::update_row;

use crate::storage::Expression;
use crate::storage::RawVal;
use crate::storage::ResultSet;

use crate::utils::ExecutionError;

fn get_attributions(
    db_name: &String,
    table_name: &String,
    assignments: Vec<Assignment>
) -> Vec<Attribution> {
    let mut attributions: Vec<Attribution> = Vec::new();

    for assignment in assignments {
        match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                let column = Column::new(
                    0u64,
                    db_name.clone(),
                    table_name.clone(),
                    name.to_string(),
                    ColumnType::Undefined,
                    false,
                    false,
                    false,
                    String::from("")
                );
                let expression = Expression::Const(RawVal::Str(assignment.value.to_string()));
                let attribution = Attribution::new(column, expression);
                attributions.push(attribution);
            },
            AssignmentTarget::Tuple(name) => {
                println!("2 {:?}", name);
            }
        }
    }

    return attributions; 
}

pub fn update(
    machine: &mut Machine,
    table_with_joins: TableWithJoins,
    assignments: Vec<Assignment>,
    _selection: Option<Expr>,
    _returning: Option<Vec<SelectItem>>
) -> Result<ResultSet, ExecutionError> { 
    if let Some(db_name) = machine.actual_database.clone() {
        let table_name = table_with_joins.to_string();
        let table = Table::new(db_name.clone(), table_name.clone());

        let attributions = get_attributions(&db_name, &table_name, assignments);

        return update_row(
           machine,
           &table,
           &attributions,
           Expression::Const(RawVal::Str(String::from("fabiano")))
        );
    } else {
        return Err(ExecutionError::DatabaseNotSetted);
    }
}

