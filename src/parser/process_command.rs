
use sqlparser::ast::ObjectType;
use sqlparser::ast::Statement;

use crate::parser::use_database::use_database;
use crate::parser::show_databases::show_databases;
use crate::parser::show_tables::show_tables;

use crate::parser::create_database::create_database;
use crate::parser::drop_database::drop_database;

use crate::parser::create_table::create_table;
use crate::parser::drop_table::drop_table;

use crate::parser::create_sequence::create_sequence;
use crate::parser::drop_sequence::drop_sequence;

use crate::parser::create_index::create_index;
use crate::parser::drop_index::drop_index;

use crate::parser::create_view::create_view;

use crate::parser::query::query;

use crate::parser::insert::insert;
use crate::parser::update::update;
use crate::parser::delete::delete;

use crate::machine::Machine;
use crate::storage::ResultSet;
use crate::utils::ExecutionError;

pub fn process_command(machine: &mut Machine, statement: Statement) -> Result<ResultSet, ExecutionError> { 
    match statement {
        Statement::Use(statement) => use_database(machine, statement),
        Statement::CreateDatabase { db_name, if_not_exists, .. } => {
            create_database(machine, db_name.to_string(), if_not_exists)
        },
        Statement::CreateTable(statement) => create_table(machine, statement),
        Statement::CreateSequence { name, data_type, sequence_options, owned_by, if_not_exists, .. } => { 
            create_sequence(machine, name, data_type, owned_by, if_not_exists, sequence_options)
        },
        Statement::CreateIndex(statement) => create_index(machine, statement),
        Statement::CreateView { name, query, if_not_exists, or_replace, .. } => {
            create_view(machine, &name.to_string(),query, or_replace, if_not_exists)
        },
        Statement::Drop { object_type: ObjectType::Database, if_exists, names, .. } => {
            drop_database(machine, names, if_exists)
        },
        Statement::Drop { object_type: ObjectType::Table, if_exists, names, .. } => {
            drop_table(machine, names, if_exists)
        },
        Statement::Drop { object_type: ObjectType::Index, if_exists, names, .. } => {
            drop_index(machine, names, if_exists)
        },
        Statement::Drop { object_type: ObjectType::Sequence, if_exists, names, .. } => {
            drop_sequence(machine, names, if_exists)
        },
        Statement::Delete(statement) => delete(machine, statement),
        Statement::Insert(statement) => insert(machine, statement),
        Statement::Update { table, assignments, selection, returning, .. }  => { 
            update(machine, table, assignments, selection, returning)
        },
        Statement::Query(statement) => query(machine, statement),
        Statement::ShowDatabases { .. } => show_databases(machine),
        Statement::ShowTables { .. } => show_tables(machine),
        value => { 
            println!("not implemented yet {:?}", value);
            Err(ExecutionError::NotImplementedYet)
        }
    }
}
