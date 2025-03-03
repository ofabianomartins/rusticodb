use sqlparser::ast::ObjectType;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
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
use crate::machine::result_set::ResultSet;
use crate::utils::ExecutionError;


pub struct SqlExecutor {
    pub machine: Machine
}

impl SqlExecutor {

    pub fn new(machine: Machine) -> Self {
        SqlExecutor { machine }
    }

    pub fn get_database_name(&self) -> String {
        match &self.machine.actual_database {
            Some(database_name) => database_name.clone(),
            None => String::from("<no-database>")
        }
    }

    pub fn parse_command(&mut self, sql_command: &str) -> Result<Vec<ResultSet>, ExecutionError> { 
        let dialect = PostgreSqlDialect {};

        match Parser::parse_sql(&dialect, sql_command) {
            Ok(commands) => {
                let mut result_sets: Vec<ResultSet> = Vec::new();
                for command in commands {
                    match self.process_command(command) {
                        Ok(result_set) => {
                            result_sets.push(result_set);
                        },
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }

                return Ok(result_sets);
            },
            Err(ParserError::ParserError(err)) => Err(ExecutionError::ParserError(err)),
            Err(ParserError::TokenizerError(err)) => Err(ExecutionError::TokenizerError(err)),
            Err(ParserError::RecursionLimitExceeded) => Err(ExecutionError::RecursionLimitExceeded)
        }
    }

    pub fn process_command(&mut self, statement: Statement) -> Result<ResultSet, ExecutionError> { 
        match statement {
            Statement::Use(statement) => use_database(&mut self.machine, statement),
            Statement::CreateDatabase { db_name, if_not_exists, .. } => {
                create_database(&mut self.machine, db_name.to_string(), if_not_exists)
            },
            Statement::CreateTable(statement) => create_table(&mut self.machine, statement),
            Statement::CreateSequence { name, data_type, sequence_options, owned_by, if_not_exists, .. } => { 
                create_sequence(
                    &mut self.machine,
                    name,
                    data_type,
                    owned_by,
                    if_not_exists,
                    sequence_options
                )
            },
            Statement::CreateIndex(statement) => create_index(&mut self.machine, statement),
            Statement::CreateView { name, query, if_not_exists, or_replace, .. } => {
                create_view(
                    &mut self.machine,
                    &name.to_string(),
                    query,
                    or_replace,
                    if_not_exists
                )
            },
            Statement::Drop { object_type, if_exists, names, .. } => {
                match object_type {
                    ObjectType::Database => drop_database(&mut self.machine, names, if_exists),
                    ObjectType::Table => drop_table(&mut self.machine, names, if_exists),
                    ObjectType::Index => drop_index(&mut self.machine, names, if_exists),
                    ObjectType::Sequence => drop_sequence(&mut self.machine, names, if_exists),
                    value => { 
                        println!("DROP {:?}", value);
                        Err(ExecutionError::NotImplementedYet)
                    }
                }
            },
            Statement::Delete(statement) => delete(&mut self.machine, statement),
            Statement::Insert(statement) => insert(&mut self.machine, statement),
            Statement::Update { table, assignments, selection, returning, .. }  => { 
                update(
                    &mut self.machine, 
                    table,
                    assignments,
                    selection,
                    returning
                )
            },
            Statement::Query(statement) => query(&mut self.machine, statement),
            Statement::ShowDatabases { .. } => show_databases(&mut self.machine),
            Statement::ShowTables { .. } => show_tables(&mut self.machine),
            value => { 
                println!("{:?}", value);
                Err(ExecutionError::NotImplementedYet)
            }
        }
    }

}
