pub mod database;
pub mod table;
pub mod column;
pub mod result_set;
pub mod condition;
pub mod raw_val;

pub use self::table::Table;
pub use self::column::{ Column, ColumnType };

use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::DataType;
use sqlparser::ast::SequenceOptions;

use crate::config::Config;
use crate::storage::pager::Pager;
use crate::storage::os_interface::OsInterface;
use crate::storage::tuple::Tuple;
use crate::machine::result_set::ResultSet;
use crate::machine::result_set::ResultSetType;
use crate::machine::raw_val::RawVal;
use crate::machine::condition::Condition;
use crate::machine::condition::Condition2Type;
use crate::utils::logger::Logger;
use crate::utils::execution_error::ExecutionError;

fn get_columns_table_definition() -> Vec<Column> {
    return vec![
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("id"),
            ColumnType::UnsignedBigint,
            true,
            true,
            true
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("database_name"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("table_name"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("name"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("type"),
            ColumnType::Varchar,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("not_null"),
            ColumnType::Boolean,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("primary_key"),
            ColumnType::Boolean,
            true,
            false,
            false
        ),
        Column::new(
            String::from("rusitcodb"),
            String::from("columns"),
            String::from("unique"),
            ColumnType::Boolean,
            true,
            false,
            false

        ),
    ];
}

#[derive(Debug)]
pub struct Machine { 
    pub pager: Pager,
    pub actual_database: Option<String>
}

impl Machine {
    pub fn new(pager: Pager) -> Self {
        Self { pager, actual_database: None }
    }

    pub fn database_exists(&mut self, database_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_database_name(database_name));
    }

    pub fn table_exists(&mut self, database_name: &String, table_name: &String) -> bool{
        return OsInterface::path_exists(&self.pager.format_table_name(database_name, table_name));
    }

    pub fn set_actual_database(&mut self, name: String) -> Result<ResultSet, ExecutionError> {
        if self.check_database_exists(&name) == false {
            return Err(ExecutionError::DatabaseNotExists(name));
        }
        self.actual_database = Some(name);
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("USE DATABASE")))
    }

    pub fn create_database(&mut self, database_name: String, if_not_exists: bool) -> Result<ResultSet, ExecutionError>{
        if self.check_database_exists(&database_name) && if_not_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")));
        }
        if self.check_database_exists(&database_name) {
            return Err(ExecutionError::DatabaseExists(database_name));
        }
        OsInterface::create_folder(&self.pager.format_database_name(&database_name));

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_unsigned_bigint(1u64);
        tuple.push_string(&database_name);
        tuples.push(tuple);

        let table = Table::new(Config::system_database(), Config::system_database_table_databases());

        self.insert_tuples(&table, &mut tuples);

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE DATABASE")))
    }

    pub fn drop_database(&mut self, database_name: String, if_exists: bool) -> Result<ResultSet, ExecutionError>{
        if self.check_database_exists(&database_name) == false && if_exists {
            return Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")));
        }
        if self.check_database_exists(&database_name) == false {
            return Err(ExecutionError::DatabaseNotExists(database_name));
        }

        for table in self.get_tables(&database_name) {
            self.drop_columns(&table);
            self.drop_table_ref(&table);
        }
        self.drop_database_ref(&database_name);

        OsInterface::destroy_folder(&self.pager.format_database_name(&database_name));

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP DATABASE")))
    }

    pub fn create_table(
        &mut self, 
        table: &Table, 
        if_not_exists: bool,
        columns: Vec<ColumnDef>
    ) -> Result<ResultSet, ExecutionError>{
        if self.check_table_exists(&table) && if_not_exists {
            return Ok(
                ResultSet::new_command(
                    ResultSetType::Change, String::from("CREATE TABLE")
                )
            );
        }
        if self.check_table_exists(&table) {
            return Err(ExecutionError::DatabaseExists(table.database_name.to_string()));
        }
        OsInterface::create_file(
            &self.pager.format_table_name(&table.database_name, &table.name)
        );

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_unsigned_bigint(1u64);
        tuple.push_string(&table.database_name);
        tuple.push_string(&table.name);
        tuples.push(tuple);

        let table_tables = Table::new(
            Config::system_database(),
            Config::system_database_table_tables()
        );

        self.insert_tuples(&table_tables, &mut tuples);

        for column in columns.iter() {
            let mut type_column: String = String::from("");
            let mut notnull_column: bool = false;
            let mut unique_column: bool = false;
            let mut primary_key_column: bool = false;

            match column.data_type {
                DataType::BigInt(None) => { type_column = String::from("BIGINT") },
                DataType::Integer(None) => { type_column = String::from("INTEGER") },
                DataType::Varchar(None) => { type_column = String::from("VARCHAR") }
                _ => {}
            }

            for option in &column.options {
                match option.option {
                    ColumnOption::NotNull => { notnull_column = true }
                    ColumnOption::Unique { is_primary, ..} => {
                        notnull_column = true;
                        unique_column = true;
                        primary_key_column = is_primary;
                    }
                    _ => {}
                }
            }

            let mut tuples: Vec<Tuple> = Vec::new();
            let mut tuple: Tuple = Tuple::new();
            tuple.push_unsigned_bigint(1u64);
            tuple.push_string(&table.database_name);
            tuple.push_string(&table.name);
            tuple.push_string(&column.name.to_string());
            tuple.push_string(&type_column);
            tuple.push_boolean(notnull_column);
            tuple.push_boolean(unique_column);
            tuple.push_boolean(primary_key_column);
            tuples.push(tuple);

            let table_columns = Table::new(
                Config::system_database(),
                Config::system_database_table_columns()
            );

            self.insert_tuples(&table_columns, &mut tuples);

            if primary_key_column {
                let mut tuples: Vec<Tuple> = Vec::new();
                let mut tuple: Tuple = Tuple::new();
                tuple.push_unsigned_bigint(1u64);
                tuple.push_string(&table.database_name);
                tuple.push_string(&table.name);
                tuple.push_string(&column.name.to_string());
                tuple.push_string(
                    &format!(
                        "{}_{}_{}_primary_key",
                        table.database_name,
                        table.name,
                        column.name.to_string()
                    )
                );
                tuple.push_unsigned_bigint(1u64);
                tuples.push(tuple);

                let table_sequences = Table::new(
                    Config::system_database(),
                    Config::system_database_table_sequences()
                );

                self.insert_tuples(&table_sequences, &mut tuples);
            }
        }
        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE TABLE")))
    }

    pub fn drop_table(&mut self, table: &Table, if_exists: bool) -> Result<ResultSet, ExecutionError>{
        if self.check_table_exists(table) == false && if_exists {
            return Ok(
                ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE"))
            );
        }
        if self.check_table_exists(table) == false {
            return Err(ExecutionError::TableNotExists(table.database_name.to_string()));
        }

        self.drop_columns(table);
        self.drop_table_ref(table);

        OsInterface::destroy_file(
            &self.pager.format_table_name(&table.database_name, &table.name)
        );

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")))
    }

    pub fn create_sequence(
        &mut self, 
        database_name: &String, 
        table_name: &String,
        column_name: &String,
        sequence_name: &String,
        _data_type: Option<DataType>,
        _if_not_exists: bool,
        _sequence_options: Vec<SequenceOptions>
    ) -> Result<ResultSet, ExecutionError>{

        let mut tuples: Vec<Tuple> = Vec::new();
        let mut tuple: Tuple = Tuple::new();
        tuple.push_unsigned_bigint(1);
        tuple.push_string(&database_name);
        tuple.push_string(&table_name);
        tuple.push_string(&column_name);
        tuple.push_string(&sequence_name);
        tuple.push_unsigned_bigint(1u64);
        tuples.push(tuple);

        let table = Table::new(
            Config::system_database(),
            Config::system_database_table_sequences()
        );

        self.insert_tuples(&table, &mut tuples);

        Ok(ResultSet::new_command(ResultSetType::Change, String::from("CREATE SEQUENCE")))
    }

    pub fn check_database_exists(&mut self, database_name: &String) -> bool {
        let table_databases = Table::new(
            Config::system_database(),
            Config::system_database_table_databases()
        );

        let condition = Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("name"))),
            Box::new(Condition::Const(RawVal::Str(database_name.clone())))
        );

        let columns = self.get_columns(&table_databases);
        let tuples: Vec<Tuple> = self.read_tuples(&table_databases)
            .into_iter()
            .filter(|tuple| condition.evaluate(tuple, &columns))
            .collect();

        return tuples.len() > 0;
    }

    pub fn check_table_exists(&mut self, table: &Table) -> bool {
        let tables: Vec<Table> = self.get_tables(&table.database_name)
            .into_iter()
            .filter(|tuple| tuple.name == table.name)
            .collect();

        return tables.len() > 0;
    }

    pub fn get_sequence_next_id(&mut self, column: &Column) -> Option<u64> {
        let table_sequences = Table::new(
            Config::system_database(),
            Config::system_database_table_sequences()
        );
        let condition = Condition::Func2(
            Condition2Type::And,
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("database_name"))),
                Box::new(Condition::Const(RawVal::Str(column.database_name.clone())))
            )),
            Box::new(Condition::Func2(
                Condition2Type::And,
                Box::new(Condition::Func2(
                    Condition2Type::Equal,
                    Box::new(Condition::ColName(String::from("table_name"))),
                    Box::new(Condition::Const(RawVal::Str(column.table_name.clone())))
                )),
                Box::new(Condition::Func2(
                    Condition2Type::Equal,
                    Box::new(Condition::ColName(String::from("column_name"))),
                    Box::new(Condition::Const(RawVal::Str(column.name.clone())))
                ))
            ))
        );

        let columns = self.get_columns(&table_sequences);

        let tuples: Vec<Tuple> = self.read_tuples(&table_sequences)
            .into_iter()
            .filter(|tuple| condition.evaluate(tuple, &columns))
            .collect();

        let mut next_id: Option<u64> = None;

        for elem in tuples.into_iter() {
            let mut new_elem = elem.clone();
            let next_id_value = elem.get_unsigned_bigint(5).unwrap();

            new_elem.push_unsigned_bigint(next_id_value);
            self.update_tuples(&table_sequences, &mut  vec![new_elem]);
            next_id = Some(elem.get_unsigned_bigint(5).unwrap());

            break;
        }

        return next_id;
    }

    pub fn get_tables(&mut self, database_name: &String) -> Vec<Table> {
        let mut tables: Vec<Table> = Vec::new();

        let table_tables = Table::new(
            Config::system_database(),
            Config::system_database_table_tables()
        );

        let condition = Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("database_name"))),
            Box::new(Condition::Const(RawVal::Str(database_name.clone())))
        );

        let columns = self.get_columns(&table_tables);

        let tuples: Vec<Tuple> = self.read_tuples(&table_tables)
            .into_iter()
            .filter(|tuple| condition.evaluate(tuple, &columns))
            .collect();

        for elem in tuples.into_iter() {
            tables.push(
                Table::new_with_alias(
                    database_name.clone(),
                    database_name.clone(),
                    elem.get_string(2).unwrap(),
                    elem.get_string(2).unwrap()
                )
            );
        }

        return tables;
    }

    pub fn drop_database_ref(&mut self, database_name: &String) {
        let table_databases = Table::new(
            Config::system_database(),
            Config::system_database_table_databases()
        );

        let columns = self.get_columns(&table_databases);

        let condition = Condition::Func2(
            Condition2Type::Equal,
            Box::new(Condition::ColName(String::from("name"))),
            Box::new(Condition::Const(RawVal::Str(database_name.clone())))
        );

        self.drop_tuples(&table_databases, columns, &condition);
    }

    pub fn drop_table_ref(&mut self, table: &Table) {
        let table_tables = Table::new(
            Config::system_database(),
            Config::system_database_table_tables()
        );

        let columns = self.get_columns(&table_tables);

        let condition = Condition::Func2(
            Condition2Type::And,
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("database_name"))),
                Box::new(Condition::Const(RawVal::Str(table.database_name.clone())))
            )),
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("name"))),
                Box::new(Condition::Const(RawVal::Str(table.name.clone())))
            ))
        );

        self.drop_tuples(&table_tables, columns, &condition);
    }

    pub fn get_columns(&mut self, table: &Table) -> Vec<Column> {
        let table_columns = Table::new(
            Config::system_database(),
            Config::system_database_table_columns()
        );

        let condition = Condition::Func2(
            Condition2Type::And,
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("database_name"))),
                Box::new(Condition::Const(RawVal::Str(table.database_name.clone())))
            )),
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("table_name"))),
                Box::new(Condition::Const(RawVal::Str(table.name.clone())))
            ))
        );

        let tuples: Vec<Tuple> = self.read_tuples(&table_columns)
            .into_iter()
            .filter(|tuple| condition.evaluate(tuple, &get_columns_table_definition()))
            .collect();

        let mut columns: Vec<Column> = Vec::new();

        for elem in tuples.into_iter() {
            columns.push(
                Column::new_with_alias(
                    table.database_name.clone(),
                    table.database_alias.clone(),
                    table.name.clone(),
                    table.alias.clone(),
                    elem.get_string(3).unwrap(),
                    elem.get_string(3).unwrap(),
                    ColumnType::Varchar,
                    elem.get_boolean(5).unwrap(),
                    elem.get_boolean(6).unwrap(),
                    elem.get_boolean(7).unwrap(),
                )
            );
        }

        return columns;
    }

    pub fn drop_columns(&mut self, table: &Table) {
        let table_columns = Table::new(
            Config::system_database(),
            Config::system_database_table_columns()
        );

        let condition = Condition::Func2(
            Condition2Type::And,
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("database_name"))),
                Box::new(Condition::Const(RawVal::Str(table.database_name.clone())))
            )),
            Box::new(Condition::Func2(
                Condition2Type::Equal,
                Box::new(Condition::ColName(String::from("table_name"))),
                Box::new(Condition::Const(RawVal::Str(table.name.clone())))
            ))
        );

        self.drop_tuples(&table_columns, get_columns_table_definition(), &condition);
    }

    pub fn insert_row(
        &mut self,
        table: &Table,
        columns: &Vec<Column>,
        tuples: &mut Vec<Tuple>
    ) -> Result<ResultSet, ExecutionError>{

        let table_columns = self.get_columns(table);

        if let Err(error) = self.validate_columns(columns, &table_columns) {
            return Err(error);
        }

        let adjusted_tuples_result = self.adjust_tuples(table, columns, tuples);
        if let Err(error) = adjusted_tuples_result {
            return Err(error);
        }

        let mut adjusted_tuples = adjusted_tuples_result.unwrap();

        self.pager.insert_tuples(&table.database_name, &table.name, &mut adjusted_tuples);
        self.pager.flush_page(&table.database_name, &table.name);

        return Ok(ResultSet::new_command(ResultSetType::Change, String::from("INSERT")))
    }

    pub fn validate_columns(&mut self, _columns: &Vec<Column>, _table_columns: &Vec<Column>) -> Result<bool, ExecutionError> {
        return Ok(true);
    }

    fn adjust_tuples(
        &mut self,
        table: &Table,
        columns: &Vec<Column>,
        tuples: &mut Vec<Tuple>
    ) -> Result<Vec<Tuple>, ExecutionError> {
        let table_columns = self.get_columns(table);

        let new_tuples: Vec<Tuple> = tuples.iter_mut()
            .map(|tuple| { 
                let mut new_tuple = Tuple::new();

                for column in &table_columns {
                    let index_result = columns.iter().position(|e| e == column);
                    if let Some(index) = index_result {
                       new_tuple.append_cell(tuple.get_cell(index as u16));
                    } else {
                        if column.primary_key == true {
                            if let Some(next_id) = self.get_sequence_next_id(column) {
                                new_tuple.push_unsigned_bigint(next_id);
                            }
                        }
                    }
                }

                new_tuple
            })
            .collect::<Vec<_>>();

        return Ok(new_tuples);
    }

    pub fn insert_tuples(&mut self, table: &Table, tuples: &mut Vec<Tuple>) {
        self.pager.insert_tuples(&table.database_name, &table.name, tuples);
        self.pager.flush_page(&table.database_name, &table.name);
    }

    pub fn update_tuples(&mut self, table: &Table, tuples: &mut Vec<Tuple>) {
        self.pager.update_tuples(&table.database_name, &table.name, tuples);
        self.pager.flush_page(&table.database_name, &table.name);
    }

    pub fn drop_tuples(&mut self, table: &Table, columns: Vec<Column>, condition: &Condition) {
        let mut tuples: Vec<Tuple> = self.pager.read_tuples(&table.database_name, &table.name)
            .into_iter()
            .filter(|tuple| !condition.evaluate(tuple, &columns))
            .collect();

        self.pager.update_tuples(&table.database_name, &table.name, &mut tuples);
        self.pager.flush_page(&table.database_name, &table.name);
    }

    pub fn read_tuples(&mut self, table: &Table) -> Vec<Tuple> {
        Logger::debug(format!("Reading ({}, {})", table.database_name, table.name).leak());
        return self.pager.read_tuples(&table.database_name, &table.name)
    }

    pub fn product_cartesian(&mut self, tables: Vec<Table>) -> ResultSet {
        let mut result_set = ResultSet::new_empty();
        
        for (_dx, table) in tables.iter().enumerate() {
            let columns1 = self.get_columns(&table);
            let tuples1: Vec<Tuple> = self.read_tuples(&table);
            let result_set1 = ResultSet::new_select(columns1, tuples1);

            result_set = result_set.cartesian_product(&result_set1);
        }

        return result_set;
    }


}
