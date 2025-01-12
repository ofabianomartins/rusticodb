
use crate::machine::Table;
use crate::machine::Machine;
use crate::machine::ResultSet;
use crate::machine::ResultSetType;
use crate::machine::drop_columns;
use crate::machine::drop_table_ref;
use crate::machine::check_table_exists;

use crate::storage::os_interface::OsInterface;

use crate::utils::execution_error::ExecutionError;

pub fn drop_table(machine: &mut Machine, table: &Table, if_exists: bool) -> Result<ResultSet, ExecutionError>{
    if check_table_exists(machine, table) == false && if_exists {
        return Ok(
            ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE"))
        );
    }
    if check_table_exists(machine, table) == false {
        return Err(ExecutionError::TableNotExists(table.database_name.to_string()));
    }

    drop_columns(machine, table);
    drop_table_ref(machine, table);

    OsInterface::destroy_file(
        &machine.pager.format_table_name(&table.database_name, &table.name)
    );

    Ok(ResultSet::new_command(ResultSetType::Change, String::from("DROP TABLE")))
}
