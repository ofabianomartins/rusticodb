use crate::config::Config;

pub fn format_table_name(database_name: &String, table_name: &String) -> String{
    return format!("{}/{}/{}.db", Config::data_folder(), database_name, table_name);
}
