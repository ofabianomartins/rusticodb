use crate::config::Config;

pub fn format_database_name(database_name: &String) -> String{
    return format!("{}/{}", Config::data_folder(), database_name);
}
