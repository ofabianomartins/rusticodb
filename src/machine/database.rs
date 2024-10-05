
pub struct Database {
    name: String
}

impl Database {

    pub fn new(name: String) -> Self {
        Database { name } 
    }

    pub fn check_name(&self, other_name: &String) -> bool {
        return self.name == *other_name; 
    }

}
