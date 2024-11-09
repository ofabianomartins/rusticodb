
#[derive(Debug)]
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

impl PartialEq for Database {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Database {}
