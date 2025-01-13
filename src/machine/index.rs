use std::fmt;

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub alias: String
}

impl Index {

    pub fn new(name: String) -> Self {
        Index { name: name.clone(), alias: name }
    }

    pub fn new_with_alias(name: String, alias: String) -> Self {
        Index { name, alias }
    }
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Index {}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Index({}, {})", self.name, self.alias)
    }
}
