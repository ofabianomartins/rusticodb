use std::fmt;

#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,
    pub alias: String
}

impl Sequence {

    pub fn new(name: String) -> Self {
        Sequence { name: name.clone(), alias: name }
    }

    pub fn new_with_alias(name: String, alias: String) -> Self {
        Sequence { name, alias }
    }
}

impl PartialEq for Sequence {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Sequence {}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Sequence({}, {})", self.name, self.alias)
    }
}
