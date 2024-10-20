
#[derive(Debug)]
pub struct ResultSet {

}

#[derive(Debug)]
pub enum ExecutionError {
    ParserError(String),
    TokenizerError(String),
    RecursionLimitExceeded,
    DatabaseNotExists(String),
    DatabaseExists(String),
    DatabaseNotSetted
}
