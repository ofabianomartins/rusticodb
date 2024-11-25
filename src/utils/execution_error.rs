
#[derive(Debug)]
pub enum ExecutionError {
    ParserError(String),
    TokenizerError(String),
    RecursionLimitExceeded,

    NoneExists,
    WrongFormat,
    WrongLength,
    StringParseFailed,

    DatabaseNotExists(String),
    DatabaseExists(String),
    DatabaseNotSetted, 
    TableNotExists(String),
    TableExists(String),
    ColumnNotExists(String),
    TupleNotExists(usize),
    NotImplementedYet
}
