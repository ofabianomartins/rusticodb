
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
    ColumnCantBeNull(String, String, String),

    WrongTupleSize(usize, usize),
    TupleNotExists(usize),

    NotImplementedYet
}
