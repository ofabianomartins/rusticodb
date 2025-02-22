
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

    SequenceNotExists(String),
    SequenceExists(String),

    IndexNotExists(String),
    IndexExists(String),

    ViewNotExists(String),
    ViewExists(String),

    ColumnNotExists(String),
    ColumnCantBeNull(String, String, String),
    ColumnTypeNotMatch(String, String, String),

    WrongTupleSize(usize, usize),
    TupleNotExists(usize),
    FailedUpdateTuples,

    NotImplementedYet
}
