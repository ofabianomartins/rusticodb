use failure::Fail;

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

#[derive(Fail, Debug)]
pub enum QueryError {
    #[fail(display = "Failed to parse query. Chars remaining: {}", _0)]
    SytaxErrorCharsRemaining(String),
    #[fail(display = "Failed to parse query. Bytes remaining: {:?}", _0)]
    SyntaxErrorBytesRemaining(Vec<u8>),
    #[fail(display = "Failed to parse query: {}", _0)]
    ParseError(String),
    // #[fail(display = "Some assumption was violated. This is a bug: {}", _0)]
    // FatalError(String, Backtrace),
    #[fail(display = "Not implemented: {}", _0)]
    NotImplemented(String),
    #[fail(display = "Type error: {}", _0)]
    TypeError(String),
    #[fail(display = "Overflow or division by zero")]
    Overflow,
}
