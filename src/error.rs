use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("caught me with my pants down huh...well {0}")]
    NotImplementedYet(String),

    #[error("page {0} not found in store")]
    PageNotFound(u64),

    #[error("page is full, cannot insert cell")]
    PageFull,

    #[error("tuple size {0} exceeds maximum allowed size")]
    TupleTooLarge(usize),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt page detected: {0}")]
    CorruptPage(String),

    #[error("replacer either empty, or every tracked page currently pinned")]
    LruEviction,

    #[error("key already exists, cannot insert again: key={0}")]
    DuplicateKey(u64),

    #[error("key does not exists: key={0}")]
    KeyNotFound(u64),

    #[error("Parse Error: {}", 0)]
    ParseErr(String),

    #[error("Syntax Error: {}", 0)]
    SyntaxErr(String),

    #[error("column not found: column_name={}", 0)]
    ColumnNotFound(String),

    #[error("DuplicatesNotAllowed: {}", 0)]
    Duplicate(String),

    #[error("IndexConstraint: {}", 0)]
    ConstraintViolation(String),

    #[error("requested table was not found: table_name={0}")]
    TableNotFound(String),

    #[error("InvalidAction: {}", 0)]
    ActionNotAllowed(String),
}
