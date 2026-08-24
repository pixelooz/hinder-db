/// The keywords and symbols the front-end of the database recognises consequently supports.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    From,
    As,
    Where,
    And,
    Or,
    Create,
    Table,
    Database,
    Databases,
    Use,
    Show,
    Index,
    Unique,
    On,
    Commit,
    Begin,
    Transaction,
    Rollback,

    Count,
    Avg,

    Offset,
    Limit,

    Order,
    By,
    Group,

    Asc,
    Desc,

    Inner,
    Left,
    Right,
    Join,

    // Data Type
    BigIntType,
    IntType,
    BooleanType,
    VarcharType,

    // Identifiers & Literals
    StringLit(String),
    Ident(String),
    IntLit(i64),
    BoolLit(bool),

    // Symbols & Operators
    Asterisk,
    Comma,
    Dot,
    LParen,
    RParen,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte, // >=
    Lte, // <=
    Semicolon,

    // End of file/input-stream
    Eof,
}
