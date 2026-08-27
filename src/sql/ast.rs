use crate::{relation::types::DataType, sql::parser::AstLiteral};

// TODO: After writing the binder layer see if the 'Strings' can be used as '&str'

/// The top-level root of any parsed SQL query; by returning this enum the parser
/// guarantees, the execution engine knows exactly what type of command to route.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),

    Commit,
    Begin,
    Rollback,

    CreateTable(CreateTable),
    CreateIndex(CreateIndex),

    UseDatabase(String),

    DropTable(String),
    ShowTables,
    ShowIndexes,
}

/// Components of a SELECT statement and its derivatives.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub select_list: Vec<DerivedColumn>,
    pub from: Option<TableReference>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<ColumnReference>,
    pub order_by: Vec<SortSpecification>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Components of a INSERT statement, can also handle expressions.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

/// Components of a UPDATE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

/// Association between a column and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column_name: String,
    pub value: Expr,
}

/// Components of a DELETE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

/// Components of a CREATE TABLE statement and its derivatives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

/// The actual definition of a column with its name and **[DataType]**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    /// Currently used for VARCHAR(n), n being the length.
    pub length: Option<u32>,
    pub is_primary_key: bool,
}

/// Components of a CREATE INDEX statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndex {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub unique: bool,
}

/// Exhaustively defines where rows can be sourced from.
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    BaseTable { name: String, alias: Option<String> },
    // Boxed joins because JOINs are recursive (e.g., Table A JOIN Table B JOIN Table C).
    // This is a general pattern to convince the compiler with a Sized type, and prevent
    // infinite sizing of the enum.
    Join(Box<QualifiedJoin>),
}

/// Represents a Join Type (Inner, Left, Right) with its components.
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedJoin {
    pub left: TableReference,
    pub right: TableReference,
    pub join_type: JoinType,
    pub condition: Expr,
}

/// Types of JOINs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// A recursive tree representing mathematical and boolean logic.
///
/// # Note For me
/// Equivalent of Expr in the compiler book. Later compare it
/// with the compiler's Expr and examine flow.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColumnReference),
    Literal(AstLiteral),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    Average(ColumnReference),
    Count(Option<ColumnReference>), // None represents COUNT(*).
}

/// All the supported binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
}

/// Information about the column in question along with the table it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReference {
    pub qualifier: Option<String>, // "users" in "users.id"
    pub column_name: String,       // "id"
}

/// Column derived after evaluating an expression.
#[derive(Debug, Clone, PartialEq)]
pub struct DerivedColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// Holds information about how to sort a dataset, and which one to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpecification {
    pub column: ColumnReference,
    pub descending: bool,
}
