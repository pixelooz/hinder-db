use std::mem;

/* TODO: Find out a better way to write tests instead of writing entire tree structure
for every statement being tested, like a harness or constructor or something. */

use crate::{
    error::Error,
    relation::types::DataType,
    sql::{
        ast::{
            Assignment, BinaryOperator, ColumnDefinition, ColumnReference, CreateIndex,
            CreateTable, Delete, DerivedColumn, Expr, Insert, JoinType, QualifiedJoin, Select,
            SortSpecification, Statement, TableReference, Update,
        },
        lexer::Lexer,
        token::Token,
    },
};

/// A Recursive Descent LL(1) parser. It lazily consumes the `Lexer` stream producing an
/// AST (an Abstract Syntax Tree) which is the structure that gets evaluated to call our
/// storage engine.
///
/// # Note for me
/// Modeled after the parser in the compiler, however toydb uses `Peekable`, so look into
/// it when we extend our front-end further later down the future.
#[derive(Debug)]
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    curr_token: Token,
    peek_token: Token,
}

/// Represents a raw literal value parsed directly from the SQL string.
/// The parses uses this raw representation while processing because
/// it hasn't yet consulted the catalog yet to know the actual storage
/// constraints.
///
/// I'm sleepy asf right now, if this doesn't work out I'm gonna loose
/// my mind.
#[derive(Debug, Clone, PartialEq)]
pub enum AstLiteral {
    String(String),
    Int(i64),
    Boolean(bool),
    Null,
}

impl<'a> Parser<'a> {
    /// Initializes the `Parser` with the provided lexer and loads the first two tokens.
    pub fn new(mut lexer: Lexer<'a>) -> Result<Self, Error> {
        let curr_token = lexer.next_token()?;
        let peek_token = lexer.next_token()?;
        Ok(Self {
            lexer,
            curr_token,
            peek_token,
        })
    }

    /// Advances the parser by one token safely without cloning.
    fn advance(&mut self) -> Result<(), Error> {
        let next = self.lexer.next_token()?;
        self.curr_token = mem::replace(&mut self.peek_token, next);
        Ok(())
    }

    /// Checks if the current token matches the expected variant.
    fn check(&self, expected: &Token) -> bool {
        mem::discriminant(&self.curr_token) == mem::discriminant(expected)
    }

    /// Checks if the peek token matches the expected variant.
    fn check_peek(&self, expected: &Token) -> bool {
        mem::discriminant(&self.peek_token) == mem::discriminant(expected)
    }

    /// If the current token matches the expected variant, advances the parser and
    /// returns true.
    fn match_token(&mut self, expected: &Token) -> Result<bool, Error> {
        if self.check(expected) {
            self.advance()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Asserts that the current token matches the expected variant and advances
    /// the parser.
    /// If it does not match, returns a strict, descriptive `SyntaxErr`.
    fn consume(&mut self, expected: &Token) -> Result<(), Error> {
        if self.check(expected) {
            self.advance()?;
            Ok(())
        } else {
            Err(Error::SyntaxErr(format!(
                "expected {:?}, found {:?}",
                expected, self.curr_token
            )))
        }
    }

    /// Parses an entire SQL script containing multiple semicolon-separated statements.
    /// Ignores empty statements (redundant semicolons).
    pub fn parse_script(&mut self) -> Result<Vec<Statement>, Error> {
        let mut statements = Vec::new();

        while !self.check(&Token::Eof) {
            // Consume trailing semicolons.
            if self.match_token(&Token::Semicolon)? {
                continue;
            }
            // Parse the actual statement.
            statements.push(self.parse_statement()?);
            // The semicolons are ignored for single statements so
            // we consume it manually here.
            self.match_token(&Token::Semicolon)?;
        }
        Ok(statements)
    }

    /// The root of the AST tree. Routes to specific statement parser.
    pub fn parse_statement(&mut self) -> Result<Statement, Error> {
        match self.curr_token {
            Token::Select => self.parse_select().map(Statement::Select),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            Token::Commit => {
                self.advance()?; // Consume 'COMMIT'
                self.match_token(&Token::Transaction)?;
                Ok(Statement::Commit)
            }
            Token::Begin => {
                self.advance()?;
                self.match_token(&Token::Transaction)?;
                Ok(Statement::Begin)
            }
            Token::Rollback => {
                self.advance()?;
                self.match_token(&Token::Transaction)?;
                Ok(Statement::Rollback)
            }
            Token::Create => {
                self.advance()?;
                match self.curr_token {
                    Token::Table => {
                        self.advance()?;
                        self.parse_create_table().map(Statement::CreateTable)
                    }
                    Token::Unique => {
                        self.advance()?;
                        self.consume(&Token::Index)?;
                        self.parse_create_unique_index().map(Statement::CreateIndex)
                    }
                    Token::Index => {
                        self.advance()?;
                        self.parse_create_index().map(Statement::CreateIndex)
                    }
                    Token::Database => {
                        self.advance()?;
                        let db_name = self.parse_identifier()?;
                        // CREATE DATABASE used as router after creation.
                        Ok(Statement::UseDatabase(db_name))
                    }
                    _ => Err(Error::SyntaxErr(format!(
                        "expected TABLE or INDEX after CREATE, found {:?}",
                        self.curr_token
                    ))),
                }
            }
            Token::Use => {
                self.advance()?;
                let db_name = self.parse_identifier()?;
                Ok(Statement::UseDatabase(db_name))
            }
            Token::Show => {
                self.advance()?;
                self.consume(&Token::Databases)?;
                Ok(Statement::ShowDatabases)
            }
            _ => Err(Error::SyntaxErr(format!(
                "unexpected token at start of statement: {:?}",
                self.curr_token
            ))),
        }
    }

    /// Extracts raw string data from an Identifier token.
    fn parse_identifier(&mut self) -> Result<String, Error> {
        match &self.curr_token {
            Token::Ident(name) => {
                let id = name.clone();
                self.advance()?;
                Ok(id)
            }
            _ => Err(Error::SyntaxErr(format!(
                "expected identifier, found {:?}",
                self.curr_token
            ))),
        }
    }

    /// Parses a complete SELECT query.
    ///
    /// ```sql
    /// SELECT col_a, col_b AS b, COUNT(*)
    /// FROM table_1 JOIN table_2 ON condition
    /// WHERE condition
    /// GROUP BY col_a
    /// ORDER BY col_b DESC
    /// LIMIT 10 OFFSET 5;
    /// ```
    fn parse_select(&mut self) -> Result<Select, Error> {
        self.consume(&Token::Select)?;
        let select_list = self.parse_select_list()?;

        let Ok(from) = self.parse_from_clause() else {
            return Err(Error::SyntaxErr(format!(
                "expected FROM clause or end of statement, found {:?}",
                self.curr_token
            )));
        };

        let where_clause = self.parse_where_clause()?;
        let group_by = self.parse_group_by()?;
        let order_by = self.parse_order_by()?;
        let (limit, offset) = self.parse_limit_offset()?;

        Ok(Select {
            select_list,
            from,
            where_clause,
            group_by,
            order_by,
            limit,
            offset,
        })
    }

    /// Parses the projection list of a SELECT query.
    ///
    /// ```sql
    /// SELECT *, col_name, table_name.col_name, COUNT(*), AVG(col) AS alias ...
    /// ```
    fn parse_select_list(&mut self) -> Result<Vec<DerivedColumn>, Error> {
        let mut select_list = Vec::new();

        // We represent SELECT * as an empty qualifier and "*" col name.
        if self.match_token(&Token::Asterisk)? {
            select_list.push(DerivedColumn {
                expr: Expr::Column(ColumnReference {
                    qualifier: None,
                    column_name: "*".into(),
                }),
                alias: None,
            });
            return Ok(select_list);
        }
        loop {
            let expr = self.parse_expressions()?;
            let mut alias = None;

            if let Token::Ident(name) = &self.curr_token {
                alias = Some(name.clone()); // Implicit alias 'SELECT col1 name From ...'
                self.advance()?;
            } else if self.match_token(&Token::As)? {
                alias = Some(self.parse_identifier()?);
            }
            select_list.push(DerivedColumn { expr, alias });
            if !self.match_token(&Token::Comma)? {
                break;
            }
        }
        Ok(select_list)
    }

    /// Parses the from clause and builds a left-deep join tree if applicable.
    ///
    /// ```sql
    /// SELECT ... FROM table_1 ...;
    /// SELECT ... FROM t1 JOIN t2 ON t1.id = t2.id ...;
    /// SELECT ... FROM t1 LEFT JOIN t2 ON ... RIGHT JOIN t3 ON ...;
    /// SELECT ... FROM user1 AS u1 LEFT JOIN user2 AS u2 ON ... RIGHT JOIN t3 ON ...;
    /// ```
    fn parse_from_clause(&mut self) -> Result<Option<TableReference>, Error> {
        self.consume(&Token::From)?;
        let base_table = self.parse_identifier()?;

        let base_alias = self
            .match_token(&Token::As)?
            .then(|| self.parse_identifier())
            .transpose()?;

        let mut curr_lhs = TableReference::BaseTable {
            name: base_table,
            alias: base_alias,
        };
        loop {
            let mut join_type = JoinType::Inner;
            match &self.curr_token {
                Token::Right => {
                    self.advance()?;
                    join_type = JoinType::Right;
                }
                Token::Inner => {
                    self.advance()?;
                }
                Token::Left => {
                    self.advance()?;
                    join_type = JoinType::Left;
                }
                _ => {}
            }
            if !self.match_token(&Token::Join)? {
                break;
            }
            let right_table = self.parse_identifier()?;

            let right_alias = self
                .match_token(&Token::As)?
                .then(|| self.parse_identifier())
                .transpose()?;

            let rhs = TableReference::BaseTable {
                name: right_table,
                alias: right_alias,
            };
            self.consume(&Token::On)?;
            let condition = self.parse_expressions()?;

            curr_lhs = TableReference::Join(Box::new(QualifiedJoin {
                left: curr_lhs,
                right: rhs,
                join_type,
                condition,
            }))
        }
        Ok(Some(curr_lhs))
    }

    /// Parses insert query statement.
    ///
    /// ```sql
    /// INSERT INTO table_name (col1, col2) VALUES (val1, val2), (val3, val4), (...);
    ///
    /// Also supports implicit insertions:
    /// INSERT INTO table_name VALUES (val1, val2), (val3, val4), (...);
    /// ```
    fn parse_insert(&mut self) -> Result<Insert, Error> {
        self.consume(&Token::Insert)?;
        self.consume(&Token::Into)?;

        let table_name = self.parse_identifier()?;
        let mut columns = Vec::new();

        // Optional column list: (col1, col2, col3, ...)
        if self.match_token(&Token::LParen)? {
            loop {
                columns.push(self.parse_identifier()?);
                if !self.match_token(&Token::Comma)? {
                    break;
                }
            }
            self.consume(&Token::RParen)?;
        }
        self.consume(&Token::Values)?;

        // Parse multiple rows of values: (val1, val2), (val3, val4)
        let mut values = Vec::new();
        loop {
            self.consume(&Token::LParen)?;
            let mut row_values = Vec::new();
            loop {
                row_values.push(self.parse_expressions()?);
                if !self.match_token(&Token::Comma)? {
                    break;
                }
            }
            self.consume(&Token::RParen)?;

            values.push(row_values);
            if !self.match_token(&Token::Comma)? {
                break;
            }
        }
        Ok(Insert {
            table_name,
            columns,
            values,
        })
    }

    /// Parses the GROUP BY clause for aggregation.
    ///
    /// ```sql
    /// SELECT ... GROUP BY col1, col2, table_name.col3;
    /// ```
    fn parse_group_by(&mut self) -> Result<Vec<ColumnReference>, Error> {
        if !self.match_token(&Token::Group)? {
            return Ok(Vec::new());
        }
        self.consume(&Token::By)?;

        let mut col_refs = Vec::new();
        loop {
            col_refs.push(self.parse_column_reference()?);
            if !self.match_token(&Token::Comma)? {
                break;
            }
        }
        Ok(col_refs)
    }

    /// Parses the ORDER BY clause for sorting.
    ///
    /// ```sql
    /// ... ORDER BY col_1 ASC, col2 DESC;
    /// ```
    fn parse_order_by(&mut self) -> Result<Vec<SortSpecification>, Error> {
        if !self.match_token(&Token::Order)? {
            return Ok(Vec::new());
        }
        self.consume(&Token::By)?;

        let mut sort_specs = Vec::new();
        loop {
            let column = self.parse_column_reference()?;
            let descending = self.match_token(&Token::Desc)?;

            if !descending {
                // Consume a token only if ASC is present.
                self.match_token(&Token::Asc)?;
            }
            sort_specs.push(SortSpecification { column, descending });
            if !self.match_token(&Token::Comma)? {
                break;
            }
        }
        Ok(sort_specs)
    }

    /// Parses LIMIT and OFFSET clauses in any order.
    ///
    /// ```sql
    /// LIMIT 10 OFFSET 5;
    /// OFFSET 5 LIMIT 10;
    /// ```
    fn parse_limit_offset(&mut self) -> Result<(Option<usize>, Option<usize>), Error> {
        let mut offset = None;
        let mut limit = None;
        loop {
            match self.curr_token {
                Token::Limit if limit.is_none() => {
                    self.advance()?;
                    limit = Some(self.parse_usize_literal()?);
                }
                Token::Offset if offset.is_none() => {
                    self.advance()?;
                    offset = Some(self.parse_usize_literal()?);
                }
                _ => break,
            }
        }
        Ok((limit, offset))
    }

    /// Basically a integer parser but returns error if the value is less than 0.
    fn parse_usize_literal(&mut self) -> Result<usize, Error> {
        match self.curr_token {
            Token::IntLit(val) => {
                if val < 0 {
                    return Err(Error::SyntaxErr("value cannot be negative".into()));
                }
                self.advance()?;
                Ok(val as usize)
            }
            _ => Err(Error::SyntaxErr(format!(
                "expected positive integer, found {:?}",
                self.curr_token
            ))),
        }
    }

    /// Parses a set of limited expressions.
    ///
    /// TODO: document later, the precedence thing, as in why call parse_or?
    fn parse_expressions(&mut self) -> Result<Expr, Error> {
        self.parse_or_condition()
    }

    /// Parses the OR operator. (Lowest precedence)
    ///
    /// ```sql
    /// expr1 OR expr2 OR expr3
    /// ```
    fn parse_or_condition(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_and_condition()?;
        while self.match_token(&Token::Or)? {
            let right = self.parse_and_condition()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            }
        }
        Ok(left)
    }

    /// Parses the AND operator. (Higher precendence than OR)
    ///
    /// ```sql
    /// ... expr1 AND expr2 AND expr3
    /// ```
    fn parse_and_condition(&mut self) -> Result<Expr, Error> {
        let mut left_expr = self.parse_comparison_predicate()?;
        while self.match_token(&Token::And)? {
            let right_expr = self.parse_comparison_predicate()?;
            left_expr = Expr::BinaryOp {
                left: Box::new(left_expr),
                op: BinaryOperator::And,
                right: Box::new(right_expr),
            };
        }
        Ok(left_expr)
    }

    /// Parses comparison operators. (Higher precendence than AND)
    ///
    /// ```sql
    /// expr1 = expr2, expr1 != expr2, >, <, >=, <=
    /// ```
    fn parse_comparison_predicate(&mut self) -> Result<Expr, Error> {
        let left_expr = self.parse_primary()?;
        let bin_op = match &self.curr_token {
            Token::Gte => BinaryOperator::Gte,
            Token::Gt => BinaryOperator::Gt,
            Token::Lte => BinaryOperator::Lte,
            Token::Lt => BinaryOperator::Lt,
            Token::Neq => BinaryOperator::Neq,
            Token::Eq => BinaryOperator::Eq,
            _ => return Ok(left_expr),
        };
        self.advance()?;
        let right_expr = self.parse_primary()?;

        Ok(Expr::BinaryOp {
            left: Box::new(left_expr),
            op: bin_op,
            right: Box::new(right_expr),
        })
    }

    /// Parses base values and parenthesis. (Highest predence)
    ///
    /// ```sql
    /// 123, "string", TRUE, FALSE, col_name, users.id, COUNT(*), AVG(col), (nested_expr);
    /// ```
    fn parse_primary(&mut self) -> Result<Expr, Error> {
        // Parse Count(*)/Count(col_ref)
        if self.match_token(&Token::Count)? {
            return self.parse_count();
        }
        // Parse Avg(col)
        if self.match_token(&Token::Avg)? {
            self.consume(&Token::LParen)?;
            let col_ref = self.parse_column_reference()?;
            self.consume(&Token::RParen)?;
            return Ok(Expr::Average(col_ref));
        }
        match &self.curr_token {
            Token::StringLit(value) => {
                let val = AstLiteral::String(value.clone());
                self.advance()?;
                Ok(Expr::Literal(val))
            }
            Token::IntLit(value) => {
                let val = AstLiteral::Int(*value);
                self.advance()?;
                Ok(Expr::Literal(val))
            }
            Token::BoolLit(value) => {
                let val = AstLiteral::Boolean(*value);
                self.advance()?;
                Ok(Expr::Literal(val))
            }
            Token::Ident(_) => {
                let col_ref = self.parse_column_reference()?;
                Ok(Expr::Column(col_ref))
            }
            // Start of a nested expression
            Token::LParen => {
                self.advance()?;
                // Jump back to top precedence to handle nested logic.
                let expr = self.parse_expressions()?;
                self.consume(&Token::RParen)?;
                Ok(expr)
            }
            _ => Err(Error::SyntaxErr(format!(
                "expected literal, identifier, or '(', found {:?}",
                self.curr_token
            ))),
        }
    }

    /// Parses the count keyword and whatever's inside it.
    ///
    /// ```sql
    /// SELECT COUNT(*) or COUNT(col_name) ...
    /// ```
    fn parse_count(&mut self) -> Result<Expr, Error> {
        self.consume(&Token::LParen)?;
        if self.match_token(&Token::Asterisk)? {
            self.consume(&Token::RParen)?;
            return Ok(Expr::Count(None));
        }
        let col_ref = self.parse_column_reference()?;

        self.consume(&Token::RParen)?;
        Ok(Expr::Count(Some(col_ref)))
    }

    /// Parses update query statement.
    ///
    /// ```sql
    /// UPDATE table_name SET col1 = expr1, col2 = expr2 WHERE condition;
    /// ```
    fn parse_update(&mut self) -> Result<Update, Error> {
        self.consume(&Token::Update)?;

        let table_name = self.parse_identifier()?;
        self.consume(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col_name = self.parse_identifier()?;
            self.consume(&Token::Eq)?;
            let value = self.parse_expressions()?;

            assignments.push(Assignment {
                column_name: col_name,
                value,
            });
            if !self.match_token(&Token::Comma)? {
                break;
            }
        }
        let where_clause = self.parse_where_clause()?;
        Ok(Update {
            table_name,
            assignments,
            where_clause,
        })
    }

    /// Parses an optional WHERE clause.
    ///
    /// ```sql
    /// ... WHERE expr;
    /// ```
    fn parse_where_clause(&mut self) -> Result<Option<Expr>, Error> {
        if self.match_token(&Token::Where)? {
            let expr = self.parse_expressions()?;
            Ok(Some(expr))
        } else {
            Ok(None)
        }
    }

    /// Parses delete query statement.
    ///
    /// ```sql
    /// DELETE FROM table_name WHERE condition;
    /// ```
    fn parse_delete(&mut self) -> Result<Delete, Error> {
        self.consume(&Token::Delete)?;
        self.consume(&Token::From)?;

        let table_name = self.parse_identifier()?;
        let where_clause = self.parse_where_clause()?;
        Ok(Delete {
            table_name,
            where_clause,
        })
    }

    /// Parses the create table query statement.
    ///
    /// ```sql
    /// CREATE TABLE table_name (id BIGINT, name VARCHAR(255), correct BOOLEAN);
    /// ```
    fn parse_create_table(&mut self) -> Result<CreateTable, Error> {
        let table_name = self.parse_identifier()?;
        self.consume(&Token::LParen)?;

        let mut columns = Vec::new();
        // Consume all the comma-separated column definition
        loop {
            columns.push(self.parse_column_definition()?);
            if !self.match_token(&Token::Comma)? {
                break;
            }
        }
        self.consume(&Token::RParen)?;
        Ok(CreateTable { table_name, columns })
    }

    /// Parses a single column definition: `column_name DATA_TYPE`.
    fn parse_column_definition(&mut self) -> Result<ColumnDefinition, Error> {
        let col_name = self.parse_identifier()?;

        let data_type_token = self.curr_token.clone();
        self.advance()?;

        let mut length = None;

        // Maps the SQL AST token directly to the storage engine's DataType enum.
        let data_type = match data_type_token {
            Token::BigIntType => DataType::BigInt,
            Token::IntType => DataType::Int,
            Token::BooleanType => DataType::Boolean,
            Token::VarcharType => {
                length = Some(self.parse_varchar()?);
                DataType::Varchar
            }
            _ => {
                return Err(Error::SyntaxErr(format!(
                    "expected data type (BIGINT, INT, BOOLEAN, VARCHAR), found {:?}",
                    data_type_token
                )));
            }
        };
        Ok(ColumnDefinition {
            name: col_name,
            data_type,
            length,
        })
    }

    /// Parses a column reference, handling single name or table-qualified names.
    ///
    /// ```sql
    /// id, user.id
    /// ```
    fn parse_column_reference(&mut self) -> Result<ColumnReference, Error> {
        let mut name = self.parse_identifier()?;
        let mut qualifier = None;

        if self.match_token(&Token::Dot)? {
            qualifier = Some(name);
            name = self.parse_identifier()?;
        }
        Ok(ColumnReference {
            qualifier,
            column_name: name,
        })
    }

    /// Parses the `VARCHAR(N)` syntax and returns the N if valid.
    fn parse_varchar(&mut self) -> Result<u32, Error> {
        self.consume(&Token::LParen)?; // Consume '(' in VARCHAR(255)
        let length;

        if let Token::IntLit(val) = self.curr_token {
            if val <= 0 {
                return Err(Error::SyntaxErr(
                    "VARCHAR length must be greater than 0".into(),
                ));
            }
            length = val as u32;
            self.advance()?;
        } else {
            return Err(Error::SyntaxErr(format!(
                "expected integer length for VARCHAR, found {:?}",
                self.curr_token
            )));
        }
        self.consume(&Token::RParen)?;
        Ok(length)
    }

    /// Parses create index query statement.
    ///
    /// ```sql
    /// CREATE INDEX index_name ON table_name (col_name);
    /// ```
    fn parse_create_index(&mut self) -> Result<CreateIndex, Error> {
        let index_name = self.parse_identifier()?;
        self.consume(&Token::On)?;

        let table_name = self.parse_identifier()?;
        self.consume(&Token::LParen)?;

        let column_name = self.parse_identifier()?;
        self.consume(&Token::RParen)?;

        Ok(CreateIndex {
            index_name,
            table_name,
            column_name,
            unique: false,
        })
    }

    /// Parses create unique index query statement.
    ///
    /// ```sql
    /// CREATE UNIQUE INDEX index_name ON table_name (col_name);
    /// ```
    fn parse_create_unique_index(&mut self) -> Result<CreateIndex, Error> {
        let mut create_index = self.parse_create_index()?;
        create_index.unique = true;
        Ok(create_index)
    }
}

#[cfg(test)]
mod tests {
    use core::error;

    use crate::sql::{
        ast::{
            BinaryOperator, ColumnReference, DerivedColumn, Expr, Insert, JoinType, QualifiedJoin,
            Select, SortSpecification, Statement, TableReference,
        },
        lexer::Lexer,
        parser::{AstLiteral, Parser},
    };

    #[test]
    fn test_insert_statement() -> Result<(), Box<dyn error::Error>> {
        let query = r#"insert into users (name, age) values ("parth", 23), ("juhi", 24)"#;
        let lexer = Lexer::new(&query);

        let mut parser = Parser::new(lexer)?;

        let st_got = parser.parse_statement()?;

        let expr1 = Expr::Literal(AstLiteral::String("parth".into()));
        let expr2 = Expr::Literal(AstLiteral::Int(23));

        let expr3 = Expr::Literal(AstLiteral::String("juhi".into()));
        let expr4 = Expr::Literal(AstLiteral::Int(24));

        let insert_want = Insert {
            table_name: "users".into(),
            columns: vec!["name".into(), "age".into()],
            values: vec![vec![expr1, expr2], vec![expr3, expr4]],
        };
        let st_want = Statement::Insert(insert_want);

        dbg!(&st_want, &st_got);

        assert_eq!(st_want, st_got);
        Ok(())
    }

    #[test]
    fn test_simple_select_statement() -> Result<(), Box<dyn error::Error>> {
        let query = r#"select name, age, address from users where id = 24"#;
        let lexer = Lexer::new(&query);

        let mut parser = Parser::new(lexer)?;
        let st_got = parser.parse_statement()?;

        let st_want = Statement::Select(Select {
            select_list: vec![
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: None,
                        column_name: "name".into(),
                    }),
                    alias: None,
                },
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: None,
                        column_name: "age".into(),
                    }),
                    alias: None,
                },
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: None,
                        column_name: "address".into(),
                    }),
                    alias: None,
                },
            ],
            from: Some(TableReference::BaseTable {
                name: "users".into(),
                alias: None,
            }),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnReference {
                    qualifier: None,
                    column_name: "id".into(),
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(AstLiteral::Int(24))),
            }),
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        });
        assert_eq!(st_got, st_want);

        dbg!(&st_got);
        Ok(())
    }

    #[test]
    fn test_complex_select_statement() -> Result<(), Box<dyn error::Error>> {
        let query = r#"
            select us1.name as name, us1.age age, us2.address as address
            from user1 as us1
            inner join user2 as us2 on us1.id = us2.id
            where us1.id >= 10 and us2.id <= 50
            group by us1.age
            order by us1.age desc"#;
        let lexer = Lexer::new(&query);

        let mut parser = Parser::new(lexer)?;
        let st_got = parser.parse_statement()?;

        let st_want = Statement::Select(Select {
            select_list: vec![
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "name".into(),
                    }),
                    alias: Some("name".into()),
                },
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "age".into(),
                    }),
                    alias: Some("age".into()),
                },
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: Some("us2".into()),
                        column_name: "address".into(),
                    }),
                    alias: Some("address".into()),
                },
            ],
            from: Some(TableReference::Join(Box::new(QualifiedJoin {
                left: TableReference::BaseTable {
                    name: "user1".into(),
                    alias: Some("us1".into()),
                },
                right: TableReference::BaseTable {
                    name: "user2".into(),
                    alias: Some("us2".into()),
                },
                join_type: JoinType::Inner,
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "id".into(),
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us2".into()),
                        column_name: "id".into(),
                    })),
                },
            }))),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "id".into(),
                    })),
                    op: BinaryOperator::Gte,
                    right: Box::new(Expr::Literal(AstLiteral::Int(10))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us2".into()),
                        column_name: "id".into(),
                    })),
                    op: BinaryOperator::Lte,
                    right: Box::new(Expr::Literal(AstLiteral::Int(50))),
                }),
            }),
            group_by: vec![ColumnReference {
                qualifier: Some("us1".into()),
                column_name: "age".into(),
            }],
            order_by: vec![SortSpecification {
                column: ColumnReference {
                    qualifier: Some("us1".into()),
                    column_name: "age".into(),
                },
                descending: true,
            }],
            limit: None,
            offset: None,
        });
        assert_eq!(st_got, st_want);
        Ok(())
    }

    #[test]
    fn test_multiple_join_statement() -> Result<(), Box<dyn error::Error>> {
        let query = r#"
                    Select Count(us1.name) as name_count, us1.age age, Avg(us2.balance) as avg_balance,
                    Avg(us3.workdays) as avg_workdays
                    From user1 As us1 Inner Join user2 As us2 On us1.id = us2.id Right Join
                    users3 As us3 On us1.age = us3.age
                    Where us3.workdays > 10 And us3.workdays <= 100
                    Group By us1.age
                    Order By avg_balance Desc"#;
        let lexer = Lexer::new(&query);

        let mut parser = Parser::new(lexer)?;
        let st_got = parser.parse_statement()?;

        let st_want = Statement::Select(Select {
            select_list: vec![
                DerivedColumn {
                    expr: Expr::Count(Some(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "name".into(),
                    })),
                    alias: Some("name_count".into()),
                },
                DerivedColumn {
                    expr: Expr::Column(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "age".into(),
                    }),
                    alias: Some("age".into()),
                },
                DerivedColumn {
                    expr: Expr::Average(ColumnReference {
                        qualifier: Some("us2".into()),
                        column_name: "balance".into(),
                    }),
                    alias: Some("avg_balance".into()),
                },
                DerivedColumn {
                    expr: Expr::Average(ColumnReference {
                        qualifier: Some("us3".into()),
                        column_name: "workdays".into(),
                    }),
                    alias: Some("avg_workdays".into()),
                },
            ],

            from: Some(TableReference::Join(Box::new(QualifiedJoin {
                left: TableReference::Join(Box::new(QualifiedJoin {
                    left: TableReference::BaseTable {
                        name: "user1".into(),
                        alias: Some("us1".into()),
                    },
                    join_type: JoinType::Inner,
                    right: TableReference::BaseTable {
                        name: "user2".into(),
                        alias: Some("us2".into()),
                    },
                    condition: Expr::BinaryOp {
                        left: Box::new(Expr::Column(ColumnReference {
                            qualifier: Some("us1".into()),
                            column_name: "id".into(),
                        })),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Column(ColumnReference {
                            qualifier: Some("us2".into()),
                            column_name: "id".into(),
                        })),
                    },
                })),
                join_type: JoinType::Right,
                right: TableReference::BaseTable {
                    name: "users3".into(),
                    alias: Some("us3".into()),
                },
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us1".into()),
                        column_name: "age".into(),
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us3".into()),
                        column_name: "age".into(),
                    })),
                },
            }))),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us3".into()),
                        column_name: "workdays".into(),
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Literal(AstLiteral::Int(10))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnReference {
                        qualifier: Some("us3".into()),
                        column_name: "workdays".into(),
                    })),
                    op: BinaryOperator::Lte,
                    right: Box::new(Expr::Literal(AstLiteral::Int(100))),
                }),
            }),
            group_by: vec![ColumnReference {
                qualifier: Some("us1".into()),
                column_name: "age".into(),
            }],
            order_by: vec![SortSpecification {
                column: ColumnReference {
                    qualifier: None,
                    column_name: "avg_balance".into(),
                },
                descending: true,
            }],
            limit: None,
            offset: None,
        });
        assert_eq!(st_got, st_want);
        Ok(())
    }
}
