use std::{iter::Peekable, str::Chars};

/* TODO: Support expressions like 1 + 1 and alike in most statements, both the lexer
and parser will need to be extended. */

use crate::{error::Error, sql::token::Token};

/// Transforms a raw SQL string into a stream of structured `Tokens`.
#[derive(Debug)]
pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    /// Initializes a new Lexer wrapping a utf-8 character iterator.
    pub fn new(query: &'a str) -> Self {
        Self {
            input: query.chars().peekable(),
        }
    }

    /// Consumes whitespace characters until a significant character is found.
    fn skip_whitespace(&mut self) {
        while let Some(&ch) = self.input.peek() {
            if ch.is_whitespace() {
                self.input.next();
            } else {
                break;
            }
        }
    }

    /// Reads an alphanumeric identifier and maps it to a keyword if applicable.
    fn consume_ident_or_keyword(&mut self) -> Result<Token, Error> {
        let mut ident = String::new();

        while let Some(&ch) = self.input.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                ident.push(ch);
                self.input.next();
            } else {
                break;
            }
        }
        Ok(match ident.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "FROM" => Token::From,
            "AS" => Token::As,
            "WHERE" => Token::Where,
            "AND" => Token::And,
            "OR" => Token::Or,
            "COUNT" => Token::Count,
            "AVG" => Token::Avg,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "DATABASE" => Token::Database,
            "DATABASES" => Token::Databases,
            "INDEX" => Token::Index,
            "UNIQUE" => Token::Unique,
            "ON" => Token::On,
            "COMMIT" => Token::Commit,
            "BEGIN" => Token::Begin,
            "TRANSACTION" => Token::Transaction,
            "ROLLBACK" => Token::Rollback,

            "OFFSET" => Token::Offset,
            "LIMIT" => Token::Limit,

            "ORDER" => Token::Order,
            "BY" => Token::By,
            "GROUP" => Token::Group,

            "ASC" => Token::Asc,
            "DESC" => Token::Desc,

            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "JOIN" => Token::Join,

            "USE" => Token::Use,
            "SHOW" => Token::Show,
            "INT" => Token::IntType,
            "VARCHAR" => Token::VarcharType,
            "BOOLEAN" => Token::BooleanType,
            "BIGINT" => Token::BigIntType,
            "TRUE" => Token::BoolLit(true),
            "FALSE" => Token::BoolLit(false),
            _ => Token::Ident(ident),
        })
    }

    /// Reads a contiguous sequence of digits into an integeral literal.
    fn consume_number(&mut self) -> Result<Token, Error> {
        let mut num_str = String::new();

        while let Some(&ch) = self.input.peek() {
            if ch.is_ascii_digit() {
                num_str.push(ch);
                self.input.next();
            } else {
                break;
            }
        }
        match num_str.parse::<i64>() {
            Ok(number) => Ok(Token::IntLit(number)),
            Err(e) => Err(Error::ParseErr(format!(
                "invalid integer literal '{}': {}",
                num_str, e
            ))),
        }
    }

    /// Reads a string literal enclosed in single quotes.
    fn consume_string(&mut self) -> Result<Token, Error> {
        self.input.next(); // consume the opening quote.
        let mut string_lit = String::new();

        while let Some(&ch) = self.input.peek() {
            if ch == '\'' || ch == '\"' {
                self.input.next(); // consume the closing quote.
                return Ok(Token::StringLit(string_lit));
            } else {
                string_lit.push(ch);
                self.input.next();
            }
        }
        Err(Error::ParseErr("unclosed string literal".into()))
    }

    /// Returns the next token in the input stream, or Token::Eof if exhausted.
    pub fn next_token(&mut self) -> Result<Token, Error> {
        self.skip_whitespace();

        let Some(&ch) = self.input.peek() else {
            return Ok(Token::Eof);
        };
        match ch {
            '*' => {
                self.input.next();
                Ok(Token::Asterisk)
            }
            ',' => {
                self.input.next();
                Ok(Token::Comma)
            }
            '(' => {
                self.input.next();
                Ok(Token::LParen)
            }
            ')' => {
                self.input.next();
                Ok(Token::RParen)
            }
            ';' => {
                self.input.next();
                Ok(Token::Semicolon)
            }
            '=' => {
                self.input.next();
                Ok(Token::Eq)
            }
            '.' => {
                self.input.next();
                Ok(Token::Dot)
            }
            '<' => {
                self.input.next();
                if self.input.peek() == Some(&'=') {
                    self.input.next();
                    Ok(Token::Lte)
                } else {
                    Ok(Token::Lt)
                }
            }
            '>' => {
                self.input.next();
                if self.input.peek() == Some(&'=') {
                    self.input.next();
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            '!' => {
                self.input.next();
                if self.input.peek() == Some(&'=') {
                    self.input.next();
                    Ok(Token::Neq)
                } else {
                    Err(Error::ParseErr(
                        "unexpected character: '!' without '='".into(),
                    ))
                }
            }
            '\'' | '\"' => self.consume_string(),
            _ if ch.is_alphabetic() || ch == '_' => self.consume_ident_or_keyword(),
            _ if ch.is_ascii_digit() => self.consume_number(),
            _ => {
                self.input.next();
                Err(Error::ParseErr(format!("unrecognized character: {}", ch)))
            }
        }
    }
}
