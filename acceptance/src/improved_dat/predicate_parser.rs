//! SQL-like predicate parser for improved_dat test predicates.
//!
//! Parses predicates like `"long_col >= 900 AND long_col < 950"` into kernel [`Predicate`] types.

use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};
use delta_kernel::schema::DataType;

/// Error type for predicate parsing
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Unexpected end of input")]
    UnexpectedEof,
    #[error("Unexpected token: {0}")]
    UnexpectedToken(String),
    #[error("Expected {expected}, found {found}")]
    Expected { expected: String, found: String },
    #[error("Invalid number: {0}")]
    InvalidNumber(String),
}

/// Token types for the lexer
#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Identifiers and literals
    Ident(String),
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,

    // Comparison operators
    Eq,        // =
    Ne,        // != or <>
    Lt,        // <
    Le,        // <=
    Gt,        // >
    Ge,        // >=
    NullSafeEq, // <=>

    // Boolean operators
    And,
    Or,
    Not,

    // Keywords
    Is,
    In,
    Like,

    // Punctuation
    LParen,
    RParen,
    Comma,
    Dot,

    // End of input
    Eof,
}

/// Lexer for tokenizing predicate strings
struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.next_char();
            } else {
                break;
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' {
                self.next_char();
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_string()
    }

    fn read_number(&mut self) -> Result<Token, ParseError> {
        let start = self.pos;
        let mut has_dot = false;
        let mut has_sign = false;

        // Handle negative sign
        if self.peek_char() == Some('-') {
            has_sign = true;
            self.next_char();
        }

        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                self.next_char();
            } else if c == '.' && !has_dot {
                has_dot = true;
                self.next_char();
            } else {
                break;
            }
        }

        let num_str = &self.input[start..self.pos];

        // Check for empty number (just a sign)
        if has_sign && num_str.len() == 1 {
            return Err(ParseError::InvalidNumber(num_str.to_string()));
        }

        if has_dot {
            num_str
                .parse::<f64>()
                .map(Token::Float)
                .map_err(|_| ParseError::InvalidNumber(num_str.to_string()))
        } else {
            num_str
                .parse::<i64>()
                .map(Token::Integer)
                .map_err(|_| ParseError::InvalidNumber(num_str.to_string()))
        }
    }

    fn read_string(&mut self, quote: char) -> Result<String, ParseError> {
        self.next_char(); // consume opening quote
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c == quote {
                let s = self.input[start..self.pos].to_string();
                self.next_char(); // consume closing quote
                return Ok(s);
            } else if c == '\\' {
                self.next_char(); // skip escape
                self.next_char(); // skip escaped char
            } else {
                self.next_char();
            }
        }
        Err(ParseError::UnexpectedEof)
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        let c = match self.peek_char() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        // Single character tokens and operators
        match c {
            '(' => {
                self.next_char();
                return Ok(Token::LParen);
            }
            ')' => {
                self.next_char();
                return Ok(Token::RParen);
            }
            ',' => {
                self.next_char();
                return Ok(Token::Comma);
            }
            '.' => {
                self.next_char();
                return Ok(Token::Dot);
            }
            '=' => {
                self.next_char();
                return Ok(Token::Eq);
            }
            '<' => {
                self.next_char();
                match self.peek_char() {
                    Some('=') => {
                        self.next_char();
                        // Check for <=> (null-safe equality)
                        if self.peek_char() == Some('>') {
                            self.next_char();
                            return Ok(Token::NullSafeEq);
                        }
                        return Ok(Token::Le);
                    }
                    Some('>') => {
                        self.next_char();
                        return Ok(Token::Ne);
                    }
                    _ => return Ok(Token::Lt),
                }
            }
            '>' => {
                self.next_char();
                if self.peek_char() == Some('=') {
                    self.next_char();
                    return Ok(Token::Ge);
                }
                return Ok(Token::Gt);
            }
            '!' => {
                self.next_char();
                if self.peek_char() == Some('=') {
                    self.next_char();
                    return Ok(Token::Ne);
                }
                return Err(ParseError::UnexpectedToken("!".to_string()));
            }
            '\'' | '"' => {
                let s = self.read_string(c)?;
                return Ok(Token::String(s));
            }
            _ => {}
        }

        // Numbers (including negative)
        if c.is_ascii_digit() || (c == '-' && self.input[self.pos + 1..].chars().next().is_some_and(|c| c.is_ascii_digit())) {
            return self.read_number();
        }

        // Identifiers and keywords
        if c.is_alphabetic() || c == '_' {
            let ident = self.read_ident();
            let upper = ident.to_uppercase();
            return Ok(match upper.as_str() {
                "AND" => Token::And,
                "OR" => Token::Or,
                "NOT" => Token::Not,
                "IS" => Token::Is,
                "IN" => Token::In,
                "LIKE" => Token::Like,
                "NULL" => Token::Null,
                "TRUE" => Token::Boolean(true),
                "FALSE" => Token::Boolean(false),
                _ => Token::Ident(ident),
            });
        }

        Err(ParseError::UnexpectedToken(c.to_string()))
    }
}

/// Parser for predicate expressions
pub struct PredicateParser<'a> {
    lexer: Lexer<'a>,
    current: Token,
}

impl<'a> PredicateParser<'a> {
    /// Create a new parser for the given input
    pub fn new(input: &'a str) -> Result<Self, ParseError> {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token()?;
        Ok(Self { lexer, current })
    }

    fn advance(&mut self) -> Result<(), ParseError> {
        self.current = self.lexer.next_token()?;
        Ok(())
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.current == expected {
            self.advance()
        } else {
            Err(ParseError::Expected {
                expected: format!("{:?}", expected),
                found: format!("{:?}", self.current),
            })
        }
    }

    /// Parse the entire predicate expression
    pub fn parse(&mut self) -> Result<Predicate, ParseError> {
        let pred = self.parse_or()?;
        if self.current != Token::Eof {
            return Err(ParseError::UnexpectedToken(format!("{:?}", self.current)));
        }
        Ok(pred)
    }

    // OR has lowest precedence
    fn parse_or(&mut self) -> Result<Predicate, ParseError> {
        let mut left = self.parse_and()?;
        while self.current == Token::Or {
            self.advance()?;
            let right = self.parse_and()?;
            left = Predicate::or(left, right);
        }
        Ok(left)
    }

    // AND has higher precedence than OR
    fn parse_and(&mut self) -> Result<Predicate, ParseError> {
        let mut left = self.parse_not()?;
        while self.current == Token::And {
            self.advance()?;
            let right = self.parse_not()?;
            left = Predicate::and(left, right);
        }
        Ok(left)
    }

    // NOT has higher precedence than AND/OR
    fn parse_not(&mut self) -> Result<Predicate, ParseError> {
        if self.current == Token::Not {
            self.advance()?;
            let inner = self.parse_not()?;
            return Ok(Predicate::not(inner));
        }
        self.parse_comparison()
    }

    // Comparison operators
    fn parse_comparison(&mut self) -> Result<Predicate, ParseError> {
        let left = self.parse_primary_expr()?;

        // Handle IS NULL / IS NOT NULL
        if self.current == Token::Is {
            self.advance()?;
            let negated = if self.current == Token::Not {
                self.advance()?;
                true
            } else {
                false
            };
            if self.current != Token::Null {
                return Err(ParseError::Expected {
                    expected: "NULL".to_string(),
                    found: format!("{:?}", self.current),
                });
            }
            self.advance()?;
            return if negated {
                Ok(Predicate::is_not_null(left))
            } else {
                Ok(Predicate::is_null(left))
            };
        }

        // Handle comparison operators
        let pred = match &self.current {
            Token::Eq => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                Predicate::eq(left, right)
            }
            Token::Ne => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                Predicate::ne(left, right)
            }
            Token::Lt => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                Predicate::lt(left, right)
            }
            Token::Le => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                Predicate::le(left, right)
            }
            Token::Gt => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                Predicate::gt(left, right)
            }
            Token::Ge => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                Predicate::ge(left, right)
            }
            Token::NullSafeEq => {
                self.advance()?;
                let right = self.parse_primary_expr()?;
                // <=> is null-safe equality: returns true if both are NULL
                // This is DISTINCT inverted: NOT DISTINCT(a, b)
                Predicate::not(Predicate::distinct(left, right))
            }
            _ => {
                // Just a boolean expression (column reference)
                Predicate::from_expr(left)
            }
        };
        Ok(pred)
    }

    // Primary expressions: literals, columns, parenthesized expressions
    fn parse_primary_expr(&mut self) -> Result<Expression, ParseError> {
        match &self.current {
            Token::LParen => {
                self.advance()?;
                let pred = self.parse_or()?;
                self.expect(Token::RParen)?;
                Ok(Expression::from_pred(pred))
            }
            Token::Integer(n) => {
                let n = *n;
                self.advance()?;
                Ok(Expression::literal(Scalar::Long(n)))
            }
            Token::Float(f) => {
                let f = *f;
                self.advance()?;
                Ok(Expression::literal(Scalar::Double(f)))
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(Expression::literal(Scalar::String(s)))
            }
            Token::Boolean(b) => {
                let b = *b;
                self.advance()?;
                Ok(Expression::literal(Scalar::Boolean(b)))
            }
            Token::Null => {
                self.advance()?;
                // Use a generic NULL type
                Ok(Expression::null_literal(DataType::STRING))
            }
            Token::Ident(name) => {
                let mut parts = vec![name.clone()];
                self.advance()?;
                // Handle nested column references like "data.category"
                while self.current == Token::Dot {
                    self.advance()?;
                    if let Token::Ident(part) = &self.current {
                        parts.push(part.clone());
                        self.advance()?;
                    } else {
                        return Err(ParseError::Expected {
                            expected: "identifier".to_string(),
                            found: format!("{:?}", self.current),
                        });
                    }
                }
                Ok(Expression::column(ColumnName::new(parts)))
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }
}

/// Parse a SQL-like predicate string into a kernel Predicate
pub fn parse_predicate(input: &str) -> Result<Predicate, ParseError> {
    let mut parser = PredicateParser::new(input)?;
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_equality() {
        let pred = parse_predicate("id = 2").unwrap();
        assert!(matches!(pred, Predicate::Binary(_)));
    }

    #[test]
    fn test_compound_predicate() {
        let pred = parse_predicate("long_col >= 900 AND long_col < 950").unwrap();
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_is_null() {
        let pred = parse_predicate("col IS NULL").unwrap();
        assert!(matches!(pred, Predicate::Unary(_)));
    }

    #[test]
    fn test_is_not_null() {
        let pred = parse_predicate("col IS NOT NULL").unwrap();
        assert!(matches!(pred, Predicate::Not(_)));
    }

    #[test]
    fn test_string_literal() {
        let pred = parse_predicate("name = 'alice'").unwrap();
        assert!(matches!(pred, Predicate::Binary(_)));
    }

    #[test]
    fn test_nested_column() {
        let pred = parse_predicate("data.category = 'test'").unwrap();
        assert!(matches!(pred, Predicate::Binary(_)));
    }

    #[test]
    fn test_or_predicate() {
        let pred = parse_predicate("a = 1 OR b = 2").unwrap();
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_parentheses() {
        let pred = parse_predicate("(a = 1 OR b = 2) AND c = 3").unwrap();
        assert!(matches!(pred, Predicate::Junction(_)));
    }

    #[test]
    fn test_not_predicate() {
        let pred = parse_predicate("NOT a = 1").unwrap();
        assert!(matches!(pred, Predicate::Not(_)));
    }
}
