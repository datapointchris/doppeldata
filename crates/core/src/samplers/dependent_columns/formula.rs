//! The arithmetic a derived column's `expr` is written in: column names, numbers, `+ - * /`,
//! parentheses, unary minus, and `round(x)` or `round(x, places)`. Nothing else is evaluated.

use crate::samplers::numeric::power_of_ten;

/// The most tokens an expression holds. Parsing, evaluating and dropping a formula each recurse once per level of its
/// tree, which is never deeper than its tokens, so this bounds the stack each takes.
const MOST_TOKENS: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Formula {
    Number(f64),
    /// The column at this place in the formula's list of names.
    Column(usize),
    Negate(Box<Formula>),
    Binary(Operator, Box<Formula>, Box<Formula>),
    Round(Box<Formula>, i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Name(String),
    Symbol(char),
}

impl Formula {
    /// The formula `text` writes, and the column names it reads in the order they first appear.
    pub(crate) fn parse(text: &str) -> Result<(Self, Vec<String>), String> {
        let tokens = tokens(text)?;
        let mut parser = Parser {
            tokens,
            at: 0,
            names: Vec::new(),
        };
        let formula = parser.sum()?;
        if let Some(token) = parser.tokens.get(parser.at) {
            return Err(format!("{text:?} has {} where its end was expected", described(token)));
        }
        Ok((formula, parser.names))
    }

    /// The value for one row, given each name's value in order.
    pub(crate) fn evaluate(&self, values: &[f64]) -> f64 {
        match self {
            Self::Number(number) => *number,
            Self::Column(at) => values[*at],
            Self::Negate(inner) => -inner.evaluate(values),
            Self::Binary(operator, left, right) => {
                let (left, right) = (left.evaluate(values), right.evaluate(values));
                match operator {
                    Operator::Add => left + right,
                    Operator::Subtract => left - right,
                    Operator::Multiply => left * right,
                    Operator::Divide => left / right,
                }
            }
            Self::Round(inner, places) => {
                let factor = power_of_ten(*places);
                (inner.evaluate(values) * factor).round() / factor
            }
        }
    }
}

fn described(token: &Token) -> String {
    match token {
        Token::Number(number) => format!("the number {number}"),
        Token::Name(name) => format!("the name {name:?}"),
        Token::Symbol(symbol) => format!("{symbol:?}"),
    }
}

fn tokens(text: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some(&(start, character)) = chars.peek() {
        if character.is_whitespace() {
            chars.next();
        } else if character.is_ascii_digit() || character == '.' {
            let mut end = start;
            while let Some(&(at, next)) = chars.peek() {
                let exponent_sign = (next == '+' || next == '-') && matches!(text[..at].chars().last(), Some('e' | 'E'));
                if next.is_ascii_digit() || next == '.' || next == 'e' || next == 'E' || exponent_sign {
                    end = at + next.len_utf8();
                    chars.next();
                } else {
                    break;
                }
            }
            let number = &text[start..end];
            tokens.push(Token::Number(
                number.parse().map_err(|_| format!("{number:?} in {text:?} is not a number"))?,
            ));
        } else if character.is_alphabetic() || character == '_' {
            let mut end = start;
            while let Some(&(at, next)) = chars.peek() {
                if next.is_alphanumeric() || next == '_' {
                    end = at + next.len_utf8();
                    chars.next();
                } else {
                    break;
                }
            }
            tokens.push(Token::Name(text[start..end].to_string()));
        } else if character == '`' {
            chars.next();
            let mut name = String::new();
            loop {
                match chars.next() {
                    Some((_, '`')) => break,
                    Some((_, next)) => name.push(next),
                    None => return Err(format!("{text:?} opens a name with ` and never closes it")),
                }
            }
            tokens.push(Token::Name(name));
        } else if "+-*/(),".contains(character) {
            tokens.push(Token::Symbol(character));
            chars.next();
        } else {
            return Err(format!("{text:?} holds {character:?}, which no expression takes"));
        }
        if tokens.len() > MOST_TOKENS {
            return Err(format!("the expression holds more than {MOST_TOKENS} names, numbers and symbols"));
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
    names: Vec<String>,
}

impl Parser {
    fn next_is(&self, symbol: char) -> bool {
        self.tokens.get(self.at) == Some(&Token::Symbol(symbol))
    }

    fn expect(&mut self, symbol: char) -> Result<(), String> {
        if self.next_is(symbol) {
            self.at += 1;
            return Ok(());
        }
        let found = self.tokens.get(self.at).map_or_else(|| "its end".to_string(), described);
        Err(format!("expected {symbol:?}, found {found}"))
    }

    fn sum(&mut self) -> Result<Formula, String> {
        let mut formula = self.product()?;
        loop {
            let operator = match self.tokens.get(self.at) {
                Some(Token::Symbol('+')) => Operator::Add,
                Some(Token::Symbol('-')) => Operator::Subtract,
                _ => return Ok(formula),
            };
            self.at += 1;
            formula = Formula::Binary(operator, Box::new(formula), Box::new(self.product()?));
        }
    }

    fn product(&mut self) -> Result<Formula, String> {
        let mut formula = self.factor()?;
        loop {
            let operator = match self.tokens.get(self.at) {
                Some(Token::Symbol('*')) => Operator::Multiply,
                Some(Token::Symbol('/')) => Operator::Divide,
                _ => return Ok(formula),
            };
            self.at += 1;
            formula = Formula::Binary(operator, Box::new(formula), Box::new(self.factor()?));
        }
    }

    fn factor(&mut self) -> Result<Formula, String> {
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        match token {
            Some(Token::Number(number)) => Ok(Formula::Number(number)),
            Some(Token::Symbol('-')) => Ok(Formula::Negate(Box::new(self.factor()?))),
            Some(Token::Symbol('(')) => {
                let inner = self.sum()?;
                self.expect(')')?;
                Ok(inner)
            }
            Some(Token::Name(name)) if name == "round" && self.next_is('(') => {
                self.at += 1;
                let inner = self.sum()?;
                let places = if self.next_is(',') {
                    self.at += 1;
                    match self.tokens.get(self.at) {
                        Some(&Token::Number(places)) if places.fract() == 0.0 && (0.0..=15.0).contains(&places) => {
                            self.at += 1;
                            places as i32
                        }
                        _ => return Err("round takes its places as a whole number from 0 to 15".to_string()),
                    }
                } else {
                    0
                };
                self.expect(')')?;
                Ok(Formula::Round(Box::new(inner), places))
            }
            Some(Token::Name(name)) => {
                let at = self.names.iter().position(|known| *known == name).unwrap_or_else(|| {
                    self.names.push(name);
                    self.names.len() - 1
                });
                Ok(Formula::Column(at))
            }
            Some(token) => Err(format!("expected a name, a number or '(', found {}", described(&token))),
            None => Err("the expression ends where a name, a number or '(' was expected".to_string()),
        }
    }
}
