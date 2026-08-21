use std::{
    collections::hash_map,
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::error::Error;

/// Represents the logical data type of a column defined in Schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataType {
    BigInt = 0,
    Int = 1,
    Boolean = 2,
    Varchar = 3,
}

impl DataType {
    /// Returns the DataType corresponding to the given u8 `val`.
    pub fn from_u8(val: u8) -> Result<Self, Error> {
        match val {
            0 => Ok(Self::BigInt),
            1 => Ok(Self::Int),
            2 => Ok(Self::Boolean),
            3 => Ok(Self::Varchar),
            _ => Err(Error::CorruptPage(format!(
                "invalid DataType discriminant: {}",
                val
            ))),
        }
    }
}

/// A concrete, physical value or data stored inside a Tuple.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    BigInt(i64),
    Int(i32),
    Null,
    Boolean(bool),
    Varchar(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::BigInt(val) => write!(f, "{}", val),
            Value::Int(val) => write!(f, "{}", val),
            Value::Null => write!(f, "NULL"),
            Value::Boolean(val) => write!(f, "{}", val),
            Value::Varchar(val) => write!(f, "{}", val),
        }
    }
}

impl Value {
    /// Returns the string from the `Varchar` type.
    pub fn varchar_to_str(&self) -> Option<&str> {
        if let Value::Varchar(val) = self {
            Some(val)
        } else {
            None
        }
    }

    /// Returns the value from the `BigInt` type.
    pub fn bigint_to_i64(&self) -> Option<i64> {
        if let Value::BigInt(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the value from the `Int` type.
    pub fn int_to_i32(&self) -> Option<i32> {
        if let Value::Int(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the value from the `Boolean` type.
    pub fn boolean_to_bool(&self) -> Option<bool> {
        if let Value::Boolean(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Computes 64 bit BTree routing key from any supported value type. Uses xor based
    /// order-preserving conversion for numbers hashes for strings.
    pub fn to_index_key(&self) -> u64 {
        match self {
            // Flip the highest bit to preserve sorting order when converting from
            // signed to unsigned.
            Value::BigInt(val) => (*val as u64) ^ (1 << 63),
            Value::Int(val) => ((*val as i64) as u64) ^ (1 << 63),
            Value::Null => 0,
            Value::Boolean(val) => {
                if *val {
                    1
                } else {
                    0
                }
            }
            Value::Varchar(val) => {
                let mut hasher = hash_map::DefaultHasher::new();
                val.hash(&mut hasher);
                hasher.finish()
            }
        }
    }
}
