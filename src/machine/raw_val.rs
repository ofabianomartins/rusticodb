use std::fmt;
use std::mem;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize)]
pub enum RawVal {
    Int(u64),
    Float(OrderedFloat<f64>),
    Str(String),
    Null,
}

impl RawVal {
    pub fn heap_size_of_children(&self) -> usize {
        match *self {
            RawVal::Int(_) => 0,
            RawVal::Str(ref s) => s.capacity() * mem::size_of::<u8>(),
            RawVal::Null => 0,
            RawVal::Float(_) => 0,
        }
    }

    pub fn to_vec_u8(&self) -> Vec<u8>{
        match *self {
            RawVal::Null => vec!(0),
            RawVal::Int(i) => i.to_be_bytes().to_vec(),
            RawVal::Str(ref s) => s.as_bytes().to_vec(),
            RawVal::Float(x) => x.to_be_bytes().to_vec()
        }
    }
}

impl fmt::Display for RawVal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RawVal::Null => write!(f, "null"),
            RawVal::Int(i) => write!(f, "{}", i),
            RawVal::Str(ref s) => write!(f, "\"{}\"", s),
            RawVal::Float(x) => write!(f, "{:e}", x),
        }
    }
}

pub mod syntax {
    pub use super::RawVal::{Int, Null};

    #[allow(non_snake_case)]
    pub fn Str(s: &str) -> super::RawVal {
        super::RawVal::Str(s.to_string())
    }

    #[allow(non_snake_case)]
    pub fn Float(x: f64) -> super::RawVal {
        super::RawVal::Float(x.into())
    }
}

impl From<f64> for RawVal {
    fn from(val: f64) -> Self {
        RawVal::Float(OrderedFloat(val))
    }
}

impl From<String> for RawVal {
    fn from(val: String) -> Self {
        RawVal::Str(val)
    }
}

impl From<()> for RawVal {
    fn from(_: ()) -> Self {
        RawVal::Null
    }
}

impl<T: Into<RawVal>> From<Option<T>> for RawVal {
    fn from(val: Option<T>) -> Self {
        match val {
            Some(val) => val.into(),
            None => RawVal::Null,
        }
    }
}

impl<'a> From<&'a str> for RawVal {
    fn from(val: &str) -> RawVal {
        RawVal::Str(val.to_string())
    }
}

impl From<u64> for RawVal {
    fn from(val: u64) -> RawVal {
        RawVal::Int(val)
    }
}
