use std::fmt;
use std::mem;
use std::ops;
//use std::cmp::Ordering;
use std::string::ToString;

//use ordered_float::OrderedFloat;
use bincode::serialize;
use serde::Serialize;
use serde::Deserialize;

use crate::storage::BLOCK_SIZE;

// Should be save in one byte
#[derive(Debug,Eq,Clone, Ord, PartialOrd, Serialize, Deserialize)]
pub enum Data {
    Undefined,
    Null,
    Boolean(bool),
    UnsignedTinyint(u8),
    UnsignedSmallint(u16),
    UnsignedInt(u32),
    UnsignedBigint(u64),
    SignedTinyint(i8),
    SignedSmallint(i16),
    SignedInt(i32),
    SignedBigint(i64),
    Varchar(String),
    Text(String)
}

impl Data {

    pub fn and(&self, other: &Data) -> bool {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => *a > 0 && *b > 0,
            _ => panic!("Not implemented")
        }
    }

    pub fn or(&self, other: &Data) -> bool {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => *a > 0 || *b > 0,
            _ => panic!("Not implemented")
        }
    }

    pub fn is_true(&self) -> bool {
        return Data::Boolean(true) == *self;
    }

    pub fn heap_size_of_children(&self) -> usize {
        match *self {
            Data::Boolean(_) => 0,
            Data::UnsignedTinyint(_) => 0,
            Data::UnsignedSmallint(_) => 0,
            Data::UnsignedInt(_) => 0,
            Data::UnsignedBigint(_) => 0,
            Data::SignedTinyint(_) => 0,
            Data::SignedSmallint(_) => 0,
            Data::SignedInt(_) => 0,
            Data::SignedBigint(_) => 0,
            Data::Varchar(ref s) => s.capacity() * mem::size_of::<u8>(),
            Data::Text(ref s) => s.capacity() * mem::size_of::<u8>(),
            Data::Null => 0,
            Data::Undefined => 0
        }
    }

    pub fn to_vec_u8(&self) -> Vec<u8>{
        match *self {
            Data::Null => vec!(0),
            Data::Undefined => vec!(0),
            Data::Boolean(i) => vec![i as u8],
            Data::UnsignedTinyint(i) => i.to_be_bytes().to_vec(),
            Data::UnsignedSmallint(i) => i.to_be_bytes().to_vec(),
            Data::UnsignedInt(i) => i.to_be_bytes().to_vec(),
            Data::UnsignedBigint(i) => i.to_be_bytes().to_vec(),
            Data::SignedTinyint(i) => i.to_be_bytes().to_vec(),
            Data::SignedSmallint(i) => i.to_be_bytes().to_vec(),
            Data::SignedInt(i) => i.to_be_bytes().to_vec(),
            Data::SignedBigint(i) => i.to_be_bytes().to_vec(),
            Data::Varchar(ref s) => s.as_bytes().to_vec(),
            Data::Text(ref s) => s.as_bytes().to_vec()
        }
    }

    pub fn to_string(&self) -> String {
        match *self {
            Data::Null => "null".to_string(),
            Data::Undefined => "undef".to_string(),
            Data::Boolean(i) => format!("{}", i),
            Data::UnsignedTinyint(i) => format!("{}", i),
            Data::UnsignedSmallint(i) => format!("{}", i),
            Data::UnsignedInt(i) => format!("{}", i),
            Data::UnsignedBigint(i) => format!("{}", i),
            Data::SignedTinyint(i) => format!("{}", i),
            Data::SignedSmallint(i) => format!("{}", i),
            Data::SignedInt(i) => format!("{}", i),
            Data::SignedBigint(i) => format!("{}", i),
            Data::Varchar(ref s) => format!("{}", s),
            Data::Text(ref s) => format!("{}", s),
        }
    }
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) =>  a == b,
            _ => panic!("Not implemented") 
        }
    }
}

impl ops::Add<Data> for Data {
    type Output = Data;

    fn add(self, other: Data) -> Data {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => Data::UnsignedBigint(a + b),
            _ => panic!("Not implemented") 
        }
    }
}

impl ops::Sub<Data> for Data {
    type Output = Data;

    fn sub(self, other: Data) -> Data {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => Data::UnsignedBigint(a - b),
            _ => panic!("Not implemented") 
        }
    }
}

impl ops::Mul<Data> for Data {
    type Output = Data;

    fn mul(self, other: Data) -> Data {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => Data::UnsignedBigint(a * b),
            _ => panic!("Not implemented") 
        }
    }
}

impl ops::Div<Data> for Data {
    type Output = Data;

    fn div(self, other: Data) -> Data {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => Data::UnsignedBigint(a / b),
            _ => panic!("Not implemented") 
        }
    }
}

impl ops::Not for Data {
    type Output = Data;

    fn not(self) -> Data {
        return match self {
            Data::UnsignedBigint(a) => Data::UnsignedBigint(if a != 0 { 0 } else { 1 }),
            _ => panic!("Not implemented") 
        }
    }
}

impl ops::Neg for Data {
    type Output = Data;

    fn neg(self) -> Data {
        return match self {
            Data::UnsignedBigint(a) => Data::SignedBigint(-1 * (a as i64)),
            _ => panic!("Not implemented") 
        }
    }
}

/*
impl From<f64> for Data {
    fn from(val: f64) -> Self {
        Data::Float(OrderedFloat(val))
    }
}

impl From<String> for Data {
    fn from(val: String) -> Self {
        Data::Str(val)
    }
}

impl From<()> for Data {
    fn from(_: ()) -> Self {
        Data::Null
    }
}

impl<T: Into<Data>> From<Option<T>> for Data {
    fn from(val: Option<T>) -> Self {
        match val {
            Some(val) => val.into(),
            None => Data::Null,
        }
    }
}

impl<'a> From<&'a str> for Data {
    fn from(val: &str) -> Data {
        Data::Str(val.to_string())
    }
}

impl From<u64> for Data {
    fn from(val: u64) -> Data {
        Data::Int(val)
    }
}
*/

pub type Tuple = Vec<Data>;

pub fn tuple_new() -> Tuple {
    Vec::new()
}

/*
pub fn tuple_get_cell(tuple: &Tuple, position: u16) -> Vec<u8> {
    let cell_count = tuple_cell_count(tuple);

    if position >= cell_count {
        return Vec::new();
    }

    let mut cell_index = 0;
    let mut position_index: usize = 4;
    let mut cell_size: u32;

    loop {
        if position_index >= tuple.len() {
            return Vec::new();
        }

        if tuple[position_index as usize] == (CellType::Varchar as u8) {
            let byte_array: [u8; 2] = [tuple[position_index + 1], tuple[position_index + 2]];
            cell_size = (u16::from_be_bytes(byte_array) as u32) + 3u32; // or use `from_be_bytes` for big-endian
        } else if tuple[position_index as usize] == (CellType::Text as u8) {
            let byte_array: [u8; 4] = [
                tuple[position_index + 1],
                tuple[position_index + 2],
                tuple[position_index + 3],
                tuple[position_index + 4]
            ];
            cell_size = (u32::from_be_bytes(byte_array) as u32) + 5u32; // or use `from_be_bytes` for big-endian
        } else if tuple[position_index as usize] == (CellType::Text as u8) {
            let byte_array: [u8; 4] = [
                tuple[position_index + 1], tuple[position_index + 2],
                tuple[position_index + 3], tuple[position_index + 4]
            ];
            cell_size = u32::from_be_bytes(byte_array) + 6u32; // or use `from_be_bytes` for big-endian
        } else {
            cell_size = Cell::count_data_size(tuple[position_index as usize]);
        }

        if cell_index >= cell_count || cell_index == position {
            break;
        }

        cell_index += 1;
        position_index += cell_size as usize;
    }
    let mut buffer_array: Vec<u8> = Vec::new();
    for n in position_index..(position_index + (cell_size as usize)) {
        buffer_array.push(tuple[n as usize]);
    }
    return buffer_array;
}
*/

pub fn tuple_to_raw_data(tuple: &Tuple) -> [u8; BLOCK_SIZE] {
    let mut buffer: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

    for (idx, cell) in serialize(tuple).iter().enumerate() {
        for (idx2, elem) in cell.iter().enumerate() {
            buffer[idx + idx2 + 2] = *elem;
        }
    }
    buffer
}

pub fn tuple_display(tuple: &Tuple) {
    print!("Tuple [(");

    for (idx, cell) in tuple.iter().enumerate() {
        print!("{:?}", cell);

        if idx != tuple.len() - 1 {
          print!(",");
        }
    }
    print!(")]")
}

////////////////////////////////////////////////////////////////////////////////

pub fn get_tuple_database(name: &String) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::Varchar(name.clone()));
    return tuple;
}

pub fn get_tuple_table(db_name: &String, name: &String) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::Varchar(db_name.clone()));
    tuple.push(Data::Varchar(name.clone()));
    tuple.push(Data::Varchar("table".to_string()));
    tuple.push(Data::Varchar("".to_string()));
    return tuple;
}

pub fn get_tuple_column(
    id: u64,
    db_name: &String,
    tbl_name: &String,
    name: &String,
    ctype: &String,
    not_null: bool,
    unique: bool,
    primary_key: bool,
    default: &String
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(id));
    tuple.push(Data::Varchar(db_name.clone()));
    tuple.push(Data::Varchar(tbl_name.clone()));
    tuple.push(Data::Varchar(name.clone()));
    tuple.push(Data::Varchar(ctype.clone()));
    tuple.push(Data::Boolean(not_null));
    tuple.push(Data::Boolean(unique));
    tuple.push(Data::Boolean(primary_key));
    tuple.push(Data::Varchar(default.clone()));
    return tuple;
}

pub fn get_tuple_column_without_id(
    db_name: &String,
    tbl_name: &String,
    name: &String,
    ctype: &String,
    not_null: bool,
    unique: bool,
    primary_key: bool,
    default: &String
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::Varchar(db_name.clone()));
    tuple.push(Data::Varchar(tbl_name.clone()));
    tuple.push(Data::Varchar(name.clone()));
    tuple.push(Data::Varchar(ctype.clone()));
    tuple.push(Data::Boolean(not_null));
    tuple.push(Data::Boolean(unique));
    tuple.push(Data::Boolean(primary_key));
    tuple.push(Data::Varchar(default.clone()));
    return tuple;
}

pub fn get_tuple_sequence(
    id: u64,
    db_name: &String,
    tbl_name: &String,
    col_name: &String,
    name: &String,
    next_id: u64
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::UnsignedBigint(id));
    tuple.push(Data::Varchar(db_name.clone()));
    tuple.push(Data::Varchar(tbl_name.clone()));
    tuple.push(Data::Varchar(col_name.clone()));
    tuple.push(Data::Varchar(name.clone()));
    tuple.push(Data::UnsignedBigint(next_id));
    return tuple;
}

pub fn get_tuple_sequence_without_id(
    db_name: &String,
    tbl_name: &String,
    col_name: &String,
    name: &String,
    next_id: u64
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::Varchar(db_name.clone()));
    tuple.push(Data::Varchar(tbl_name.clone()));
    tuple.push(Data::Varchar(col_name.clone()));
    tuple.push(Data::Varchar(name.clone()));
    tuple.push(Data::UnsignedBigint(next_id));
    return tuple;
}

pub fn get_tuple_index(
    db_name: &String,
    tbl_name: &String,
    col_name: &String,
    name: &String,
    itype: &String
) -> Tuple {
    let mut tuple: Tuple = tuple_new();
    tuple.push(Data::Varchar(db_name.clone()));
    tuple.push(Data::Varchar(tbl_name.clone()));
    tuple.push(Data::Varchar(col_name.clone()));
    tuple.push(Data::Varchar(name.clone()));
    tuple.push(Data::Varchar(itype.clone()));
    return tuple;
}


