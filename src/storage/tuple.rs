use std::fmt;
use std::mem;
use std::ops;
//use std::cmp::Ordering;
use std::string::ToString;

use crate::utils::vec_u8_to_u16;
use crate::utils::vec_u8_to_u32;
use crate::utils::vec_u8_to_u64;
use crate::utils::vec_u8_to_i16;
use crate::utils::vec_u8_to_i32;
use crate::utils::vec_u8_to_i64;
use crate::utils::vec_u8_to_string;
use crate::utils::vec_u8_to_text;

// Should be save in one byte
#[derive(Debug,Eq,Clone, Ord, PartialOrd)]
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
            (Data::Boolean(a), Data::Boolean(b)) => *a && *b,
            other => panic!("Not implemented {:?}", other)
        }
    }

    pub fn or(&self, other: &Data) -> bool {
        return match (self, other) {
            (Data::UnsignedBigint(a), Data::UnsignedBigint(b)) => *a > 0 || *b > 0,
            (Data::Boolean(a), Data::Boolean(b)) => *a || *b,
            other => panic!("Not implemented {:?}", other)
        }
    }

    pub fn is_true(&self) -> bool {
        return Data::Boolean(true) == *self;
    }

    pub fn type_of_children(&self) -> u8 {
        match *self {
            Data::Null => 1,
            Data::Undefined => 2,
            Data::Boolean(_) => 3,
            Data::UnsignedTinyint(_) => 4,
            Data::UnsignedSmallint(_) => 5,
            Data::UnsignedInt(_) => 6,
            Data::UnsignedBigint(_) => 7,
            Data::SignedTinyint(_) => 8,
            Data::SignedSmallint(_) => 9,
            Data::SignedInt(_) => 10,
            Data::SignedBigint(_) => 11,
            Data::Varchar(_) => 12,
            Data::Text(_) => 13
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        match *self {
            Data::Boolean(_) => 1,
            Data::UnsignedTinyint(_) => 1,
            Data::UnsignedSmallint(_) => 2,
            Data::UnsignedInt(_) => 4,
            Data::UnsignedBigint(_) => 8,
            Data::SignedTinyint(_) => 1,
            Data::SignedSmallint(_) => 2,
            Data::SignedInt(_) => 4,
            Data::SignedBigint(_) => 8,
            Data::Varchar(ref s) => s.len() * mem::size_of::<u8>(),
            Data::Text(ref s) => s.len() * mem::size_of::<u8>(),
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
            Data::Varchar(ref s) => {
                let mut vecs = Vec::new();
                vecs.append(&mut (s.len() as u16).to_be_bytes().to_vec());
                vecs.append(&mut s.as_bytes().to_vec());
                vecs
            },
            Data::Text(ref s) => {
                let mut vecs = Vec::new();
                vecs.append(&mut (s.len() as u32).to_be_bytes().to_vec());
                vecs.append(&mut s.as_bytes().to_vec());
                vecs
            },
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
            (Data::UnsignedSmallint(a), Data::UnsignedSmallint(b)) =>  a == b,
            (Data::UnsignedInt(a), Data::UnsignedInt(b)) =>  a == b,
            (Data::UnsignedTinyint(a), Data::UnsignedTinyint(b)) =>  a == b,
            (Data::SignedBigint(a), Data::SignedBigint(b)) =>  a == b,
            (Data::SignedSmallint(a), Data::SignedSmallint(b)) =>  a == b,
            (Data::SignedInt(a), Data::SignedInt(b)) =>  a == b,
            (Data::SignedTinyint(a), Data::SignedTinyint(b)) =>  a == b,
            (Data::Boolean(a), Data::Boolean(b)) =>  a == b,
            (Data::Varchar(a), Data::Varchar(b)) =>  a == b,
            (Data::Text(a), Data::Text(b)) =>  *a == *b,
            (Data::Text(a), Data::Varchar(b)) =>  *a == *b,
            (Data::Varchar(a), Data::Text(b)) =>  *a == *b,
            (Data::Null, Data::Varchar(_)) => false,
            (Data::Null, Data::Boolean(_)) => false,
            (Data::Null, Data::UnsignedBigint(_)) => false,
            (Data::Null, Data::UnsignedTinyint(_)) => false,
            (Data::Null, Data::UnsignedSmallint(_)) => false,
            (Data::Null, Data::UnsignedInt(_)) => false,
            (Data::Null, Data::SignedBigint(_)) => false,
            (Data::Null, Data::SignedTinyint(_)) => false,
            (Data::Null, Data::SignedSmallint(_)) => false,
            (Data::Null, Data::SignedInt(_)) => false,
            (Data::Null, Data::Text(_)) => false,
            (Data::Null, Data::Null) => true,
            other => panic!("Not implemented {:?}", other) 
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

pub type Tuple = Vec<Data>;

pub fn tuple_new() -> Tuple {
    Vec::new()
}

pub fn tuple_size(tuple: &Tuple) -> usize {
    let mut tuple_size = 0;

    for (_, cell) in tuple.iter().enumerate() {
        tuple_size += cell.heap_size_of_children();
    }

    tuple_size
}

pub fn tuple_serialize(tuple: &Tuple) -> Vec<u8> {
    let mut header: Vec<u8> = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let mut buffer: Vec<u8> = Vec::new();

    for (_, cell) in tuple.iter().enumerate() {
        header.push(cell.type_of_children());
        match cell {
            Data::Null | Data::Undefined => {},
            _ => {
                body.append(&mut cell.to_vec_u8());
            }
        }
    }

    buffer.push(header.len() as u8);
    buffer.append(&mut header);
    buffer.append(&mut body);
    buffer
}

pub fn tuple_deserialize(buffer: &Vec<u8> ) -> Tuple {
    let mut tuple = Tuple::new();

    if buffer.len() > 0 {
        let cell_count: usize = buffer[0] as usize;
        let mut value_position: usize = cell_count+1;
       
        for idx in 0..cell_count {
            match buffer[idx + 1] {
                1 => tuple.push(Data::Null),
                2 => tuple.push(Data::Undefined),
                3 => { 
                    tuple.push(Data::Boolean(buffer[value_position] == 1));
                    value_position += 1;
                },
                4 => {
                    tuple.push(Data::UnsignedTinyint(buffer[value_position]));
                    value_position += 1;
                },
                5 => {
                    tuple.push(Data::UnsignedSmallint(vec_u8_to_u16(buffer, value_position)));
                    value_position += 2;
                },
                6 => {
                    tuple.push(Data::UnsignedInt(vec_u8_to_u32(buffer, value_position)));
                    value_position += 4;
                },
                7 => {
                    tuple.push(Data::UnsignedBigint(vec_u8_to_u64(buffer, value_position)));
                    value_position += 8;
                },
                8 => {
                    tuple.push(Data::SignedTinyint(buffer[value_position] as i8));
                    value_position += 1;
                },
                9 => {
                    tuple.push(Data::SignedSmallint(vec_u8_to_i16(buffer, value_position)));
                    value_position += 2;
                },
                10 => { 
                    tuple.push(Data::SignedInt(vec_u8_to_i32(buffer, value_position)));
                    value_position += 4;
                },
                11 => { 
                    tuple.push(Data::SignedBigint(vec_u8_to_i64(buffer, value_position)));
                    value_position += 8;
                },
                12 => { 
                    let string = vec_u8_to_string(buffer, value_position);
                    let string_size = string.len() + 2;
                    tuple.push(Data::Varchar(string));
                    value_position += string_size;
                },
                13 => { 
                    let string = vec_u8_to_text(buffer, value_position);
                    let string_size = string.len() + 4;
                    tuple.push(Data::Text(string));
                    value_position += string_size;
                },
                _ => {}
            }
        }
    }

    return tuple;
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


