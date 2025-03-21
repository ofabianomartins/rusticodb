use std::fmt;
use std::ops;
use std::cmp::Ordering;
use std::string::ToString;

use crate::utils::ExecutionError;

#[derive(Debug)]
pub struct Cell {
    pub data: Vec<u8>
}

// Should be save in one byte
#[derive(Debug,Eq,Clone,PartialEq, Ord, PartialOrd)]
pub enum CellType {
    Undefined = 0,
    Null = 1,
    Boolean = 2,
    UnsignedTinyint = 3,
    UnsignedSmallint = 4,
    UnsignedInt = 5,
    UnsignedBigint = 6,
    SignedTinyint = 7,
    SignedSmallint = 8,
    SignedInt = 9,
    SignedBigint = 10,
    Varchar = 11,
    Text = 12,
}

impl Cell {

    pub fn new() -> Self {
        Cell { data: Vec::new() }
    }

    pub fn new_type(cell_type: CellType, values: Vec<u8>) -> Self {
        Cell { data: [vec![cell_type as u8], values].concat() }
    }

    pub fn load_cell(data: Vec<u8>) -> Self {
        Cell { data }
    }

    pub fn is_true(&self) -> bool {
        self.data[0] == (CellType::Boolean as u8) && self.data[1] == 1
    }

    pub fn get_bin(&self) -> Result<Vec<u8>, ExecutionError> {
        if self.data.len() <= 1 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::Varchar as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        let string_size = u16::from_be_bytes(byte_array);

        if self.data.len() != ((string_size + 3) as usize) {
            return Err(ExecutionError::WrongLength)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 3;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        return Ok(bytes);
    }

    pub fn bin_to_varchar(&self) -> Result<String, ExecutionError> {
        if self.data.len() <= 1 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::Varchar as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        let string_size = u16::from_be_bytes(byte_array);

        if self.data.len() != ((string_size + 3) as usize) {
            return Err(ExecutionError::WrongLength)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 3;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        match String::from_utf8(bytes) {
            Ok(new_data) => Ok(new_data),
            Err(_error) => Err(ExecutionError::StringParseFailed)
        }
    }

    pub fn bin_to_text(&self) -> Result<String, ExecutionError> {
        if self.data.len() <= 1 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::Text as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 4] = [
            self.data[1], self.data[2], self.data[3], self.data[4]
        ];
        let string_size = u32::from_be_bytes(byte_array);

        if self.data.len() != ((string_size + 5) as usize) {
            return Err(ExecutionError::WrongLength)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 5;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        match String::from_utf8(bytes) {
            Ok(new_data) => Ok(new_data),
            Err(_error) => Err(ExecutionError::StringParseFailed)
        }
    }

    pub fn bin_to_boolean(&self) -> Result<bool, ExecutionError> {
        if self.data.len() != 2 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::Boolean as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        Ok(self.data[1] == 1u8)
    }

    pub fn bin_to_unsigned_tinyint(&self) -> Result<u8, ExecutionError> {
        if self.data.len() != 2 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedTinyint as u8) {
            return Err(ExecutionError::WrongFormat)
        } 
        
        return Ok(self.data[1]);
    }

    pub fn bin_to_unsigned_smallint(&self) -> Result<u16, ExecutionError> {
        if self.data.len() != 3 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedSmallint as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        return Ok(u16::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_unsigned_int(&self) -> Result<u32, ExecutionError> {
        if self.data.len() != 5 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedInt as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 4] = [
            self.data[1], self.data[2], self.data[3], self.data[4]
        ];
        return Ok(u32::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_unsigned_bigint(&self) -> Result<u64, ExecutionError> {
        if self.data.len() != 9 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedBigint as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 8] = [
            self.data[1], self.data[2], self.data[3], self.data[4],
            self.data[5], self.data[6], self.data[7], self.data[8]
        ];
        return Ok(u64::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_signed_tinyint(&self) -> Result<i8, ExecutionError> {
        if self.data.len() != 2 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedTinyint as u8) {
            return Err(ExecutionError::WrongFormat)
        } 
        
        return Ok(self.data[1] as i8);
    }

    pub fn bin_to_signed_smallint(&self) -> Result<i16, ExecutionError> {
        if self.data.len() != 3 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedSmallint as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        return Ok(i16::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_signed_int(&self) -> Result<i32, ExecutionError> {
        if self.data.len() != 5 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedInt as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 4] = [
            self.data[1], self.data[2], self.data[3], self.data[4]
        ];
        return Ok(i32::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_signed_bigint(&self) -> Result<i64, ExecutionError> {
        if self.data.len() != 9 {
            return Err(ExecutionError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedBigint as u8) {
            return Err(ExecutionError::WrongFormat)
        } 

        let byte_array: [u8; 8] = [
            self.data[1], self.data[2], self.data[3], self.data[4],
            self.data[5], self.data[6], self.data[7], self.data[8]
        ];
        return Ok(i64::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn count_data_size(cell_type: u8) -> u32 {
        if cell_type == (CellType::Boolean as u8) || 
            cell_type == (CellType::UnsignedTinyint as u8) ||
            cell_type == (CellType::SignedTinyint as u8) {
            return 2;
        }
        if cell_type == (CellType::UnsignedSmallint as u8) ||
          cell_type == (CellType::SignedSmallint as u8) {
            return 3;
        }
        if cell_type == (CellType::UnsignedInt as u8) ||
            cell_type == (CellType::SignedInt as u8) {
            return 5;
        }
        if cell_type == (CellType::UnsignedBigint as u8) ||
            cell_type == (CellType::SignedBigint as u8) {
            return 9;
        }
        return 1
    }

    pub fn get_type(&self) -> CellType {
        if self.data.len() == 0 {
            return CellType::Null;
        }

        match self.data[0] {
            1 => CellType::Null,
            2 => CellType::Boolean,
            3 => CellType::UnsignedTinyint,
            4 => CellType::UnsignedSmallint,
            5 => CellType::UnsignedInt,
            6 => CellType::UnsignedBigint,
            7 => CellType::SignedTinyint,
            8 => CellType::SignedSmallint,
            9 => CellType::SignedInt,
            10 => CellType::SignedBigint,
            11 => CellType::Varchar,
            12 => CellType::Text,
            _ => CellType::Undefined
        }

    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        if self.data.len() == 0 || other.data.len() == 0 {
            return false;
        }
        if self.data[0] != other.data[0] {
            return false;
        }
        return self.data == other.data;
    }
}
impl Eq for Cell {}

impl PartialOrd for Cell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.data.len() == 0 || other.data.len() == 0 {
            return None;
        }
        if self.data[0] != other.data[0] {
            return None;
        }
        if self.bin_to_unsigned_bigint().unwrap() > other.bin_to_unsigned_bigint().unwrap() {
            return Some(Ordering::Greater);
        }
        if self.bin_to_unsigned_bigint().unwrap() < other.bin_to_unsigned_bigint().unwrap() {
            return Some(Ordering::Less); 
        }
        if self.data == other.data {
            return Some(Ordering::Equal);
        }
        return None;
    }
}
//impl Ord for Cell {}

impl ops::Add<Cell> for Cell {
    type Output = Cell;

    fn add(self, other: Cell) -> Cell {
        let result = self.bin_to_unsigned_bigint().unwrap() + other.bin_to_unsigned_bigint().unwrap();

        return Cell::new_type(CellType::UnsignedBigint, result.to_be_bytes().to_vec());
    }
}

impl ops::Sub<Cell> for Cell {
    type Output = Cell;

    fn sub(self, other: Cell) -> Cell {
        let result = self.bin_to_unsigned_bigint().unwrap() - other.bin_to_unsigned_bigint().unwrap();

        return Cell::new_type(CellType::UnsignedBigint, result.to_be_bytes().to_vec());
    }
}

impl ops::Mul<Cell> for Cell {
    type Output = Cell;

    fn mul(self, other: Cell) -> Cell {
        let result = self.bin_to_unsigned_bigint().unwrap() * other.bin_to_unsigned_bigint().unwrap();

        return Cell::new_type(CellType::UnsignedBigint, result.to_be_bytes().to_vec());
    }
}

impl ops::Div<Cell> for Cell {
    type Output = Cell;

    fn div(self, other: Cell) -> Cell {
        let result = self.bin_to_unsigned_bigint().unwrap() / other.bin_to_unsigned_bigint().unwrap();

        return Cell::new_type(CellType::UnsignedBigint, result.to_be_bytes().to_vec());
    }
}

impl ops::Not for Cell {
    type Output = Cell;

    fn not(self) -> Cell {
        let result = if self.bin_to_unsigned_bigint().unwrap() != 0 { 0u8 } else { 1u8 };

        return Cell::new_type(CellType::Boolean, vec![result]);
    }
}

impl ops::Neg for Cell {
    type Output = Cell;

    fn neg(self) -> Cell {
        let result: i64 = (-1 * (self.bin_to_unsigned_bigint().unwrap() as i64)).into();

        return Cell::new_type(CellType::SignedBigint, result.to_le_bytes().to_vec());
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self.to_string());
    }
}
