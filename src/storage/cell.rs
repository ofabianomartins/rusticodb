
#[derive(Debug)]
pub struct Cell {
    pub data: Vec<u8>
}

// Should be save in one byte
pub enum CellType {
    Boolean = 2,
    UnsignedTinyint = 3,
    UnsignedSmallint = 4,
    UnsignedInt = 5,
    UnsignedBigint = 6,
    SignedTinyint = 7,
    SignedSmallint = 8,
    SignedInt = 9,
    SignedBigint = 10,
    String = 1,
    Text = 11,
    Null = 12
}

#[derive(Debug)]
pub enum ParserError {
    WrongFormat,
    WrongLength,
    StringParseFailed
}

impl Cell {

    pub fn new() -> Self {
        Cell { data: Vec::new() }
    }

    pub fn load_cell(data: Vec<u8>) -> Self {
        Cell { data }
    }

    pub fn load(&mut self, data: Vec<u8>) {
        self.data = data;
    }

    pub fn null_to_bin(&mut self) {
        self.data.push(CellType::Null as u8);
    }

    pub fn string_to_bin(&mut self,raw_data: &String) {
        let mut bytes_array = raw_data.clone().into_bytes();

        let size = bytes_array.len() as u16;  

        self.data.push(CellType::String as u8);
        self.data.append(&mut size.to_be_bytes().to_vec());
        self.data.append(&mut bytes_array);
    }

    pub fn text_to_bin(&mut self, raw_data: &String) {
        let mut bytes_array = raw_data.clone().into_bytes();

        let size = bytes_array.len() as u32;  

        self.data.push(CellType::Text as u8);
        self.data.append(&mut size.to_be_bytes().to_vec());
        self.data.append(&mut bytes_array);
    }

    pub fn boolean_to_bin(&mut self, value: bool) {
        self.data.push(CellType::Boolean as u8);
        self.data.push(value as u8);
    }

    pub fn unsigned_tinyint_to_bin(&mut self, value: u8) {
        self.data.push(CellType::UnsignedTinyint as u8);
        self.data.push(value as u8);
    }

    pub fn unsigned_smallint_to_bin(&mut self, value: u16) {
        self.data.push(CellType::UnsignedSmallint as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn unsigned_int_to_bin(&mut self, value: u32) {
        self.data.push(CellType::UnsignedInt as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn unsigned_bigint_to_bin(&mut self, value: u64) {
        self.data.push(CellType::UnsignedBigint as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn signed_tinyint_to_bin(&mut self, value: i8) {
        self.data.push(CellType::SignedTinyint as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn signed_smallint_to_bin(&mut self, value: i16) {
        self.data.push(CellType::SignedSmallint as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn signed_int_to_bin(&mut self, value: i32) {
        self.data.push(CellType::SignedInt as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn signed_bigint_to_bin(&mut self, value: i64) {
        self.data.push(CellType::SignedBigint as u8);
        self.data.append(&mut value.to_be_bytes().to_vec());
    }

    pub fn bin_to_string(&mut self) -> Result<String, ParserError> {
        if self.data.len() <= 1 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::String as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        let string_size = u16::from_be_bytes(byte_array);

        if self.data.len() != ((string_size + 3) as usize) {
            return Err(ParserError::WrongLength)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 3;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        match String::from_utf8(bytes) {
            Ok(new_data) => Ok(new_data),
            Err(_error) => Err(ParserError::StringParseFailed)
        }
    }

    pub fn bin_to_text(&mut self) -> Result<String, ParserError> {
        if self.data.len() <= 1 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::Text as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 4] = [
            self.data[1], self.data[2], self.data[3], self.data[4]
        ];
        let string_size = u32::from_be_bytes(byte_array);

        if self.data.len() != ((string_size + 5) as usize) {
            return Err(ParserError::WrongLength)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 5;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        match String::from_utf8(bytes) {
            Ok(new_data) => Ok(new_data),
            Err(_error) => Err(ParserError::StringParseFailed)
        }
    }

    pub fn bin_to_boolean(&mut self) -> Result<bool, ParserError> {
        if self.data.len() != 2 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::Boolean as u8) {
            return Err(ParserError::WrongFormat)
        } 

        Ok(self.data[1] == 1u8)
    }

    pub fn bin_to_unsigned_tinyint(&mut self) -> Result<u8, ParserError> {
        if self.data.len() != 2 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedTinyint as u8) {
            return Err(ParserError::WrongFormat)
        } 
        
        return Ok(self.data[1]);
    }

    pub fn bin_to_unsigned_smallint(&mut self) -> Result<u16, ParserError> {
        if self.data.len() != 3 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedSmallint as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        return Ok(u16::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_unsigned_int(&mut self) -> Result<u32, ParserError> {
        if self.data.len() != 5 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedInt as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 4] = [
            self.data[1], self.data[2], self.data[3], self.data[4]
        ];
        return Ok(u32::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_unsigned_bigint(&mut self) -> Result<u64, ParserError> {
        if self.data.len() != 9 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::UnsignedBigint as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 8] = [
            self.data[1], self.data[2], self.data[3], self.data[4],
            self.data[5], self.data[6], self.data[7], self.data[8]
        ];
        return Ok(u64::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_signed_tinyint(&mut self) -> Result<i8, ParserError> {
        if self.data.len() != 2 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedTinyint as u8) {
            return Err(ParserError::WrongFormat)
        } 
        
        return Ok(self.data[1] as i8);
    }

    pub fn bin_to_signed_smallint(&mut self) -> Result<i16, ParserError> {
        if self.data.len() != 3 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedSmallint as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 2] = [self.data[1], self.data[2]];
        return Ok(i16::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_signed_int(&mut self) -> Result<i32, ParserError> {
        if self.data.len() != 5 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedInt as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let byte_array: [u8; 4] = [
            self.data[1], self.data[2], self.data[3], self.data[4]
        ];
        return Ok(i32::from_be_bytes(byte_array)); // or use `from_be_bytes` for big-endian
    }

    pub fn bin_to_signed_bigint(&mut self) -> Result<i64, ParserError> {
        if self.data.len() != 9 {
            return Err(ParserError::WrongLength)
        } 

        if self.data[0] != (CellType::SignedBigint as u8) {
            return Err(ParserError::WrongFormat)
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
        return 0
    }

    pub fn data_size(&mut self) -> u32 {
        if self.data.len() < 1 {
            return 0;
        }

        if self.data[0] == (CellType::String as u8) {
            let byte_array: [u8; 2] = [self.data[1], self.data[2]];
            return (u16::from_be_bytes(byte_array) as u32) + 3u32; // or use `from_be_bytes` for big-endian
        }

        if self.data[0] == (CellType::Text as u8) {
            let byte_array: [u8; 4] = [
                self.data[1], self.data[2], self.data[3], self.data[4]
            ];
            return u32::from_be_bytes(byte_array) + 5u32;
        }

        return Cell::count_data_size(self.data[0]);
    }
}
