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
    Text = 11
}

pub enum ParserError {
    WrongFormat,
    StringParseFailed
}

impl Cell {

    pub fn new() -> Self {
        Cell { data: Vec::new() }
    }

    pub fn load(&mut self, data: Vec<u8>) {
        self.data = data;
    }

    pub fn string_to_bin(&mut self,raw_data: &String) {
        let mut bytes_array = raw_data.clone().into_bytes();

        let size = bytes_array.len() as u16;  

        self.data.push(CellType::String as u8);
        self.data.append(&mut size.to_le_bytes().to_vec());
        self.data.append(&mut bytes_array);
    }

    pub fn text_to_bin(&mut self, raw_data: &String) {
        let mut bytes_array = raw_data.clone().into_bytes();

        let size = bytes_array.len() as u32;  

        self.data.push(CellType::Text as u8);
        self.data.append(&mut size.to_le_bytes().to_vec());
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
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn unsigned_int_to_bin(&mut self, value: u32) {
        self.data.push(CellType::UnsignedInt as u8);
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn unsigned_bigint_to_bin(&mut self, value: u64) {
        self.data.push(CellType::UnsignedBigint as u8);
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn signed_tinyint_to_bin(&mut self, value: i8) {
        self.data.push(CellType::SignedTinyint as u8);
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn signed_smallint_to_bin(&mut self, value: i16) {
        self.data.push(CellType::SignedSmallint as u8);
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn signed_int_to_bin(&mut self, value: i32) {
        self.data.push(CellType::SignedInt as u8);
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn signed_bigint_to_bin(&mut self, value: i64) {
        self.data.push(CellType::SignedBigint as u8);
        self.data.append(&mut value.to_le_bytes().to_vec());
    }

    pub fn bin_to_string(&mut self) -> Result<String, ParserError> {
        if self.data[0] != (CellType::String as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 3;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        match String::from_utf8(bytes) {
            Ok(new_data) => Ok(new_data),
            Err(error) => Err(ParserError::StringParseFailed)
        }
    }

    pub fn bin_to_text(&mut self) -> Result<String, ParserError> {
        if self.data[0] != (CellType::Text as u8) {
            return Err(ParserError::WrongFormat)
        } 

        let mut bytes: Vec<u8> = Vec::new();
        let mut index: usize = 5;

        while index < self.data.len() {
            bytes.push(*self.data.get(index).unwrap());
            index += 1;
        }

        match String::from_utf8(bytes) {
            Ok(new_data) => Ok(new_data),
            Err(error) => Err(ParserError::StringParseFailed)
        }
    }

    pub fn bin_to_boolean(&mut self) -> Result<bool, ParserError> {
        if self.data[0] != (CellType::Boolean as u8) {
            return Err(ParserError::WrongFormat)
        } 

        Ok(self.data[1] == 1u8)
    }

}
