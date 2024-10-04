#[derive(Debug)]
pub struct Cell {
    pub data: Vec<u8>
}

pub enum CellType {

}

impl Cell {

    pub fn new() -> Self {
        Cell { data: Vec::new() }
    }

    pub fn string_to_bin(&mut self,raw_data: &String) {
        let mut bytes_array = raw_data.clone().into_bytes();

        let size = bytes_array.len() as u16;  

        self.data.append(&mut size.to_le_bytes().to_vec());
        self.data.append(&mut bytes_array);
    }

}
