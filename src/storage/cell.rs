#[derive(Debug)]
pub struct Cell {
    pub data: Vec<u8>
}

impl Cell {

    pub fn new() -> Self {
        Cell { data: Vec::new() }
    }

    pub fn insert_string(&mut self,raw_data: &String) {
        let mut bytes_array = raw_data.clone().into_bytes();

        let size = bytes_array.len() as u64;  

        self.data.append(&mut size.to_le_bytes().to_vec());
        self.data.append(&mut bytes_array);
    }

}
