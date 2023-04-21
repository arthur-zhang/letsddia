#[derive(Debug)]
pub enum BitCaskError {
    IoError,
    ParseError,
}

impl From<std::io::Error> for BitCaskError {
    fn from(_: std::io::Error) -> Self {
        BitCaskError::IoError
    }
}

impl From<std::num::ParseIntError> for BitCaskError {
    fn from(_: std::num::ParseIntError) -> Self {
        BitCaskError::ParseError
    }
}
