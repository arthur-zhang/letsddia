use crate::bitcask::BitCaskResult;
use crate::block::Block;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Write;

pub trait WriteBlock {
    fn write_block(&mut self, block: &Block) -> BitCaskResult<u64>;
}

impl WriteBlock for std::fs::File {
    fn write_block(&mut self, block: &Block) -> BitCaskResult<u64> {
        let vec = block.serialize();
        self.write_all(&vec)?;
        Ok(vec.len() as u64)
    }
}

pub trait ReadExt {
    fn read_block_at(&mut self, offset: u64) -> BitCaskResult<Block>;
}

impl<T> ReadExt for T
where
    T: std::io::Read + std::io::Seek,
{
    fn read_block_at(&mut self, offset: u64) -> BitCaskResult<Block> {
        self.seek(std::io::SeekFrom::Start(offset))?;

        let crc = self.read_u32::<LittleEndian>()?;
        let tstamp = self.read_u32::<LittleEndian>()?;
        let ksz = self.read_u32::<LittleEndian>()?;
        let value_sz = self.read_u32::<LittleEndian>()?;
        let mut key = vec![0; ksz as usize];
        self.read_exact(&mut key)?;
        let mut value = vec![0; value_sz as usize];
        self.read_exact(&mut value)?;

        Ok(Block {
            crc,
            tstamp,
            ksz,
            value_sz,
            key,
            value,
        })
    }
}
