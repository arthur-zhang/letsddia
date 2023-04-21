use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::bitcask::{Key, Value, REMOVE_TOMBSTONE};
use crate::utils;

pub const HEADER_SIZE: usize = 16;

pub struct Block {
    pub crc: u32,
    // u32 will cover time to 2106, it's enough
    pub tstamp: u32,
    pub ksz: u32,
    pub value_sz: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Block {
    pub fn new(tstamp: u32, key: Key, value: Value) -> Self {
        let mut block = Self {
            crc: 0,
            tstamp,
            ksz: key.len() as u32,
            value_sz: value.len() as u32,
            key,
            value,
        };
        block.crc = utils::block_crc(&block);
        block
    }
    pub fn is_removed(&self) -> bool {
        (self.value_sz == REMOVE_TOMBSTONE.len() as u32) && self.value == REMOVE_TOMBSTONE
    }
    pub fn size(&self) -> usize {
        4 * 4 + self.key.len() + self.value.len()
    }
    pub fn serialize(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(self.size());
        vec.write_u32::<LittleEndian>(self.crc).unwrap();
        vec.write_u32::<LittleEndian>(self.tstamp).unwrap();
        vec.write_u32::<LittleEndian>(self.ksz).unwrap();
        vec.write_u32::<LittleEndian>(self.value_sz).unwrap();
        vec.write_all(&self.key).unwrap();
        vec.write_all(&self.value).unwrap();
        vec
    }
}
