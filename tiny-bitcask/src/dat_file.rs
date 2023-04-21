use std::io::{Read, Seek};
use std::path::Path;

use crate::bitcask::{BitCaskResult, KeyRef, Value, ValueRef};
use crate::block::Block;
use crate::file_ext::{ReadExt, WriteBlock};
use crate::utils::*;

pub struct DatFileIter {
    pos: u32,
    file: std::fs::File,
}

impl DatFileIter {
    pub fn new(mut file: std::fs::File) -> Self {
        file.rewind().unwrap();
        Self { pos: 0, file }
    }
}

impl Iterator for DatFileIter {
    type Item = (u32, Block);

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.file.read_block_at(self.pos as u64).ok();

        let pos = self.pos;
        return match block {
            None => None,
            Some(block) => {
                self.pos += block.size() as u32;
                Some((pos, block))
            }
        };
    }
}

#[derive(Debug)]
pub struct DatFile {
    pub id: u32,
    pub path: std::path::PathBuf,
    file: std::fs::File,
    offset: u32,
}

impl DatFile {
    pub fn from_path(path: &Path, readonly: bool) -> BitCaskResult<Self> {
        let file_id = get_file_id_from_path(path)?;
        let mut file = file_utils::open_file(path, readonly)?;
        let offset = file.stream_position()? as u32;
        Ok(Self {
            id: file_id,
            path: path.to_path_buf(),
            file,
            offset,
        })
    }

    pub fn new(base_dir: &Path, file_id: u32, readonly: bool) -> BitCaskResult<Self> {
        let file_name = format_dat_file_name(file_id);
        let path = base_dir.join(file_name);
        let file = file_utils::open_file(&path, readonly)?;
        Ok(Self {
            path,
            file,
            offset: 0,
            id: file_id,
        })
    }

    pub fn iter(self) -> DatFileIter {
        DatFileIter::new(self.file)
    }
    pub fn write(&mut self, tstamp: u32, key: &KeyRef, value: &ValueRef) -> BitCaskResult<u32> {
        let block = Block::new(tstamp, key.to_vec(), value.to_vec());
        let file_offset = self.offset;
        let _ = self.file.write_block(&block)?;
        self.offset += block.size() as u32;
        Ok(file_offset)
    }

    pub fn read_value(&mut self, value_sz: u32, offset: u64) -> BitCaskResult<Value> {
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        let mut value = vec![0; value_sz as usize];
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    pub fn sync(&self) -> BitCaskResult<()> {
        self.file.sync_all().map_err(|e| e.into())
    }

    pub fn rename(&mut self, new_path: &std::path::PathBuf) -> BitCaskResult<()> {
        std::fs::rename(&self.path, new_path)?;
        self.path = new_path.to_path_buf();
        Ok(())
    }

    pub fn get_offset(&mut self) -> u32 {
        self.offset
    }
}
