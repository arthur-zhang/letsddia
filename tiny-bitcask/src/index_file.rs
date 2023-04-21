use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::bitcask::{BitCaskResult, Key, KeyDirEntry, KeyRef};

pub struct HintFile {
    path: PathBuf,
    file: std::fs::File,
}

impl HintFile {
    pub fn open_by_path(path: PathBuf, readonly: bool) -> BitCaskResult<Self> {
        let file = if readonly {
            OpenOptions::new().read(true).open(path.clone())?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(path.clone())?
        };

        Ok(Self { path, file })
    }

    pub fn put(&mut self, key: &KeyRef, entry: KeyDirEntry) -> BitCaskResult<()> {
        self.file.write_u32::<LittleEndian>(key.len() as u32)?;
        self.file.write_all(key)?;
        self.file.write_u32::<LittleEndian>(entry.value_sz)?;
        self.file.write_u32::<LittleEndian>(entry.value_pos)?;
        self.file.write_u32::<LittleEndian>(entry.tstamp)?;
        Ok(())
    }
    pub fn next_record(&mut self) -> BitCaskResult<Option<IndexRecord>> {
        let key_len = self.file.read_u32::<LittleEndian>()?;
        let mut key = vec![0; key_len as usize];
        self.file.read_exact(&mut key)?;
        let value_sz = self.file.read_u32::<LittleEndian>()?;
        let value_pos = self.file.read_u32::<LittleEndian>()?;
        let tstamp = self.file.read_u32::<LittleEndian>()?;
        Ok(Some(IndexRecord {
            key,
            value_sz,
            value_pos,
            tstamp,
        }))
    }
    pub fn iter(self) -> HintFileIter {
        HintFileIter { file: self }
    }

    pub fn rename(&self, new_path: &PathBuf) -> BitCaskResult<()> {
        std::fs::rename(&self.path, new_path)?;
        Ok(())
    }
    pub fn sync(&self) -> BitCaskResult<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

pub struct IndexRecord {
    pub key: Key,
    pub value_sz: u32,
    pub value_pos: u32,
    pub tstamp: u32,
}

pub struct HintFileIter {
    file: HintFile,
}

impl Iterator for HintFileIter {
    type Item = IndexRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.file.next_record().ok().flatten()
    }
}
