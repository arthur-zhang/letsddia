use std::collections::BTreeMap;
use std::fs;
use std::fs::create_dir_all;
use std::io::ErrorKind;

use crate::block::HEADER_SIZE;
use crate::dat_file::DatFile;
use crate::errors::BitCaskError;
use crate::index_file::HintFile;
use crate::utils::*;

pub trait BitCask {
    fn open(dir_name: std::path::PathBuf, opts: Opts) -> BitCaskResult<Self>
    where
        Self: Sized;
    fn get(&self, key: &KeyRef) -> Option<Value>;
    fn put(&mut self, key: &KeyRef, value: &ValueRef) -> BitCaskResult<()>;
    fn delete(&mut self, key: &KeyRef) -> BitCaskResult<bool>;

    fn list_keys(&self) -> Vec<Key>;
    fn merge(&self) -> BitCaskResult<()>;
    fn sync(&self) -> BitCaskResult<()>;
    fn close(&self) -> BitCaskResult<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Opts {
    data_file_limit: u32,
}

impl Opts {
    pub fn new(data_file_limit: u32) -> Self {
        Self { data_file_limit }
    }
}

pub type KeyDir = BTreeMap<Vec<u8>, KeyDirEntry>;

#[derive(Clone)]
pub struct KeyDirEntry {
    pub file_id: u32,
    pub value_sz: u32,
    pub value_pos: u32,
    pub tstamp: u32,
}

pub struct BitCaskHandle {
    opts: Opts,
    base_dir: std::path::PathBuf,
    next_file_id: u32,
    key_dir: KeyDir,
    active_data_file: Option<DatFile>,
}

impl BitCaskHandle {
    fn load_files_in_dir(&mut self, dat_files: &mut Vec<std::path::PathBuf>) -> BitCaskResult<()> {
        if dat_files.is_empty() {
            return Ok(());
        }

        for path in dat_files {
            let dat_file = DatFile::from_path(path, true)?;
            let file_id = dat_file.id;
            let index_path = self.base_dir.join(format_idx_file_name(file_id));
            if index_path.exists() {
                let hint_file = HintFile::open_by_path(index_path, true)?;
                for record in hint_file.iter() {
                    self.key_dir.insert(
                        record.key,
                        KeyDirEntry {
                            file_id,
                            value_sz: record.value_sz,
                            value_pos: record.value_pos,
                            tstamp: record.tstamp,
                        },
                    );
                }
            } else {
                for (offset, block) in dat_file.iter() {
                    if block.is_removed() {
                        self.key_dir.remove(&block.key);
                        continue;
                    }
                    let maybe_entry = self.key_dir.get(&block.key);
                    match maybe_entry {
                        None => {
                            let entry = KeyDirEntry {
                                file_id,
                                value_sz: block.value_sz,
                                value_pos: offset + HEADER_SIZE as u32 + block.ksz,
                                tstamp: block.tstamp,
                            };
                            self.key_dir.insert(block.key, entry);
                        }
                        Some(entry) => {
                            let entry_ts = entry.tstamp;
                            if entry_ts < block.tstamp {
                                let entry = KeyDirEntry {
                                    file_id,
                                    value_sz: block.value_sz,
                                    value_pos: offset + HEADER_SIZE as u32 + block.ksz,
                                    tstamp: block.tstamp,
                                };
                                self.key_dir.insert(block.key, entry);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn create_new_dat_file(&mut self, file_id: u32) -> BitCaskResult<()> {
        let dat_file = DatFile::new(&self.base_dir, file_id, false)?;
        self.active_data_file = Some(dat_file);
        Ok(())
    }

    fn check_write(&mut self, data_len: u32) -> BitCaskResult<()> {
        match self.active_data_file {
            None => {
                self.create_new_dat_file(self.next_file_id)?;
            }
            Some(_) => {}
        }
        if self.active_data_file.is_none() {
        } else {
            let dat_file = self.active_data_file.as_mut().unwrap();
            // rotate
            if dat_file.get_offset() + HEADER_SIZE as u32 + data_len > self.opts.data_file_limit {
                self.next_file_id += 1;
                self.create_new_dat_file(self.next_file_id)?;
            }
        }
        Ok(())
    }
}

pub type BitCaskResult<T> = Result<T, BitCaskError>;

pub type Key = Vec<u8>;
pub type KeyRef = [u8];
pub type Value = Vec<u8>;
pub type ValueRef = [u8];

pub const REMOVE_TOMBSTONE: &[u8] = b"%_%_%_%<!(R|E|M|O|V|E|D)!>%_%_%_%_";

impl BitCask for BitCaskHandle {
    fn open(base_dir: std::path::PathBuf, opts: Opts) -> BitCaskResult<Self> {
        create_base_dir_if_not_exists(&base_dir)?;

        let mut dat_files = get_dat_files(&base_dir)?;

        let next_id = get_next_id(&dat_files);

        let mut db = BitCaskHandle {
            opts,
            base_dir,
            active_data_file: None,
            key_dir: Default::default(),
            next_file_id: next_id,
        };

        db.load_files_in_dir(&mut dat_files)?;
        Ok(db)
    }
    fn get(&self, key: &KeyRef) -> Option<Value> {
        self.key_dir
            .get(key)
            .map(|entry| {
                let file = DatFile::new(&self.base_dir, entry.file_id, true);
                if file.is_err() {
                    return None;
                }
                let mut file = file.unwrap();
                file.read_value(entry.value_sz, entry.value_pos as u64).ok()
            })
            .unwrap_or(None)
    }

    fn put(&mut self, key: &KeyRef, value: &ValueRef) -> BitCaskResult<()> {
        let data_len = key.len() + value.len();
        self.check_write(data_len as u32)?;

        let active_file = self.active_data_file.as_mut().unwrap();
        let tstamp = now_ts();
        let offset = active_file.write(tstamp, key, value)?;
        self.key_dir.insert(
            key.to_vec(),
            KeyDirEntry {
                file_id: active_file.id,
                value_sz: value.len() as u32,
                value_pos: offset + (HEADER_SIZE + key.len()) as u32,
                tstamp,
            },
        );
        Ok(())
    }

    fn delete(&mut self, key: &KeyRef) -> BitCaskResult<bool> {
        if !self.key_dir.contains_key(key) {
            return Ok(false);
        }
        self.put(key, REMOVE_TOMBSTONE)?;
        self.key_dir.remove(key);
        Ok(true)
    }

    fn list_keys(&self) -> Vec<Key> {
        self.key_dir.keys().cloned().collect()
    }

    fn merge(&self) -> BitCaskResult<()> {
        let dat_files = get_dat_files(&self.base_dir)?;
        println!("all dat files, size : {:?}", dat_files.len());
        if dat_files.len() <= 1 {
            return Ok(());
        }
        // Leave last file where current writes are landing
        let dat_files_to_merge = &dat_files[0..dat_files.len() - 1];
        println!("to merge dat files, size : {:?}", dat_files_to_merge.len());
        for path in dat_files_to_merge {
            println!("merging file {}", path.display());
        }

        let last_id = get_file_id_from_path(dat_files_to_merge.last().unwrap())?;

        println!("last id {}", last_id);
        // process latest file first
        let mut key_dir = KeyDir::new();

        let tmp_dir = self.base_dir.join("tmp");
        if tmp_dir.exists() && !tmp_dir.is_dir() {
            let _ = fs::remove_file(&tmp_dir);
        }
        if !tmp_dir.exists() {
            create_dir_all(&tmp_dir)?;
        }
        let tmp_file_path = tmp_dir.join(format_dat_file_name(last_id));

        let mut tmp_dat_file = DatFile::from_path(&tmp_file_path, false)?;
        let tmp_hint_path = self.base_dir.join(format_idx_file_name(last_id));
        let mut tmp_hint_file = HintFile::open_by_path(tmp_hint_path, false)?;

        for path in dat_files_to_merge.iter().rev() {
            let fid = get_file_id_from_path(path)?;
            let dat_file_iter = DatFile::from_path(path, true)?.iter();
            for (offset, block) in dat_file_iter {
                if key_dir.contains_key(&block.key) {
                    continue;
                }
                let key_dir_entry = KeyDirEntry {
                    file_id: fid,
                    value_sz: block.value_sz,
                    value_pos: offset + HEADER_SIZE as u32 + block.ksz,
                    tstamp: block.tstamp,
                };
                tmp_dat_file.write(block.tstamp, &block.key, &block.value)?;
                tmp_hint_file.put(&block.key, key_dir_entry.clone())?;
                key_dir.insert(block.key, key_dir_entry);
            }
        }

        tmp_dat_file.sync()?;
        tmp_hint_file.sync()?;

        tmp_dat_file.rename(&self.base_dir.join(format_dat_file_name(last_id)))?;
        tmp_hint_file.rename(&self.base_dir.join(format_idx_file_name(last_id)))?;

        // last file is the the to reserve file, so we don't delete it
        let files_to_delete = &dat_files_to_merge[0..dat_files_to_merge.len() - 1];

        for path in files_to_delete {
            if let Err(err) = delete_file(path) {
                if err.kind() != ErrorKind::NotFound {
                    println!("failed to delete file {}, err: {}", path.display(), err);
                }
            }
            let hint_file_path = get_hint_from_dat_path(path);
            if let Err(err) = delete_file(&hint_file_path) {
                if err.kind() != ErrorKind::NotFound {
                    println!(
                        "failed to delete file {}, err: {}",
                        hint_file_path.display(),
                        err
                    );
                }
            }
        }
        if let Err(err) = fs::remove_dir_all(&tmp_dir) {
            if err.kind() != ErrorKind::NotFound {
                println!(
                    "failed to delete tmp dir {}, err: {}",
                    tmp_dir.display(),
                    err
                );
            }
        }
        Ok(())
    }

    fn sync(&self) -> BitCaskResult<()> {
        if let Some(ref f) = self.active_data_file {
            f.sync()?
        }
        Ok(())
    }

    fn close(&self) -> BitCaskResult<()> {
        self.sync()
    }
}
