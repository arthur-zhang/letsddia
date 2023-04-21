use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;

use crate::bitcask::BitCaskResult;
use crate::block::Block;

pub fn format_dat_file_name(file_id: u32) -> String {
    format!("{:0>9}.dat", file_id)
}

pub fn format_idx_file_name(file_id: u32) -> String {
    format!("{:0>9}.idx", file_id)
}

pub fn now_ts() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32
}

pub fn block_crc(block: &Block) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&block.tstamp.to_le_bytes());
    hasher.update(&block.ksz.to_le_bytes());
    hasher.update(&block.value_sz.to_le_bytes());
    hasher.update(&block.key);
    hasher.update(&block.value);
    hasher.finalize()
}

mod file_name_utils {
    use std::path::Path;

    pub fn get_file_name_without_extension(path: &Path) -> std::io::Result<&str> {
        path.file_stem()
            .and_then(|path| path.to_str())
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::Other,
                "parse error",
            ))
    }

    pub fn get_file_name(path: &Path) -> std::io::Result<&str> {
        path.file_name()
            .and_then(|path| path.to_str())
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::Other,
                "parse error",
            ))
    }
}

pub mod file_utils {
    use std::fs::OpenOptions;
    use std::path::{Path, PathBuf};

    pub type FileNameFilter = dyn Fn(&Path) -> bool;

    pub fn open_file(path: &Path, readonly: bool) -> std::io::Result<std::fs::File> {
        if readonly {
            OpenOptions::new().read(true).open(path)
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
        }
    }

    pub fn list_files_in_dir(dir: &Path, filter: &FileNameFilter) -> std::io::Result<Vec<PathBuf>> {
        let mut files = std::fs::read_dir(dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| filter(path.as_path()))
            .collect::<Vec<_>>();
        files.sort();
        Ok(files)
    }
}

pub fn create_base_dir_if_not_exists(base_dir: &Path) -> std::io::Result<()> {
    if !base_dir.exists() {
        fs::create_dir_all(base_dir)?;
    }
    Ok(())
}

pub fn get_file_id_from_path(path: &Path) -> BitCaskResult<u32> {
    let file_name = file_name_utils::get_file_name_without_extension(path)?;
    let fid = file_name.parse::<u32>()?;
    Ok(fid)
}

fn dat_file_filter(path: &Path) -> bool {
    let re = Regex::new(r"^\d+\.dat$").unwrap();
    file_name_utils::get_file_name(path).map_or(false, |file_name| re.is_match(file_name))
}

pub fn get_dat_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = file_utils::list_files_in_dir(dir, &dat_file_filter)?;
    files.sort();
    Ok(files)
}

const DATAFILE_START_INDEX: u32 = 0;

pub fn get_next_id(dat_files: &Vec<PathBuf>) -> u32 {
    if dat_files.is_empty() {
        return DATAFILE_START_INDEX;
    }
    let last_dat_file = dat_files.last().unwrap();
    get_file_id_from_path(last_dat_file.as_path()).unwrap() + 1
}

pub fn delete_file(path: &PathBuf) -> std::io::Result<()> {
    fs::remove_file(path)
}

pub fn get_hint_from_dat_path(dat_file_path: &PathBuf) -> PathBuf {
    let mut hint_file_path = dat_file_path.clone();
    hint_file_path.set_extension("idx");
    hint_file_path
}

#[cfg(test)]
mod test_utils {
    use crc32fast::Hasher;

    use crate::block::Block;
    use crate::utils::{block_crc, get_dat_files};

    #[test]
    fn test_crc32() {
        let block = Block {
            crc: 0,
            tstamp: 123456,
            ksz: 5,
            value_sz: 5,
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };
        let crc1 = {
            let serialize_data = block.serialize();
            let mut hasher = Hasher::new();
            hasher.update(&serialize_data[4..]);
            hasher.finalize()
        };

        let crc2 = { block_crc(&block) };
        assert_eq!(crc1, crc2);
    }

    #[test]
    fn test_list_file() {
        let files = get_dat_files(&std::path::PathBuf::from(
            "/Users/arthur/CLionProjects/lets-ddia",
        ));
        for file in files.unwrap() {
            println!("{:?}", file);
        }
    }

    #[test]
    fn test_fid() {
        let fid = "000001234.dat";
        let id = str::parse::<u32>(fid).unwrap();
        assert_eq!(id, 1234);
    }
}
