mod bitcask;
mod block;
mod dat_file;
mod errors;
mod file_ext;
mod index_file;
mod utils;

#[cfg(test)]
mod tests {
    use crate::bitcask::{BitCask, BitCaskHandle, Opts};

    const TEST_DIR: &str = "/tmp/bitcask_test";

    #[test]
    fn test_open() {
        let db = BitCaskHandle::open(TEST_DIR.into(), Opts::default());
        assert!(db.is_ok());
        let db = db.unwrap();
        for x in db.list_keys() {
            println!("{:?}", String::from_utf8(x).unwrap());
        }
    }

    #[test]
    fn test_put() {
        let mut db = BitCaskHandle::open(TEST_DIR.into(), Opts::default()).unwrap();
        for i in 0..10 {
            let key = format!("hello#{i}");
            let world = format!("world#{i}");
            db.put(key.as_bytes(), world.as_bytes()).unwrap();
        }
        for i in 0..10 {
            let key = format!("hello#{i}");
            let value = db.get(key.as_bytes());
            assert_eq!(value.unwrap(), format!("world#{i}").as_bytes().to_vec());
        }
    }

    #[test]
    fn test_delete() {
        let opts = Opts::new(1024);
        {
            let mut db = BitCaskHandle::open(TEST_DIR.into(), opts).unwrap();
            db.delete("foo".as_bytes()).unwrap();
        }
        let db = BitCaskHandle::open(TEST_DIR.into(), opts).unwrap();
        let hello = db.get("foo".as_bytes());
        assert!(hello.is_none())
    }

    #[test]
    fn test_file_limit() {
        let opts = Opts::new(128);
        let mut db = BitCaskHandle::open(TEST_DIR.into(), opts).unwrap();
        for i in 0..10 {
            let key = format!("hello#{i}");
            let world = format!("world#{i}");
            db.put(key.as_bytes(), world.as_bytes()).unwrap();
        }
    }

    #[test]
    fn test_get() {
        let opts = Opts::new(128);
        let db = BitCaskHandle::open(TEST_DIR.into(), opts).unwrap();
        let res = db.get(b"hello#1").unwrap();
        println!("res: {:?}", String::from_utf8(res.clone()).unwrap());
        assert_eq!(res, b"world#1".to_vec());
    }

    #[test]
    fn test_merge() {
        let opts = Opts::new(20);

        let mut db = BitCaskHandle::open(TEST_DIR.into(), opts).unwrap();

        for i in 0..10 {
            let key = format!("hello#{i}");
            let world = format!("world#{i}");
            db.put(key.as_bytes(), world.as_bytes()).unwrap();
        }

        let res = db.merge();
        assert!(res.is_ok());
    }
}
