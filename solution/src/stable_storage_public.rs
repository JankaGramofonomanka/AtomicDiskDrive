
#[async_trait::async_trait]
/// A helper trait for small amount of durable metadata needed by the register algorithm
/// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
/// is durable, as one could expect.
pub trait StableStorage: Send + Sync {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    async fn get(&self, key: &str) -> Option<Vec<u8>>;
}

use std::path::PathBuf;
// You can add here other imports from std or crates listed in Cargo.toml.
use std::path::Path;
use std::collections::HashMap;
use tokio::fs::{File, rename};
use tokio::io::{AsyncWriteExt, AsyncReadExt, ErrorKind};

// You can add any private types, structs, consts, functions, methods, etc., you need.
const MAX_KEY_SIZE: usize = 255;
const MAX_VALUE_SIZE: usize = 65535;

const KEY_TO_LONG_ERROR: &str = "key size is to big (max 255 bytes)";
const VALUE_TO_LONG_ERROR: &str = "value size is to big (max 65535 bytes)";

type Nat = u64;


const KEYSFILE: &str = "keys";
const TMPFILE: &str = "tmpfile";

#[allow(non_snake_case)]
fn MK_FILENAME(num: Nat) -> String {
    format!("file{}", num)
}



struct Storage {
    dir: PathBuf,
    map: HashMap<String, Nat>,
    next_file_num: Nat,
}

impl Storage {

    async fn new(dir: PathBuf) -> Self {
        
        let mut res = Storage {
            dir: dir,
            map: HashMap::new(),
            next_file_num: 0,
        };

        res.restore_keys().await.unwrap();

        res
    }

    fn get_path(&self, filename: impl AsRef<Path>) -> PathBuf {
        let mut dir = self.dir.clone();
        dir.push(filename);
        dir
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Atomically stores `value` in `{self.dir}/{filename}`
    async fn store_atomic(&mut self, filename: impl AsRef<Path>, value: &[u8])
        -> Result<(), String> {

        let tmpfile_path = self.get_path(TMPFILE);

        {
            let mut tmpfile = File::create(tmpfile_path.clone()).await.unwrap();
            tmpfile.write_all(value).await.unwrap();
            tmpfile.sync_data().await.unwrap();
        }

        let file_path = self.get_path(filename);
        
        rename(tmpfile_path, file_path).await.unwrap();
        
        
        let dir = File::open(self.dir.clone()).await.unwrap();
        dir.sync_data().await.unwrap();

        Ok(())
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Inserts a new key to the runtime map and to the file with all keys
    async fn insert_key(&mut self, key: &str) -> Result<(), String> {
        let file_num = self.next_file_num;
        self.next_file_num += 1;

        // read keys file
        let keys_file_path = self.get_path(KEYSFILE);
        let mut keys_file = File::open(keys_file_path).await.unwrap();
        let mut keys_file_contents: Vec<u8> = vec![];
        keys_file.read_to_end(&mut keys_file_contents).await.unwrap();

        // push `key`
        let key = key.as_bytes();
        let key_length = key.len() as u8;

        keys_file_contents.push(key_length);
        keys_file_contents.append(&mut key.to_vec());

        // write to file
        self.store_atomic(KEYSFILE, &keys_file_contents[..]).await.unwrap();

        // update the runtime map
        let k = String::from_utf8(key.to_vec()).unwrap();
        self.map.insert(k, file_num);

        Ok(())

    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Fills the runtime map based on the file with keys
    async fn restore_keys(&mut self) -> Result<(), String> {
        
        let keys_file_path = self.get_path(KEYSFILE);
        
        match File::open(keys_file_path.clone()).await {
            
            Err(_) => {
                    File::create(keys_file_path).await.unwrap();
                },

            Ok(mut keys_file) => loop {

                    // read key length
                    let mut len_arr: [u8; 1] = [0; 1];
                    match keys_file.read_exact(&mut len_arr).await {
                        Ok(_) => {},

                        // break if EOF was reached
                        Err(e) => if e.kind() == ErrorKind::UnexpectedEof {
                                    break;
                                } else {
                                    panic!("{}", e);
                                }
                    };
                    let key_length = len_arr[0] as usize;
    
                    // read key
                    let mut key: Vec<u8> = vec![0; key_length];
                    keys_file.read_exact(&mut key).await.unwrap();
    
                    // store the key and file number in the runtime map
                    let k = String::from_utf8(key).unwrap();
                    self.map.insert(k, self.next_file_num);

                    self.next_file_num += 1;
                }
                
        }

        Ok(())
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Stores `value` in a file corresponding to `key`
    async fn store(&mut self, key: &str, value: &[u8]) -> Result<(), String> {

        if key.len() > MAX_KEY_SIZE {
            return Err(String::from(KEY_TO_LONG_ERROR));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(String::from(VALUE_TO_LONG_ERROR));
        }
        
        let k = String::from_utf8(key.as_bytes().to_vec()).unwrap();
        
        // If the key is new, store it
        if !self.map.contains_key(&k) { 
            self.insert_key(key).await.unwrap();
        }

        // atomically store the value
        let file_num = self.map[&k];
        let filename = MK_FILENAME(file_num);
        self.store_atomic(filename, value).await.unwrap();

        Ok(())
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Retrieves the value stored in a file corresponding to `key`
    async fn retrieve(&self, key: &str) -> Option<Vec<u8>> {
        
        let k = String::from_utf8(key.as_bytes().to_vec()).unwrap();
        
        // return `None` if the key is not in the map
        if !self.map.contains_key(&k) { 
            return None;
        }
        
        // otherwise read the file corresponding to `key` and return its contents
        let file_num = self.map[&k];
        let file_path = self.get_path(MK_FILENAME(file_num));

        let mut file = File::open(file_path).await.unwrap();
        let mut file_contents: Vec<u8> = vec![];
        file.read_to_end(&mut file_contents).await.unwrap();

        Some(file_contents)
        
    }
}


#[async_trait::async_trait]
impl StableStorage for Storage {
    
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        self.store(key, value).await
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.retrieve(key).await
    }

}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    Box::new(Storage::new(root_storage_dir).await)
}
