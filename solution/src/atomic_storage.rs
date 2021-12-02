
use std::path::{Path, PathBuf};
use tokio::fs::{File, rename};
use tokio::io::{AsyncWriteExt, AsyncReadExt, ErrorKind};

const TMPFILE: &str = "tmpfile";


pub struct AtomicStorage {
    dir: PathBuf,
}

impl AtomicStorage {

    pub fn new(dir: PathBuf) -> Self {
        
        AtomicStorage {
            dir: dir,
        }
    }

    pub fn get_path(&self, filename: impl AsRef<Path>) -> PathBuf {
        let mut dir = self.dir.clone();
        dir.push(filename);
        dir
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Atomically stores `value` in `{self.dir}/{filename}`
    pub async fn store_atomic(&self, filename: impl AsRef<Path>, value: &[u8])
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

    pub async fn read(&self, filename: impl AsRef<Path>, num_bytes: Option<usize>) 
        -> Option<Vec<u8>> {
        
        let file_path = self.get_path(filename);

        if file_path.is_file() {
            let mut file = File::open(file_path).await.unwrap();
            let mut file_contents: Vec<u8> = vec![];

            match num_bytes {
                None    => {
                    file.read_to_end(&mut file_contents).await.unwrap();
                },

                Some(n) => {
                    file_contents = vec![0; n];
                    file.read_exact(&mut file_contents).await.unwrap();
                },
            }

            Some(file_contents)

        } else {

            None
        }
        
    }
}

