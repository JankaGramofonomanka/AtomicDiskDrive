
use std::path::{Path, PathBuf};
use tokio::fs::{File, rename, create_dir_all, remove_file};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::ffi::OsStr;


#[allow(non_snake_case)]
fn TMP_PATH(filepath: &PathBuf) -> PathBuf {
    
    let parent = PARENT(&filepath);
    let filename = match filepath.file_name() {
        Some(name)  => name,
        None        => OsStr::new(""),
    };
    let tmp_filename = format!("{}.tmp", filename.to_str().unwrap());

    let mut tmp_path = PathBuf::new();
    tmp_path.push(parent);
    tmp_path.push(tmp_filename);
    tmp_path
}

#[allow(non_snake_case)]
fn PARENT(filepath: &PathBuf) -> PathBuf {
    let parent = match filepath.parent() {
        Some(path)  => path,
        None        => Path::new("/"),
    };

    let mut parent_buf = PathBuf::new();
    parent_buf.push(parent);
    parent_buf
}

pub struct AtomicStorage {
    dir: PathBuf,
}

impl AtomicStorage {

    pub fn new(dir: impl AsRef<Path>) -> Self {
        
        let mut main_dir = PathBuf::new();
        main_dir.push(dir);
        
        AtomicStorage {
            dir: main_dir,
        }
    }

    pub fn get_path(&self, filename: impl AsRef<Path>) -> PathBuf {
        let mut dir = self.dir.clone();
        dir.push(filename);
        dir
    }
        

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    /// Atomically stores `value` in `{self.dir}/{relative_path}`.
    pub async fn store_atomic(&self, relative_path: impl AsRef<Path>, value: &[u8])
        -> Result<(), String> {

        let file_path = self.get_path(relative_path);
        let tmpfile_path = TMP_PATH(&file_path);
        let parent_dir = PARENT(&file_path);
        if !parent_dir.is_dir() {
            create_dir_all(&parent_dir).await.unwrap();
        }

        {
            let mut tmpfile = File::create(tmpfile_path.clone()).await.unwrap();
            tmpfile.write_all(value).await.unwrap();
            tmpfile.sync_data().await.unwrap();
        }

        rename(tmpfile_path, file_path).await.unwrap();
        
        let dir = File::open(parent_dir).await.unwrap();
        dir.sync_data().await.unwrap();

        Ok(())
    }

    /// Reads a file if it exists (and is indeed a file) and returns its content.
    /// Otherwise, returns `None`.
    pub async fn read(&self, relative_path: impl AsRef<Path>) 
        -> Option<Vec<u8>> {
        
        let file_path = self.get_path(relative_path);

        if file_path.is_file() {
            let mut file = File::open(file_path).await.unwrap();
            let mut file_contents: Vec<u8> = vec![];

            file.read_to_end(&mut file_contents).await.unwrap();

            Some(file_contents)

        } else {

            None
        }
    }

    /// Removes a file if it exists (and is indeed a file).
    pub async fn remove(&self, relative_path: impl AsRef<Path>) {
        let file_path = self.get_path(relative_path);

        if file_path.is_file() {
            remove_file(file_path).await.unwrap();

        } else {

            
        }
    }
}

