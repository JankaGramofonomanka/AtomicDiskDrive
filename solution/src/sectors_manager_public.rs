
use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;

use crate::atomic_storage::AtomicStorage;
use std::collections::HashMap;
use crate::constants::*;
use std::convert::TryInto;
use crate::domain::*;
use tokio::sync::Mutex;
use std::ops::DerefMut;
use std::num::ParseIntError;
use tokio::runtime::Runtime;

#[async_trait::async_trait]
pub trait SectorsManager: Send + Sync {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec;

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
}

/// Path parameter points to a directory to which this method has exclusive access.
pub fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    Arc::new(SMModule::new(path))
}

const DEFAULT_TS: u64 = 0;
const DEFAULT_WR: u8 = 0;
const DEFAULT_METADATA: (u64, u8) = (DEFAULT_TS, DEFAULT_WR);

// ----------------------------------------------------------------------------
struct SMModule {
    storage: AtomicStorage,
    metadata: Mutex<HashMap<SectorIdx, (u64, u8)>>,
}

impl SMModule {
    fn new(path: PathBuf) -> Self {

        /*
        let sectors_manager = SMModule {
            storage: AtomicStorage::new(&path),
            metadata: Mutex::new(HashMap::new()),
        };

        let dir = std::fs::read_dir(&path).unwrap();

        for entry in dir {
            let entry = entry.unwrap();
            let filename = entry.file_name().into_string().unwrap();
            match SMModule::decode_filename(filename.as_str()) {
                
                Ok((idx, timestamp, write_rank)) => {

                    // TODO: this panics because this function is called inside 
                    // asynchronous code.
                    Runtime::new().unwrap().block_on(
                        sectors_manager.store_metadata(idx, timestamp, write_rank)
                    );
                },

                Err(_) => {
                    // This means the file is a leftover temporary file and 
                    // should be removed.
                    std::fs::remove_file(entry.path()).unwrap();
                },
            }
        }

        
        sectors_manager
        */

        let mut metadata = HashMap::new();

        let dir = std::fs::read_dir(&path).unwrap();

        for entry in dir {
            let entry = entry.unwrap();
            let filename = entry.file_name().into_string().unwrap();
            match SMModule::decode_filename(filename.as_str()) {
                
                Ok((idx, timestamp, write_rank)) => {

                    // The same as `SMModule::store_metadata` but synchronously

                    let stored = match metadata.get(&idx) {
                        None => None,
                        Some((ts, wr)) => Some((*ts, *wr)),
                    };
                    match stored {
                        None => {
                            metadata.insert(idx, (timestamp, write_rank));
                        }
                        Some((ts, wr)) =>

                            if ts > timestamp || ts == timestamp && wr > write_rank {
                                
                                if entry.path().is_file() {
                                    std::fs::remove_file(entry.path()).unwrap();
                                }

                            } else if (ts, wr) != (timestamp, write_rank) {
            
                                metadata.insert(idx, (timestamp, write_rank));
                                let old_filename = SMModule::encode_filename(idx, ts, wr);
                                let mut old_path = path.clone();
                                old_path.push(old_filename);

                                if entry.path().is_file() {
                                    std::fs::remove_file(old_path).unwrap();
                                }
                                
                            }
                    }
                    
                },

                Err(_) => {
                    // This means the file is a leftover temporary file and 
                    // should be removed.
                    std::fs::remove_file(entry.path()).unwrap();
                },
            }
        }

        
        SMModule {
            storage: AtomicStorage::new(&path),
            metadata: Mutex::new(metadata),
        }
        


    }

    fn encode_filename(idx: SectorIdx, ts: u64, wr: u8) -> String {

        // u64 has 16 hexadecimal digits, u8 has 2.
        format!("{:016x}{:016x}{:02x}", idx, ts, wr)
    }

    fn decode_filename(filename: &str) -> Result<(SectorIdx, u64, u8), ParseIntError> {
        
        let encoded_id = &filename[ 0..16];
        let encoded_ts = &filename[16..32];
        let encoded_wr = &filename[32..34];

        let id = u64::from_str_radix(encoded_id, 16)?;
        let ts = u64::from_str_radix(encoded_ts, 16)?;
        let wr = u8 ::from_str_radix(encoded_wr, 16)?;

        Ok((id, ts, wr))
    }

    async fn write_sector_data(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        
        let (data, timestamp, write_rank) = sector;
        let filename = SMModule::encode_filename(idx, *timestamp, *write_rank);

        match self.get_metadata(idx).await {
            None => {},
            Some((ts, wr)) => {
                if ts > *timestamp { return; }
                else if ts == *timestamp && wr > *write_rank { return; }
            }
        }

        self.storage.store_atomic(filename, &data.0[..]).await.unwrap();

        self.store_metadata(idx, *timestamp, *write_rank).await;
    }

    async fn read_sector_data(&self, idx: SectorIdx) -> (SectorVec, u64, u8) {
        match self.get_metadata(idx).await {
            None            => (SectorVec(vec![0; SECTOR_SIZE]), DEFAULT_TS, DEFAULT_WR),
            Some((ts, wr))  => {
                
                let filename = SMModule::encode_filename(idx, ts, wr);
                
                let value = self.storage.read(filename).await;
                match value {
                    None                => (SectorVec(vec![0; SECTOR_SIZE]), DEFAULT_TS, DEFAULT_WR),
                    Some(sector_data)   => (SectorVec(sector_data), ts, wr),
                }
            }
        }
        
    }

    async fn store_metadata(&self, idx: SectorIdx, timestamp: u64, write_rank: u8) {
        let mut metadata_guard = self.metadata.lock().await;
        let metadata = metadata_guard.deref_mut();
        
        let stored = match metadata.get(&idx) {
            None => None,
            Some((ts, wr)) => Some((*ts, *wr)),
        };

        match stored {
            None => {
                metadata.insert(idx, (timestamp, write_rank));
            },

            Some((ts, wr)) => {
                if ts > timestamp || ts == timestamp && wr > write_rank {
                    /* This means that a newer version of sector data was stored
                     * and therefore we should remove the old sector data (which 
                     * is stored in a different file, the one consistent with 
                     * the metadata we are trying to store) ) and not store the 
                     * older metadata
                     */
                    let filename = SMModule::encode_filename(idx, timestamp, write_rank);
                    self.storage.remove(filename).await;
                } else if (ts, wr) != (timestamp, write_rank) {

                    // This means we are overwriting older metadata and 
                    // therefore we have to also remove old sector data.
                    metadata.insert(idx, (timestamp, write_rank));
                    let filename = SMModule::encode_filename(idx, ts, wr);
                    self.storage.remove(filename).await;
                    
                }
            },
        }
        
    }

    async fn get_metadata(&self, idx: SectorIdx) -> Option<(u64, u8)> {
        let mut metadata_guard = self.metadata.lock().await;
        let metadata = metadata_guard.deref_mut();
        match metadata.get(&idx) {
            None            => None,
            Some((ts, wr))  => Some((*ts, *wr)),
        }
    }

}

#[async_trait::async_trait]
impl SectorsManager for SMModule {

    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let (data, _, _) = self.read_sector_data(idx).await;
        data
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        match self.get_metadata(idx).await {
            None => DEFAULT_METADATA,
            Some(data) => data,
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        self.write_sector_data(idx, sector).await;
    }

}
