
use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;

use crate::atomic_storage::AtomicStorage;
use std::collections::HashMap;
use crate::constants::*;
use std::convert::TryInto;

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



// ----------------------------------------------------------------------------
struct SMModule {
    storage: AtomicStorage,
    //metadata: HashMap<SectorIdx, (u64, u8)>,
}

impl SMModule {
    fn new(path: PathBuf) -> Self {
        SMModule {
            storage: AtomicStorage::new(path),
            //metadata: HashMap::new(),
        }
    }

    fn mk_filename(idx: SectorIdx) -> String {
        format!("sec{}", idx)
    }

    //async fn read_sector_data(&mut self, idx: SectorIdx) -> (SectorVec, u64, u8) {}

    async fn write_sector_data(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let (data, timestamp, write_rank) = sector;
        
        let sector_data = [
            &timestamp.to_be_bytes()[..],
            &[*write_rank],
            &data.0[..],
        ].concat();

        let filename = SMModule::mk_filename(idx);

        self.storage.store_atomic(filename, &sector_data[..]).await.unwrap();

        //self.metadata.insert(idx, (*timestamp, *write_rank));
    }

    async fn read_sector_data(&self, idx: SectorIdx) -> (SectorVec, u64, u8) {
        let filename = SMModule::mk_filename(idx);
        
        // TODO: remove the `read` method and read the file directly here
        let mut value = self.storage.read(filename, None).await;
        match value {
            None => {
                let data = vec![0; SECTOR_SIZE];
                return (SectorVec(data), 0, 0);
            }
            
            Some(mut sector_data) => {
                let timestamp_as_bytes: Vec<u8> = sector_data.drain(..8).collect();
                let timestamp = u64::from_be_bytes(timestamp_as_bytes.try_into().unwrap());
                
                let write_rank_as_bytes: Vec<u8> = sector_data.drain(..1).collect();
                let write_rank: u8 = write_rank_as_bytes[0];


                return (SectorVec(sector_data), timestamp, write_rank);
            }
        }
    }

    async fn read_sector_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let filename = SMModule::mk_filename(idx);
        
        // TODO: remove the `read` method and read the file directly here
        let mut value = self.storage.read(filename, Some(9)).await;
        match value {
            None => {
                return (0, 0);
            }
            
            Some(mut metadata) => {
                let write_rank = metadata.pop().unwrap();
                let timestamp = u64::from_be_bytes(metadata.try_into().unwrap());
                return (timestamp, write_rank);
            }
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
        self.read_metadata(idx).await
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        self.write_sector_data(idx, sector).await;
    }

}
