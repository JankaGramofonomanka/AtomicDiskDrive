use std::path::PathBuf;
use crate::atomic_storage::AtomicStorage;


#[async_trait::async_trait]
/// A helper trait for small amount of durable metadata needed by the register algorithm
/// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
/// is durable, as one could expect.
pub trait StableStorage: Send + Sync {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    async fn get(&self, key: &str) -> Option<Vec<u8>>;
}


/* `AtomicStorage` does not satisfy the requirements of `StableStorage` from 
 * the small assignment, as it just stores the data under the file named `key` 
 * when `storage.put(key, value)` is called. The operation is however atomic 
 * and this is all we need for storing metadata of an atomic register.
*/
#[async_trait::async_trait]
impl StableStorage for AtomicStorage {
    
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        self.store_atomic(key, value).await
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.read(key).await
    }

}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    Box::new(AtomicStorage::new(root_storage_dir))
}
