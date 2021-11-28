mod domain;
mod atomic_register_public;
mod sectors_manager_public;
mod transfer_public;
mod register_client_public;
mod stable_storage_public;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
}




