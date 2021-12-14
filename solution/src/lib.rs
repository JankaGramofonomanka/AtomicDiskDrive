mod domain;
mod atomic_register_public;
mod sectors_manager_public;
mod transfer_public;
mod register_client_public;
mod stable_storage_public;
mod atomic_storage;
mod constants;
mod register_process;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;
pub use register_process::*;

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::register_process::RegisterProcess;


pub async fn run_register_process(config: Configuration) {

    // TODO: what to do with the unwraps?

    let self_rank = config.public.self_rank;
    let (self_host, self_port) = &config.public.tcp_locations[(self_rank - 1) as usize];

    let address = format!("{}:{}", self_host, self_port);
    let listener = TcpListener::bind(address).await.unwrap();

    let register_process = RegisterProcess::new(config).await;
    loop {
        
        let (stream, _) = listener.accept().await.unwrap();
        let (read_stream, write_stream) = stream.into_split();

        tokio::spawn(
            register_process.clone().handle_stream(
                Arc::new(Mutex::new(read_stream)),
                Arc::new(Mutex::new(write_stream)),
            )
        );
    }

}
