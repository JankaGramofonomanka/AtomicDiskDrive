mod domain;
mod atomic_register_public;
mod sectors_manager_public;
mod transfer_public;
mod register_client_public;
mod stable_storage_public;
mod atomic_storage;
mod constants;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;

use tokio::net::TcpListener;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use tokio::fs::create_dir;
use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;

pub async fn run_register_process(config: Configuration) {

    // TODO: what to do with the unwraps?

    let self_rank = config.public.self_rank;
    let (self_host, self_port) = &config.public.tcp_locations[(self_rank - 1) as usize];

    let address = format!("{}:{}", self_host, self_port);
    let listener = TcpListener::bind(address).await.unwrap();

    let mut register_process = RegisterProcess::new(listener, config).await;
    
    loop {
        register_process.process_command().await;
    }

}



struct RegisterProcess {
    self_rank: u8,
    listener: TcpListener,

    // TODO: more atomic registers
    atomic_register: Box<dyn AtomicRegister>,

    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],

    task_handles: Vec<JoinHandle<()>>
}


impl RegisterProcess {

    async fn new(listener: TcpListener, config: Configuration) -> Self {

        // TODO: what to do with the unwraps?

        let hmac_system_key = config.hmac_system_key;
        let hmac_client_key = config.hmac_client_key;

        let processes_count = config.public.tcp_locations.len();

        // build register client
        let register_client = Arc::new(
            RCModule::new(config.public.tcp_locations, hmac_system_key)
        );

        // build sectors manager
        let mut sectors_dir = config.public.storage_dir.clone();
        sectors_dir.push("sectors");
        if !sectors_dir.is_dir() {
            create_dir(sectors_dir.clone()).await.unwrap();
        }
        let sectors_manager = build_sectors_manager(sectors_dir);

        let self_rank = config.public.self_rank;

    

        // build stable sotrage
        // TODO: should this be global or per atomic register
        let mut metadata_dir = config.public.storage_dir.clone();
        metadata_dir.push("ar_metadata");
        if !metadata_dir.is_dir() {
            create_dir(metadata_dir.clone()).await.unwrap();
        }
        let metadata = Storage::new(metadata_dir).await;
        
        let atomic_register = build_atomic_register(
            self_rank,
            Box::new(metadata),
            register_client.clone(),
            sectors_manager.clone(),
            processes_count,
        ).await;
    

        RegisterProcess {
            self_rank: self_rank,
            listener: listener,
            atomic_register: atomic_register,
            hmac_system_key: hmac_system_key,
            hmac_client_key: hmac_client_key,
            task_handles: vec![],
        }

    }

    async fn process_command(&mut self) {
        let (mut stream, _) = self.listener.accept().await.unwrap();

        // TODO: unwrap?
        let (cmd, valid) = deserialize_register_command(
            &mut stream,
            &self.hmac_system_key,
            &self.hmac_client_key,
        ).await.unwrap();

        if !valid {
            unimplemented!();
            return;
        }


        // TODO: spawn tasks and put their handles to `self.task_handles`
        //let handle = match cmd {
        match cmd {
            RegisterCommand::System(cmd) => {
                //tokio::spawn(self.atomic_register.system_command(cmd))
                self.atomic_register.system_command(cmd).await;
            },
            
            RegisterCommand::Client(cmd) => {
                let hmac_key = self.hmac_client_key.clone();
                let operation_complete: Box<
                    dyn FnOnce(OperationComplete) 
                            -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>>
                        + core::marker::Send
                        + core::marker::Sync,
                > = Box::new(move |result| {
                    Box::pin(async move {
                        serialize_response(&result, &mut stream, &hmac_key).await;
                    })
                });
                //tokio::spawn(self.atomic_register.client_command(cmd, operation_complete))
                self.atomic_register.client_command(cmd, operation_complete).await;
            },
        };

        //self.task_handles.push(handle);
    }

}


