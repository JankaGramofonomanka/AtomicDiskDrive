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
use std::sync::Arc;
use tokio::fs::create_dir;
use std::future::Future;
use std::pin::Pin;
use std::io::ErrorKind;
use tokio::sync::Mutex;
use std::ops::DerefMut;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub async fn run_register_process(config: Configuration) {

    // TODO: what to do with the unwraps?

    let self_rank = config.public.self_rank;
    let (self_host, self_port) = &config.public.tcp_locations[(self_rank - 1) as usize];

    let address = format!("{}:{}", self_host, self_port);
    let listener = TcpListener::bind(address).await.unwrap();

    let atomic_register = Arc::new(Mutex::new(build_ar(&config).await));
    
    loop {
        
        let (stream, _) = listener.accept().await.unwrap();
        let (read_stream, write_stream) = stream.into_split();

        tokio::spawn(
            handle_stream(
                Arc::new(Mutex::new(read_stream)),
                Arc::new(Mutex::new(write_stream)),
                atomic_register.clone(),
                config.hmac_system_key,
                config.hmac_client_key,
                config.public.max_sector,
            )
        );
    }

}



async fn handle_stream<'a>(
    read_stream_ref: Arc<Mutex<OwnedReadHalf>>, 
    write_stream_ref: Arc<Mutex<OwnedWriteHalf>>, 
    atomic_register: Arc<Mutex<dyn AtomicRegister>>,
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
    max_sector: u64,
) {
    loop {
        
        let cmd;
        let valid;

        {
            let mut stream_guard = read_stream_ref.lock().await;
            let stream = stream_guard.deref_mut();
            let res = deserialize_register_command(
                &mut *stream,
                &hmac_system_key,
                &hmac_client_key,
            ).await;

            match res {

                Ok((c, v)) => {
                    cmd = c;
                    valid = v;
                }
                
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break; 
                    }
                    continue;
                },
            }
        }

        // invalid tag --------------------------------------------------------
        if !valid {
            match &cmd {
                // TODO: what in case of system commands?
                RegisterCommand::System(_) => {},

                RegisterCommand::Client(cmd) => {
                    let result = invalid_result(cmd, StatusCode::AuthFailure);
                    
                    let mut stream_guard = write_stream_ref.lock().await;
                    let stream = stream_guard.deref_mut();
                    serialize_response(&result, &mut *stream, &hmac_client_key).await.unwrap();
                }
            }
            
            return;
        }

        // invalid sector id --------------------------------------------------
        let sector_idx = get_sector_idx(&cmd);
        if sector_idx > max_sector {
            match &cmd {
                // TODO: what in case of system commands?
                RegisterCommand::System(_) => {},

                RegisterCommand::Client(cmd) => {
                    let result = invalid_result(cmd, StatusCode::InvalidSectorIndex);
                    
                    let mut stream_guard = write_stream_ref.lock().await;
                    let stream = stream_guard.deref_mut();
                    serialize_response(&result, &mut *stream, &hmac_client_key).await.unwrap();
                }
            }
            
            return;
        }


        // handling -----------------------------------------------------------
        match cmd {
            RegisterCommand::System(cmd) => {
                
                let mut ar = atomic_register.lock().await;
                ar.system_command(cmd).await;
            },
            
            RegisterCommand::Client(cmd) => {
                
                let hmac_key = hmac_client_key.clone();
                let write_stream_ref_clone = write_stream_ref.clone();
                let operation_complete: Box<
                    dyn FnOnce(OperationComplete) 
                            -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>>
                        + core::marker::Send
                        + core::marker::Sync,
                > = Box::new(move |result| {
                    Box::pin(async move {
                        
                        let mut stream_guard = write_stream_ref_clone.lock().await;
                        
                        let stream = stream_guard.deref_mut();
                        serialize_response(&result, &mut *stream, &hmac_key).await.unwrap();
                    })
                });

                let mut ar = atomic_register.lock().await;
                ar.client_command(cmd, operation_complete).await;
            },
        };

    } 

}




async fn build_ar(config: &Configuration)
    -> ARModule {

    // TODO: what to do with the unwraps?

    let processes_count = config.public.tcp_locations.len();

    // build register client
    let register_client = Arc::new(
        RCModule::new(config.public.tcp_locations.clone(), config.hmac_system_key)
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
    
    let atomic_register = ARModule::new(
        self_rank,
        Box::new(metadata),
        register_client.clone(),
        sectors_manager.clone(),
        processes_count,
    ).await;


    atomic_register

}




fn invalid_result(cmd: &ClientRegisterCommand, status: StatusCode) -> OperationComplete {
    OperationComplete {
        status_code: status,
        request_identifier: cmd.header.request_identifier,
        op_return: match cmd.content {
            ClientRegisterCommandContent::Read
                => OperationReturn::Read(ReadReturn { read_data: None }),

            ClientRegisterCommandContent::Write { data: _ }
                => OperationReturn::Write,
            
        },
    }
}


fn get_sector_idx(cmd: &RegisterCommand) -> SectorIdx {
    match cmd {
        RegisterCommand::System(cmd) => cmd.header.sector_idx,
        RegisterCommand::Client(cmd) => cmd.header.sector_idx,
    }
}

