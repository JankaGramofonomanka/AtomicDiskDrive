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

    let atomic_register = build_ar(&config).await;
    
    let mut iter = 0;
    loop {
        println!(
            "(proc_id: {}, stream_id: {}) new stream", 
            self_rank,
            iter, 
        );
        


        let (stream, _) = listener.accept().await.unwrap();
        let (read_stream, write_stream) = stream.into_split();

        tokio::spawn(
            handle_stream(
                Arc::new(Mutex::new(read_stream)),
                Arc::new(Mutex::new(write_stream)),
                atomic_register.clone(),
                self_rank,
                config.hmac_system_key,
                config.hmac_client_key,
                iter,
            )
        );
        
        iter += 1;
    }

}



async fn handle_stream<'a>(
    read_stream_ref: Arc<Mutex<OwnedReadHalf>>, 
    write_stream_ref: Arc<Mutex<OwnedWriteHalf>>, 
    atomic_register: Arc<Mutex<Box<dyn AtomicRegister>>>,
    self_rank: u8,
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
    stream_id: u32,
) {
    loop {
        
        let res;

        {
            let mut stream_guard;
            { stream_guard = read_stream_ref.lock().await; }
            println!(
                "(proc_id: {}, stream_id: {}) stream locked to read", 
                self_rank, 
                stream_id,
            );

            let stream = stream_guard.deref_mut();
            res = deserialize_register_command(
                &mut *stream,
                &hmac_system_key,
                &hmac_client_key,
            ).await;
        }

        println!(
            "(proc_id: {}, stream_id: {}) stream unlocked after read", 
            self_rank, 
            stream_id
        );

        match res {

            Ok((cmd, valid)) => {
                
                let cmd_type = cmd_type(&cmd);
                let cmd_sender = cmd_sender(&cmd);

                println!(
                    "(proc_id: {}, stream_id: {}) processing command `{}` from {}", 
                    self_rank, 
                    stream_id,
                    cmd_type,
                    cmd_sender,
                );

                match &cmd {
                    RegisterCommand::Client(cmd)
                        => println!(
                            "(proc_id: {}, stream_id: {}) request id: {}", 
                            self_rank, 
                            stream_id,
                            cmd.header.request_identifier
                        ),
                    
                    RegisterCommand::System(cmd)
                        => println!(
                            "(proc_id: {}, stream_id: {}) msg id: {}", 
                            self_rank, 
                            stream_id,
                            cmd.header.msg_ident
                        ),
                }

                if !valid {
                    match cmd {
                        // TODO: what in case of system commands?
                        RegisterCommand::System(_) => {},
                        
                        RegisterCommand::Client(cmd) => {
                            let result = invalid_tag_result(cmd);
                            {
                                println!("invalid hmac tag");
                                let mut stream_guard;
                                { stream_guard = write_stream_ref.lock().await; }
                                println!(
                                    "(proc_id: {}, stream_id: {}) stream locked to write (invalid tag)", 
                                    self_rank, 
                                    stream_id
                                );
                                
                                let stream = stream_guard.deref_mut();
                                serialize_response(&result, &mut *stream, &hmac_client_key).await.unwrap();
                            }
                            println!(
                                "(proc_id: {}, stream_id: {}) stream unlocked after write (invalid tag)", 
                                self_rank, 
                                stream_id
                            );
                        }
                    }
                    
                    return;
                }
        
        
                match cmd {
                    RegisterCommand::System(cmd) => {
                        
                        {
                            let mut ar;
                            { ar = atomic_register.lock().await; }
                            println!(
                                "(proc_id: {}, stream_id: {}) ar locked to handle `{}` from {}", 
                                self_rank, 
                                stream_id,
                                cmd_type,
                                cmd_sender,
                            );
                            ar.system_command(cmd).await;
                        }
                        println!(
                            "(proc_id: {}, stream_id: {}) ar unlocked after handle `{}` from {}", 
                            self_rank, 
                            stream_id,
                            cmd_type,
                            cmd_sender,
                        );
                    },
                    
                    RegisterCommand::Client(cmd) => {
                        let req_id = cmd.header.request_identifier;
        
                        let hmac_key = hmac_client_key.clone();
                        let write_stream_ref_clone = write_stream_ref.clone();
                        let operation_complete: Box<
                            dyn FnOnce(OperationComplete) 
                                    -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>>
                                + core::marker::Send
                                + core::marker::Sync,
                        > = Box::new(move |result| {
                            Box::pin(async move {
                                
                                println!(
                                    "(proc_id: {}, stream_id: {}) responding to request {}", 
                                    self_rank,
                                    stream_id,
                                    req_id, 
                                );

                                {
                                    let mut stream_guard;
                                    { stream_guard = write_stream_ref_clone.lock().await; }
                                    println!(
                                        "(proc_id: {}, stream_id: {}) stream locked to write", 
                                        self_rank, 
                                        stream_id
                                    );
                                    
                                    let stream = stream_guard.deref_mut();
                                    serialize_response(&result, &mut *stream, &hmac_key).await.unwrap();
                                }

                                println!(
                                    "(proc_id: {}, stream_id: {}) stream unlocked after write", 
                                    self_rank, 
                                    stream_id
                                );
                                println!(
                                    "(proc_id: {}, stream_id: {}) responded to request {}", 
                                    self_rank, 
                                    stream_id,
                                    req_id, 
                                );
                                
                                
                            })
                        });
        
                        {   
                            let mut ar ;
                            { ar = atomic_register.lock().await; }
                            println!(
                                "(proc_id: {}, stream_id: {}) ar locked to handle `{}` from {}", 
                                self_rank,  
                                stream_id,
                                cmd_type,
                                cmd_sender,
                            );
                            ar.client_command(cmd, operation_complete).await;
                        }
                        println!(
                            "(proc_id: {}, stream_id: {}) ar unlocked after handle `{}` from {}", 
                            self_rank,  
                            stream_id,
                            cmd_type,
                            cmd_sender,
                        );
                    },
                };
        
                println!(
                    "(proc_id: {}, stream_id: {}) end of `process_command`", 
                    self_rank, 
                    stream_id,
                );
            },

            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    println!(
                        "(proc_id: {}, stream_id: {}) connection closed", 
                        self_rank, 
                        stream_id,
                    );
                    break; 
                }
                println!(
                    "(proc_id: {}, stream_id: {}) ERROR: {}", 
                    self_rank, 
                    stream_id, e
                );
            },
        }

        println!(
            "(proc_id: {}, stream_id: {}) end of loop body", 
            self_rank, 
            stream_id,
        );
    } 

    println!(
        "(proc_id: {}, stream_id: {}) stream handled", 
        self_rank, 
        stream_id,
    );

}




async fn build_ar(config: &Configuration)
    -> Arc<Mutex<Box<dyn AtomicRegister>>> {

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
    
    let atomic_register = build_atomic_register(
        self_rank,
        Box::new(metadata),
        register_client.clone(),
        sectors_manager.clone(),
        processes_count,
    ).await;


    Arc::new(Mutex::new(atomic_register))    

}


fn cmd_type(cmd: &RegisterCommand) -> String {
    match cmd {
        RegisterCommand::System(cmd) => {
            match cmd.content {
                SystemRegisterCommandContent::ReadProc
                    => format!("ReadProc"),
                SystemRegisterCommandContent::Value { timestamp: _, write_rank: _, sector_data: _ }
                    => format!("Value"),
                SystemRegisterCommandContent::WriteProc { timestamp: _, write_rank: _, data_to_write: _ }
                    => format!("WriteProc"),
                SystemRegisterCommandContent::Ack
                    => format!("Ack"),
            }
        }
        
        RegisterCommand::Client(cmd) => {
            match cmd.content {
                ClientRegisterCommandContent::Read
                    => format!("Read"),
                ClientRegisterCommandContent::Write { data: _ }
                    => format!("Write"),
            }
        }
    }

}

fn invalid_tag_result(cmd: ClientRegisterCommand) -> OperationComplete {
    OperationComplete {
        status_code: StatusCode::AuthFailure,
        request_identifier: cmd.header.request_identifier,
        op_return: match cmd.content {
            ClientRegisterCommandContent::Read
                => OperationReturn::Read(ReadReturn { read_data: None }),

            ClientRegisterCommandContent::Write { data: _ }
                => OperationReturn::Write,
            
        },
    }
}

fn cmd_sender(cmd: &RegisterCommand) -> String {
    match cmd {
        RegisterCommand::System(cmd) => format!("{}", cmd.header.process_identifier),
        
        RegisterCommand::Client(_) => format!("client"),
    }

}