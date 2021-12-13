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
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;
use crate::constants::*;

pub async fn run_register_process(config: Configuration) {

    // TODO: what to do with the unwraps?

    let self_rank = config.public.self_rank;
    let (self_host, self_port) = &config.public.tcp_locations[(self_rank - 1) as usize];

    let address = format!("{}:{}", self_host, self_port);
    let listener = TcpListener::bind(address).await.unwrap();

    //let atomic_register = Arc::new(Mutex::new(RegisterProcess::build_ar(&config).await));
    
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

struct RegisterProcess {
    config:             Configuration,
    atomic_registers:   HashMap<Uuid, Arc<Mutex<ARModule>>>,
    ar_ids:             Mutex<VecDeque<Uuid>>,

}

impl RegisterProcess {

    async fn new(config: Configuration) -> Arc<Self> {

        let mut process = RegisterProcess{
            config:             config,
            atomic_registers:   HashMap::new(),
            ar_ids:             Mutex::new(VecDeque::new()),
        };

        process.build_registers(NUM_REGISTERS).await;
        
        Arc::new(process)
    }

    async fn next_id(&self) -> Uuid {
        let mut ids = self.ar_ids.lock().await; 
        let id = ids.pop_front();
        match id {
            None => panic!("no registers in register process"),
            Some(id) => {
                ids.push_back(id);
                id
            }
        }
    }

    async fn handle_stream<'a>(
        self: Arc<Self>, 
        read_stream_ref: Arc<Mutex<OwnedReadHalf>>, 
        write_stream_ref: Arc<Mutex<OwnedWriteHalf>>, 
    ) {
        loop {
            
            let cmd;
            let valid;

            {
                let mut stream_guard = read_stream_ref.lock().await;
                let stream = stream_guard.deref_mut();
                let res = deserialize_register_command(
                    &mut *stream,
                    &self.config.hmac_system_key,
                    &self.config.hmac_client_key,
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
                        serialize_response(&result, &mut *stream, &self.config.hmac_client_key).await.unwrap();
                    }
                }
                
                return;
            }

            // invalid sector id --------------------------------------------------
            let sector_idx = get_sector_idx(&cmd);
            if sector_idx > self.config.public.max_sector {
                match &cmd {
                    // TODO: what in case of system commands?
                    RegisterCommand::System(_) => {},

                    RegisterCommand::Client(cmd) => {
                        let result = invalid_result(cmd, StatusCode::InvalidSectorIndex);
                        
                        let mut stream_guard = write_stream_ref.lock().await;
                        let stream = stream_guard.deref_mut();
                        serialize_response(&result, &mut *stream, &self.config.hmac_client_key).await.unwrap();
                    }
                }
                
                return;
            }


            // handling -----------------------------------------------------------
            match cmd {
                RegisterCommand::System(cmd) => {

                    /* If `cmd` is a response (ie. `Ack` or `Value`) then 
                     * `cmd.header.msg_ident` is the id of the register that 
                     * had sent the request
                     */
                    let ar_id = if is_response(&cmd) { cmd.header.msg_ident } else { self.next_id().await };
                    match self.atomic_registers.get(&ar_id) {

                        /* If `cmd.header.msg_ident` was invalid, that means
                         * `cmd` is not a response to any of our atomic 
                         * registers, therefore do nothing
                         */
                        None => {},

                        Some(ar) => {
                            ar.lock().await.system_command(cmd).await;
                        }
                    }
                },
                
                RegisterCommand::Client(cmd) => {
                    
                    let hmac_key = self.config.hmac_client_key.clone();
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

                    let ar_id = self.next_id().await;
                    let mut ar = self.atomic_registers.get(&ar_id).unwrap().lock().await;
                    ar.client_command(cmd, operation_complete).await;
                },
            };

        } 

    }




    async fn build_registers(&mut self, num_registers: u32) {

        // TODO: what to do with the unwraps?

        let processes_count = self.config.public.tcp_locations.len();

        // build register client
        let register_client = Arc::new(
            RCModule::new(self.config.public.tcp_locations.clone(), self.config.hmac_system_key)
        );

        // build sectors manager
        let mut sectors_dir = self.config.public.storage_dir.clone();
        sectors_dir.push("sectors");
        if !sectors_dir.is_dir() {
            create_dir(sectors_dir.clone()).await.unwrap();
        }
        let sectors_manager = build_sectors_manager(sectors_dir);

        let self_rank = self.config.public.self_rank;


        self.atomic_registers = HashMap::new();
        let mut ar_ids = VecDeque::new();

        for _ in 0..num_registers {
            // build stable sotrage
            // TODO: should this be global or per atomic register
            let mut metadata_dir = self.config.public.storage_dir.clone();
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

            let ar_id = atomic_register.info.uuid;

            self.atomic_registers.insert(ar_id, Arc::new(Mutex::new(atomic_register)));
            ar_ids.push_back(ar_id);
        }

        self.ar_ids = Mutex::new(ar_ids);
    }
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

fn is_response(cmd: &SystemRegisterCommand) -> bool {
    match cmd.content {
        SystemRegisterCommandContent::Ack               => true,
        SystemRegisterCommandContent::ReadProc          => false,
        SystemRegisterCommandContent::WriteProc { .. }  => false,
        SystemRegisterCommandContent::Value     { .. }  => true,
    }
}

