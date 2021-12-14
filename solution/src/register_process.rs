
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use std::io::ErrorKind;
use std::ops::DerefMut;
use std::collections::{HashMap, VecDeque};

use tokio::fs::create_dir;
use tokio::sync::{Mutex, Notify};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use uuid::Uuid;

use crate::constants::*;
use crate::domain::*;
use crate::atomic_register_public::*;
use crate::transfer_public::*;
use crate::register_client_public::*;
use crate::sectors_manager_public::*;
use crate::stable_storage_public::*;


pub struct RegisterProcess {
    config:             Configuration,
    atomic_registers:   HashMap<Uuid, Arc<Mutex<ARModule>>>,
    ar_ids:             Mutex<VecDeque<Uuid>>,

    // if `sector_handlers[i]` holds `None`, it means no command regarding 
    // sector `i` is currently handled 
    sector_handlers:    Vec<Mutex<Option<Arc<Notify>>>>,

    // a queue with ids of registers that are not handling any client requests
    // and a notifier in case the queue is empty
    free_registers:     (Mutex<VecDeque<Uuid>>, Arc<Notify>),

}

impl RegisterProcess {

    pub async fn new(config: Configuration) -> Arc<Self> {

        let mut sector_handlers = vec![];
        let mut pending_handlers = vec![];
        for _ in 0..config.public.max_sector {
            sector_handlers.push(Mutex::new(None));
            pending_handlers.push(Mutex::new(0));
        }

        let mut process = RegisterProcess{
            config:             config,
            atomic_registers:   HashMap::new(),
            ar_ids:             Mutex::new(VecDeque::new()),
            sector_handlers:    sector_handlers,
            free_registers:     (Mutex::new(VecDeque::new()), Arc::new(Notify::new())),
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

    async fn put_free_id(&self, id: Uuid) {
        let (ids, notifier) = &self.free_registers;
        ids.lock().await.push_back(id);

        // notify a pending client handler that an atomic register is free
        notifier.notify_waiters();
    }
    
    async fn get_free_id(&self) -> Uuid {

        let (ids, notifier) = &self.free_registers;
        
        loop {
            let id = ids.lock().await.pop_front();
            match id {
                None => { 
                    notifier.notified().await; 
                }
                Some(id) => { 
                    return id; 
                }
            }
        }
    }

    async fn wait_till_ready(&self, sector_idx: SectorIdx, notifier: Arc<Notify>) {
        let mut ready = false;
        let sector_idx = sector_idx as usize;
        
        while !ready {
            let notif;
            {
                let guard = self.sector_handlers[sector_idx].lock().await;
                notif = (*guard).clone();
            }
            match notif {
                None => { 
        
                    // mark the sector as being in use
                    let mut guard = self.sector_handlers[sector_idx].lock().await;
                    let notif = guard.deref_mut();
                    *notif = Some(notifier.clone());
        
                    ready = true; 
                },

                Some(previous_notifier) => {
        
                    previous_notifier.notified().await;
                },
            }
        }
    }
    
    async fn free_up_sector(&self, sector_idx: SectorIdx, notifier: Arc<Notify>) {
        let sector_idx = sector_idx as usize;

        // mark the sector as free
        {
            let mut guard = self.sector_handlers[sector_idx].lock().await;
            let notif = guard.deref_mut();
            *notif = None;
        }

        // notify any pending handlers that the sector is free
        notifier.notify_waiters();
    }

    pub async fn handle_stream<'a>(
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
                        serialize_response(
                            &result, 
                            &mut *stream, 
                            &self.config.hmac_client_key,
                        ).await.unwrap();
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
                        serialize_response(
                            &result, 
                            &mut *stream,
                            &self.config.hmac_client_key,
                        ).await.unwrap();
                    }
                }
                
                return;
            }


            // handling -----------------------------------------------------------
            self.clone().handle_cmd(write_stream_ref.clone(), cmd).await;

        } 

    }


    async fn handle_cmd(
        self: Arc<Self>, 
        write_stream_ref: Arc<Mutex<OwnedWriteHalf>>, 
        cmd: RegisterCommand,
    ) {
        

        

        let sector_idx = get_sector_idx(&cmd);
        

        match cmd {
            RegisterCommand::System(cmd) => {

                /* If `cmd` is a response (ie. `Ack` or `Value`) then 
                 * `cmd.header.msg_ident` is the id of the register that 
                 * had sent the request
                 */
                let ar_id = if is_response(&cmd) { 
                    cmd.header.msg_ident 
                } else { 
                    self.next_id().await 
                };

                match self.atomic_registers.get(&ar_id) {

                    /* If `cmd.header.msg_ident` was invalid, that means
                     * `cmd` is not a response to any of our atomic 
                     * registers, therefore do nothing
                     */
                    None => {},

                    Some(ar_mutex) => {

                        let notifier = Arc::new(Notify::new());
                        self.wait_till_ready(sector_idx, notifier.clone()).await;
                        
                        let mut ar_guard = ar_mutex.lock().await;
                        let ar = ar_guard.deref_mut();
                        ar.system_command(cmd).await;
                        
                        self.free_up_sector(sector_idx, notifier).await;
                    }
                }

                
            },
            
            RegisterCommand::Client(cmd) => {
                
                let hmac_key = self.config.hmac_client_key.clone();
                let write_stream_ref_clone = write_stream_ref.clone();

                let ar_id = self.get_free_id().await;
                let self_ref = self.clone();

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

                        self_ref.put_free_id(ar_id).await;
                        
                    })
                });

                let notifier = Arc::new(Notify::new());
                self.wait_till_ready(sector_idx, notifier.clone()).await;
                
                let mut ar = self.atomic_registers.get(&ar_id).unwrap().lock().await;
                {ar.client_command(cmd, operation_complete).await;}

                self.free_up_sector(sector_idx, notifier).await;
            },
        };

        
    }


    async fn build_registers(&mut self, num_registers: u32) {

        // TODO: what to do with the unwraps?

        let processes_count = self.config.public.tcp_locations.len();

        // build register client
        let register_client = Arc::new(
            RCModule::new(
                self.config.public.tcp_locations.clone(), 
                self.config.hmac_system_key,
            )
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
        let mut free_ids = VecDeque::new();

        for i in 0..num_registers {
            
            // build stable sotrage
            // TODO: should this be global or per atomic register
            let mut metadata_dir = self.config.public.storage_dir.clone();
            metadata_dir.push(format!("ar{}", i));
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
            free_ids.push_back(ar_id);
        }

        self.ar_ids = Mutex::new(ar_ids);
        self.free_registers.0 = Mutex::new(free_ids);
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

