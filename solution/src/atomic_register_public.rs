
use crate::{RegisterClient, SectorsManager, StableStorage};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use std::cmp::{Eq, PartialEq};
use uuid::Uuid;
use tokio::sync::Mutex;
use std::ops::DerefMut;
use crate::domain::*;

use crate::register_client_public;

#[async_trait::async_trait]
pub trait AtomicRegister: Send + Sync {
    /// Send client command to the register. After it is completed, we expect
    /// callback to be called. Note that completion of client command happens after
    /// delivery of multiple system commands to the register, as the algorithm specifies.
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        operation_complete: Box<
            dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    );

    /// Send system command to the register.
    async fn system_command(&mut self, cmd: SystemRegisterCommand);
}

/// Idents are numbered starting at 1 (up to the number of processes in the system).
/// Storage for atomic register algorithm data is separated into StableStorage.
/// Communication with other processes of the system is to be done by register_client.
/// And sectors must be stored in the sectors_manager instance.
pub async fn build_atomic_register(
    self_ident: u8,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: usize,
) -> Box<dyn AtomicRegister> {

    let armod = ARModule::new(
        self_ident,
        metadata,
        register_client,
        sectors_manager,
        processes_count,
    ).await;

    Box::new(armod)
}



const METADATA_FILE: &str = "metadata";

fn highest(readlist: &Vec<SectorData>) -> &SectorData {

    let mut max_ts = 0;
    let mut max_wr = 0;
    let mut index = 0;
    for (i, (ts, wr, val)) in readlist.iter().enumerate() {
        if *ts > max_ts { 
            max_ts = *ts;
            max_wr = *wr;
            index = i;
        } else if *wr > max_wr { 
            max_ts = *ts;
            max_wr = *wr;
            index = i;
        }
    }

    return &readlist[index];
}

type SectorData = (u64, u8, SectorVec);

#[derive(PartialEq, Eq)]
enum ARPhase {
    Reading,
    Writing,
    Idle,
}

pub struct ARInfo {
    pub processes_count:    u8,
    pub process_id:         u8,
    pub uuid:               Uuid,
}

pub struct ARState {
    
    read_id:        u64,
    sector_idx:     Option<SectorIdx>,
    readlist:       Vec<SectorData>,
    acks:           u8,
    phase:          ARPhase,
    writeval:       Option<SectorVec>,
    readval:        Option<SectorVec>,
    write_phase:    bool,
    callback_op:    Option<Box<
                        dyn FnOnce(OperationComplete)
                            -> Pin<Box<dyn Future<Output = ()> + Send>>
                            + Send
                            + Sync,
                    >>,
    request_id:     Option<u64>,
}

#[derive(Clone)]
pub struct ARModule {
    pub info:           Arc<ARInfo>,
    state:              Arc<Mutex<ARState>>,

    metadata:           Arc<Mutex<Box<dyn StableStorage>>>,
    register_client:    Arc<dyn RegisterClient>,
    sectors_manager:    Arc<dyn SectorsManager>,
}


impl ARModule {

    fn prepare_cmd(
        &self,
        header:     SystemCommandHeader,
        content:    SystemRegisterCommandContent,
    ) -> SystemRegisterCommand {
        
        let mut prep_header = header.clone();

        // TODO: are you sure `self_ident` (param of `build_atomic_register`) 
        // is the id of the process, not of the AR?
        prep_header.process_identifier = self.info.process_id;

        let cmd = SystemRegisterCommand {
            header:     prep_header,
            content:    content,
        };

        cmd

    }

    async fn broadcast(
        &self,
        header:     SystemCommandHeader,
        content:    SystemRegisterCommandContent,
    ) {
        let broadcast_cmd = self.prepare_cmd(header, content);

        let msg = register_client_public::Broadcast {
            cmd: Arc::new(broadcast_cmd),
        };
        self.register_client.broadcast(msg).await;
    }

    async fn respond(
        &self,
        request_header: SystemCommandHeader, 
        content:        SystemRegisterCommandContent, 
    ) {

        let response_cmd = self.prepare_cmd(request_header, content);

        let msg = register_client_public::Send {
            cmd:    Arc::new(response_cmd),
            target: request_header.process_identifier as usize,
        };

        self.register_client.send(msg).await;
    }

    async fn callback(
        callback_op:    Box<
                            dyn FnOnce(OperationComplete)
                                -> Pin<Box<dyn Future<Output = ()> + Send>>
                                + Send
                                + Sync,
                        >, 
        request_id:     u64,
        op_return:      OperationReturn
    ) {

        let op_complete = OperationComplete {
            status_code:        StatusCode::Ok,
            request_identifier: request_id,
            op_return:          op_return,
        };
        
        callback_op(op_complete).await;
    }


    async fn store_metadata(
        uuid:           Uuid,
        read_id:        u64,
        sector_idx:     Option<SectorIdx>,
        storage_ref:    Arc<Mutex<Box<dyn StableStorage>>>,
    ) { 
        let data = (uuid, read_id, sector_idx);
        let serialized: Vec<u8> = bincode::serialize(&data).unwrap();
        
        let mut storage_guard = storage_ref.lock().await;
        let storage = storage_guard.deref_mut();
        storage.put(METADATA_FILE, &serialized[..]).await.unwrap();
    }

    async fn restore_metadata(
        storage_ref: Arc<Mutex<Box<dyn StableStorage>>>,
    ) -> (Uuid, u64, Option<SectorIdx>) {

        let result;
        {
            let mut storage_guard = storage_ref.lock().await;
            let storage = storage_guard.deref_mut();
            result = storage.get(METADATA_FILE).await;
        }

        match result {
            None => {
                let uuid = Uuid::new_v4();
                let read_id = 0;
                let sector_idx = None;
                ARModule::store_metadata(uuid, read_id, sector_idx, storage_ref).await;
                (uuid, read_id, sector_idx)
            },

            Some(serialized) => {
                let (uuid, read_id, sector_idx) = bincode::deserialize(&serialized).unwrap();
                (uuid, read_id, sector_idx)
            },
        }
    }

    /*
    upon event < sbeb, Deliver | p [READ_PROC, r] > do
        trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;
    */
    async fn read_proc(
        &self, 
        cmd_header: SystemCommandHeader
    ) {

        let (ts, wr) = self.sectors_manager.read_metadata(cmd_header.sector_idx).await;
        let val = self.sectors_manager.read_data(cmd_header.sector_idx).await;

        let content = SystemRegisterCommandContent::Value {
            timestamp:      ts,
            write_rank:     wr,
            sector_data:    val,
        };

        self.respond(cmd_header, content).await
    }

    /*
    upon event < sbeb, Deliver | p, [WRITE_PROC, r, ts', wr', v'] > do
        if (ts', wr') > (ts, wr) then
            (ts, wr, val) := (ts', wr', v');
            store(ts, wr, val);
        trigger < sl, Send | p, [ACK, r] >;
    */
    async fn write_proc(
        &self,
        cmd_header:     SystemCommandHeader,
        timestamp:      u64,
        write_rank:     u8,
        data_to_write:  SectorVec,
    ) {
        let (ts, wr) = self.sectors_manager.read_metadata(cmd_header.sector_idx).await;
        if timestamp > ts || (timestamp == ts && write_rank > wr) {
            self.sectors_manager.write(
                cmd_header.sector_idx, 
                &(data_to_write, timestamp, write_rank),
            ).await;
        }

        self.respond(cmd_header, SystemRegisterCommandContent::Ack).await
    }

    /*
    upon event <sl, Deliver | q, [VALUE, r, ts', wr', v'] > such that r == rid and !write_phase do
        readlist[q] := (ts', wr', v');
        if #(readlist) > N / 2 and (reading or writing) then
            readlist[self] := (ts, wr, val);
            (maxts, rr, readval) := highest(readlist);
            readlist := [ _ ] `of length` N;
            acklist := [ _ ] `of length` N;
            write_phase := TRUE;
            if reading = TRUE then
                trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts, rr, readval] >;
            else
                (ts, wr, val) := (maxts + 1, rank(self), writeval);
                store(ts, wr, val);
                trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts + 1, rank(self), writeval] >;
    */
    async fn value(
        &self,
        cmd_header:     SystemCommandHeader,
        timestamp:      u64,
        write_rank:     u8,
        sector_data:    SectorVec,
    ) {
        
        let mut ar_state_guard = self.state.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        if ar_state.write_phase { return; }

        if cmd_header.read_ident != ar_state.read_id { return; }
        if Some(cmd_header.sector_idx) != ar_state.sector_idx { return; }
        if self.info.uuid != cmd_header.msg_ident { return; }

        ar_state.readlist.push((timestamp, write_rank, sector_data));

        if ar_state.phase != ARPhase::Idle 
        && ar_state.readlist.len() as u8 > self.info.processes_count / 2 
        {
            let (ts, wr) = self.sectors_manager.read_metadata(cmd_header.sector_idx).await;
            let val = self.sectors_manager.read_data(cmd_header.sector_idx).await;

            ar_state.readlist.push((ts, wr, val));

            let readlist: Vec<SectorData> = ar_state.readlist.drain(..).collect();
            let (maxts, maxwr, readval) = highest(&readlist);
            ar_state.readval = Some(readval.clone());
            ar_state.acks = 0;

            ar_state.write_phase = true;

            match ar_state.phase {
                ARPhase::Idle => panic!("implossible case"),

                ARPhase::Reading => {

                    let content = SystemRegisterCommandContent::WriteProc {
                        timestamp:      *maxts,
                        write_rank:     *maxwr,
                        data_to_write:  readval.clone(),
                    };

                    self.broadcast(cmd_header, content).await
                }

                ARPhase::Writing => {
                    
                    let writeval = match ar_state.writeval.take() {
                        Some(val) => val,
                        None => panic!("`writeval` is `None`"),
                    };

                    self.sectors_manager.write(
                        cmd_header.sector_idx, 
                        &(writeval.clone(), maxts + 1, self.info.process_id)
                    ).await;

                    let content = SystemRegisterCommandContent::WriteProc {
                        timestamp:      *maxts + 1,
                        write_rank:     self.info.process_id, 
                        data_to_write:  writeval,
                    };

                    self.broadcast(cmd_header, content).await
                }
            }
        }
    }

    /*
    upon event < sl, Deliver | q, [ACK, r] > such that r == rid and write_phase do
        acklist[q] := Ack;
        if #(acklist) > N / 2 and (reading or writing) then
            acklist := [ _ ] `of length` N;
            write_phase := FALSE;
            if reading = TRUE then
                reading := FALSE;
                trigger < nnar, ReadReturn | readval >;
            else
                writing := FALSE;
                trigger < nnar, WriteReturn >;
    */
    async fn ack(
        &self,
        cmd_header: SystemCommandHeader,
    ) {

        let mut ar_state_guard = self.state.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        if cmd_header.read_ident != ar_state.read_id { return; }
        if Some(cmd_header.sector_idx) != ar_state.sector_idx { return; }
        if self.info.uuid != cmd_header.msg_ident { return; }

        ar_state.acks += 1;
        if ar_state.phase != ARPhase::Idle && ar_state.acks > self.info.processes_count / 2 {
            ar_state.acks = 0;
            ar_state.write_phase = false;

            let callback_op = match ar_state.callback_op.take() {
                None        => panic!("`operation_complete` undefined"),
                Some(callb) => callb,
            };

            let req_id = match ar_state.request_id {
                Some(id) => id,
                None => panic!("`request_id` not defined"),
            };

            match ar_state.phase {
                ARPhase::Idle => panic!("implossible case"),

                ARPhase::Reading => {
                    ar_state.phase = ARPhase::Idle;

                    // TODO: shouldn't you panic if `self.readval == None`?
                    let read_ret = ReadReturn { read_data: ar_state.readval.take() } ;
                    ARModule::callback(callback_op, req_id, OperationReturn::Read(read_ret)).await;
                }

                ARPhase::Writing => {
                    ar_state.phase = ARPhase::Idle;

                    ARModule::callback(callback_op, req_id, OperationReturn::Write).await;
                }
            }

            ar_state.request_id = None;
            ar_state.sector_idx = None;
        }
    }

    /*
    upon event < nnar, Read > do
        rid := rid + 1;
        store(rid);
        readlist := [ _ ] `of length` N;
        acklist := [ _ ] `of length` N;
        reading := TRUE;
        trigger < sbeb, Broadcast | [READ_PROC, rid] >;
    */
    async fn read(
        &self,
        cmd_header: ClientCommandHeader,
    ) {
        let mut ar_state_guard = self.state.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        ar_state.read_id += 1;
        ar_state.readlist = vec![];
        ar_state.acks = 0;
        ar_state.phase = ARPhase::Reading;

        ARModule::store_metadata(
            self.info.uuid, 
            ar_state.read_id, 
            ar_state.sector_idx, 
            self.metadata.clone(),
        ).await;

        let broadcast_header = SystemCommandHeader {
            process_identifier: self.info.process_id,
            msg_ident:  self.info.uuid,
            read_ident: ar_state.read_id,
            sector_idx: cmd_header.sector_idx

        };

        let content = SystemRegisterCommandContent::ReadProc;
        self.broadcast(broadcast_header, content).await;
    }

    /*
    upon event < nnar, Write | v > do
        rid := rid + 1;
        writeval := v;
        acklist := [ _ ] `of length` N;
        readlist := [ _ ] `of length` N;
        writing := TRUE;
        store(rid);
        trigger < sbeb, Broadcast | [READ_PROC, rid] >;
    */
    async fn write(
        &self,
        cmd_header: ClientCommandHeader,
        data:       SectorVec,
    ) {
        let mut ar_state_guard = self.state.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        ar_state.read_id += 1;
        ar_state.writeval = Some(data);
        ar_state.acks = 0;
        ar_state.readlist = vec![];
        ar_state.phase = ARPhase::Writing;

        ARModule::store_metadata(
            self.info.uuid, 
            ar_state.read_id, 
            ar_state.sector_idx, 
            self.metadata.clone(),
        ).await;
        
        let broadcast_header = SystemCommandHeader {
            process_identifier: self.info.process_id,
            msg_ident:  self.info.uuid,
            read_ident: ar_state.read_id,
            sector_idx: cmd_header.sector_idx

        };
        let content = SystemRegisterCommandContent::ReadProc;
        self.broadcast(broadcast_header, content).await;
    }


    /* 
    upon event < nnar, Init > do
        (ts, wr, val) := (0, 0, _);
        rid:= 0;
        readlist := [ _ ] `of length` N;
        acklist := [ _ ] `of length` N;
        reading := FALSE;
        writing := FALSE;
        writeval := _;
        readval := _;
        write_phase := FALSE;
        store(wr, ts, val, rid);

    upon event < nnar, Recovery > do
        retrieve(wr, ts, val, rid);
        readlist := [ _ ] `of length` N;
        acklist := [ _ ]  `of length` N;
        reading := FALSE;
        readval := _;
        write_phase := FALSE;
        writing := FALSE;
        writeval := _;
    */
    pub async fn new(
        self_ident:         u8,
        metadata:           Box<dyn StableStorage>,
        register_client:    Arc<dyn RegisterClient>,
        sectors_manager:    Arc<dyn SectorsManager>,
        processes_count:    usize,

    ) -> Self {
        let metadata = Arc::new(Mutex::new(metadata));
        let (uuid, read_id, sector_idx) = ARModule::restore_metadata(metadata.clone()).await;

        ARModule {
            metadata:           metadata,
            register_client:    register_client,
            sectors_manager:    sectors_manager,

            info: Arc::new(
                ARInfo {
                    processes_count:    processes_count as u8,
                    process_id:         self_ident,
                    uuid:               uuid,
                }
            ),
        
            state: Arc::new(Mutex::new(
                ARState {
                    read_id:        read_id,
                    sector_idx:     sector_idx,
                    readlist:       vec![],
                    acks:           0,
                    phase:          ARPhase::Idle,
                    writeval:       None,
                    readval:        None,
                    write_phase:    false,
                
                    callback_op:    None,
                    request_id:     None,
                }
            )),
            
        }
    }

    
    async fn handle_client(
        &self,
        cmd:                ClientRegisterCommand,
        operation_complete: Box<
                                dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                                    + Send
                                    + Sync,
                            >,
    ) {
        {
            let mut ar_state_guard = self.state.lock().await;
            let ar_state = ar_state_guard.deref_mut();

            ar_state.request_id = Some(cmd.header.request_identifier);
            ar_state.sector_idx = Some(cmd.header.sector_idx);
            ar_state.callback_op = Some(operation_complete);
        }

        let cmd_header = cmd.header;
        match cmd.content {
            ClientRegisterCommandContent::Read
                => self.read(cmd_header).await,

            ClientRegisterCommandContent::Write { data }
                => self.write(cmd_header, data).await,
        }
    }

}



#[async_trait::async_trait]
impl AtomicRegister for ARModule {
    /// Send client command to the register. After it is completed, we expect
    /// callback to be called. Note that completion of client command happens after
    /// delivery of multiple system commands to the register, as the algorithm specifies.
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        operation_complete: Box<
            dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    ) {
        
        self.handle_client(cmd, operation_complete).await;
    }

    /// Send system command to the register.
    async fn system_command(&mut self, cmd: SystemRegisterCommand) {

        let cmd_header = cmd.header;
        match cmd.content {
            SystemRegisterCommandContent::ReadProc 
                => self.read_proc(cmd_header).await,
            
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data, } 
                => self.value(
                    cmd_header,
                    timestamp,
                    write_rank,
                    sector_data,
                ).await,

            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write, }
                => self.write_proc(
                    cmd_header,
                    timestamp,
                    write_rank,
                    data_to_write,
                ).await,

            SystemRegisterCommandContent::Ack
                => self.ack(cmd_header).await,
        }
    }
}

