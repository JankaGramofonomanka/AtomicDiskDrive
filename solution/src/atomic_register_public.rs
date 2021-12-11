
use crate::{
    ClientRegisterCommand, OperationComplete, RegisterClient, SectorsManager, StableStorage,
    SystemRegisterCommand, 
    SectorVec, SystemCommandHeader, SystemRegisterCommandContent,
    ClientCommandHeader, StatusCode, OperationReturn, ReadReturn,
    ClientRegisterCommandContent
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use std::cmp::{Eq, Ord, PartialOrd, PartialEq, Ordering};
use uuid::Uuid;
use tokio::sync::{Mutex, Notify};
use std::ops::DerefMut;

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

struct ARInfo {
    processes_count:    u8,
    process_id:         u8,
}

struct ARState {
    
    read_id:            u64,
    readlist:           Vec<SectorData>,
    acks:               u8,
    phase:              ARPhase,
    writeval:           Option<SectorVec>,
    readval:            Option<SectorVec>,
    write_phase:        bool,
    callback_op:        Option<Box<
                            dyn FnOnce(OperationComplete)
                                -> Pin<Box<dyn Future<Output = ()> + Send>>
                                + Send
                                + Sync,
                        >>,
    request_id:         Option<u64>,
}

pub struct ARModule {
    metadata:           Box<dyn StableStorage>,
    register_client:    Arc<dyn RegisterClient>,
    sectors_manager:    Arc<dyn SectorsManager>,

    /*
    processes_count:    u8,

    process_id:         u8,
    read_id:            u64,
    readlist:           Vec<SectorData>,
    acks:               u8,
    phase:              ARPhase,
    writeval:           Option<SectorVec>,
    readval:            Option<SectorVec>,
    write_phase:        bool,

    callback_op:        Option<Box<
                            dyn FnOnce(OperationComplete)
                                -> Pin<Box<dyn Future<Output = ()> + Send>>
                                + Send
                                + Sync,
                        >>,
    request_id:         Option<u64>,
    */

    state:              Arc<Mutex<ARState>>,
    info:               Arc<ARInfo>,

    notifier:           Arc<Notify>,
    
}


impl ARModule {

    fn prepare_cmd(
        ar_info:    Arc<ARInfo>,
        header:     SystemCommandHeader,
        content:    SystemRegisterCommandContent,
    ) -> SystemRegisterCommand {
        
        // TODO: leave the uuid the same or create unique?
        let mut prep_header = header.clone();

        // TODO: are you sure `self_ident` (param of `build_atomic_register`) 
        // is the id of the process, not of the AR?
        prep_header.process_identifier = ar_info.process_id;

        let cmd = SystemRegisterCommand {
            header: prep_header,
            content: content,
        };

        cmd

    }

    async fn broadcast(
        ar_info:            Arc<ARInfo>,
        register_client:    Arc<dyn RegisterClient>,
        header:             SystemCommandHeader,
        content:            SystemRegisterCommandContent,
    ) {

        println!(
            "(proc_id: {}) broadcast `{}` msg", 
            ar_info.process_id, 
            cmd_type(&content),
        );


        let broadcast_cmd = ARModule::prepare_cmd(ar_info, header, content);

        let msg = register_client_public::Broadcast {
            cmd: Arc::new(broadcast_cmd),
        };
        register_client.broadcast(msg).await
    }

    async fn respond(
        ar_info:            Arc<ARInfo>,
        register_client:    Arc<dyn RegisterClient>,
        request_header:     SystemCommandHeader, 
        content:            SystemRegisterCommandContent, 
    ) {

        println!(
            "(proc_id: {}) send `{}` msg to {}", 
            ar_info.process_id, 
            cmd_type(&content),
            request_header.process_identifier
        );

        let response_cmd = ARModule::prepare_cmd(ar_info, request_header, content);

        let msg = register_client_public::Send {
            cmd: Arc::new(response_cmd),
            target: request_header.process_identifier as usize,
        };

        register_client.send(msg).await;
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
            status_code: StatusCode::Ok,
            request_identifier: request_id,
            op_return: op_return,
        };
        
        callback_op(op_complete).await;
    }


    /*
    upon event < sbeb, Deliver | p [READ_PROC, r] > do
        trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;
    */
    async fn read_proc(
        ar_info:            Arc<ARInfo>,
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        cmd_header:         SystemCommandHeader,
    ) {
        let (ts, wr) = sectors_manager.read_metadata(cmd_header.sector_idx).await;
        let val = sectors_manager.read_data(cmd_header.sector_idx).await;

        let content = SystemRegisterCommandContent::Value {
            timestamp: ts,
            write_rank: wr,
            sector_data: val,
        };

        ARModule::respond(ar_info, register_client, cmd_header, content).await
    }

    /*
    upon event < sbeb, Deliver | p, [WRITE_PROC, r, ts', wr', v'] > do
        if (ts', wr') > (ts, wr) then
            (ts, wr, val) := (ts', wr', v');
            store(ts, wr, val);
        trigger < sl, Send | p, [ACK, r] >;
    */
    async fn write_proc(
        ar_info:            Arc<ARInfo>,
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        cmd_header:     SystemCommandHeader,
        timestamp:      u64,
        write_rank:     u8,
        data_to_write:  SectorVec,
    ) {
        let (ts, wr) = sectors_manager.read_metadata(cmd_header.sector_idx).await;
        if timestamp > ts || (timestamp == ts && write_rank > wr) {
            sectors_manager.write(
                cmd_header.sector_idx, 
                &(data_to_write, timestamp, write_rank),
            ).await;
        }

        ARModule::respond(
            ar_info,
            register_client,
            cmd_header, 
            SystemRegisterCommandContent::Ack
        ).await
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
        ar_info:            Arc<ARInfo>,
        ar_state_ref:       Arc<Mutex<ARState>>, 
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        cmd_header:         SystemCommandHeader,
        timestamp:          u64,
        write_rank:         u8,
        sector_data:        SectorVec,
    ) {
        let mut ar_state_guard = ar_state_ref.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        if ar_state.write_phase { return; }

        // TODO: also check sector_idx?
        if cmd_header.read_ident != ar_state.read_id { return; }

        ar_state.readlist.push((timestamp, write_rank, sector_data));

        if ar_state.phase != ARPhase::Idle 
        && ar_state.readlist.len() as u8 > ar_info.processes_count / 2 
        {
            let (ts, wr) = sectors_manager.read_metadata(cmd_header.sector_idx).await;
            let val = sectors_manager.read_data(cmd_header.sector_idx).await;

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

                    ARModule::broadcast(ar_info, register_client, cmd_header, content).await
                }

                ARPhase::Writing => {
                    
                    let writeval = match ar_state.writeval.take() {
                        Some(val) => val,
                        None => panic!("`writeval` is `None`"),
                    };

                    sectors_manager.write(
                        cmd_header.sector_idx, 
                        &(writeval.clone(), maxts + 1, ar_info.process_id)
                    ).await;

                    let content = SystemRegisterCommandContent::WriteProc {
                        timestamp:      *maxts + 1,

                        // TODO: Are you sure `self.process_id` is the same as "rank(self)"?
                        write_rank:     ar_info.process_id, 
                        data_to_write:  writeval,
                    };

                    ARModule::broadcast(ar_info, register_client, cmd_header, content).await
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
        ar_info:            Arc<ARInfo>,
        ar_state_ref:       Arc<Mutex<ARState>>, 
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        notifier:           Arc<Notify>,
        cmd_header:         SystemCommandHeader,
    ) {

        let mut ar_state_guard = ar_state_ref.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        // TODO: also check sector_idx?
        if cmd_header.read_ident != ar_state.read_id { return; }

        ar_state.acks += 1;
        if ar_state.phase != ARPhase::Idle && ar_state.acks > ar_info.processes_count / 2 {
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

            // notify any pending client operations
            ar_state.request_id = None;
            notifier.notify_one();
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
        ar_info:            Arc<ARInfo>,
        ar_state_ref:       Arc<Mutex<ARState>>, 
        register_client:    Arc<dyn RegisterClient>,
        cmd_header: ClientCommandHeader,
    ) {
        let mut ar_state_guard = ar_state_ref.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        ar_state.read_id += 1;
        ar_state.readlist = vec![];
        ar_state.acks = 0;
        ar_state.phase = ARPhase::Reading;

        let broadcast_header = SystemCommandHeader {
            process_identifier: ar_info.process_id,

            // TODO: what to do with this?
            msg_ident: Uuid::nil(),
            read_ident: ar_state.read_id,
            sector_idx: cmd_header.sector_idx

        };

        let content = SystemRegisterCommandContent::ReadProc;
        ARModule::broadcast(ar_info, register_client, broadcast_header, content).await;
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
        ar_info:            Arc<ARInfo>,
        ar_state_ref:       Arc<Mutex<ARState>>, 
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        cmd_header:         ClientCommandHeader,
        data:               SectorVec,
    ) {
        let mut ar_state_guard = ar_state_ref.lock().await;
        let ar_state = ar_state_guard.deref_mut();

        ar_state.read_id += 1;
        ar_state.writeval = Some(data);
        ar_state.acks = 0;
        ar_state.readlist = vec![];
        ar_state.phase = ARPhase::Writing;
        
        let broadcast_header = SystemCommandHeader {
            process_identifier: ar_info.process_id,

            // TODO: what to do with this?
            msg_ident: Uuid::nil(),
            read_ident: ar_state.read_id,
            sector_idx: cmd_header.sector_idx

        };
        let content = SystemRegisterCommandContent::ReadProc;
        ARModule::broadcast(ar_info, register_client, broadcast_header, content).await;
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
    */
    async fn new(
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: usize,

    ) -> Self {
        ARModule {
            metadata:           metadata,
            register_client:    register_client,
            sectors_manager:    sectors_manager,
            
            info: Arc::new(
                ARInfo {
                    processes_count:    processes_count as u8,
                    process_id:         self_ident,
                }
            ),
        
            state: Arc::new(Mutex::new(
                ARState {
                    read_id:        0,
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
            

            notifier: Arc::new(Notify::new()),
            
        }
    }

    // TODO: for now there is nothing to recover but probably there will be
    /*
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

    
    async fn handle_client(
        ar_info:            Arc<ARInfo>,
        ar_state_ref:       Arc<Mutex<ARState>>, 
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        notifier:           Arc<Notify>,
        
        cmd:                ClientRegisterCommand,
        operation_complete: Box<
                                dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                                    + Send
                                    + Sync,
                            >,
    ) {
        let mut ready;
        { ready = is_none(ar_state_ref.lock().await.request_id) }
        // wait until the previous client command is handled
        while !ready {
            
            notifier.notified().await;

            { ready = is_none(ar_state_ref.lock().await.request_id) }
        }

        {
            let mut ar_state_guard = ar_state_ref.lock().await;
            let ar_state = ar_state_guard.deref_mut();

            ar_state.request_id = Some(cmd.header.request_identifier);
            ar_state.callback_op = Some(operation_complete);
        }

        let cmd_header = cmd.header;
        match cmd.content {
            ClientRegisterCommandContent::Read
                => ARModule::read(
                    ar_info, 
                    ar_state_ref, 
                    register_client, 
                    cmd_header
                ).await,

            ClientRegisterCommandContent::Write { data }
                => ARModule::write(
                    ar_info, 
                    ar_state_ref, 
                    sectors_manager, 
                    register_client, 
                    cmd_header, data
                ).await,
        }
    }

    async fn handle_system(
        ar_info:            Arc<ARInfo>,
        ar_state_ref:       Arc<Mutex<ARState>>, 
        sectors_manager:    Arc<dyn SectorsManager>,
        register_client:    Arc<dyn RegisterClient>,
        notifier:           Arc<Notify>,

        cmd:                SystemRegisterCommand,
    ) {
        let cmd_header = cmd.header;
        match cmd.content {
            SystemRegisterCommandContent::ReadProc 
                => ARModule::read_proc(
                    ar_info,
                    sectors_manager,
                    register_client,
                    cmd_header
                ).await,
            
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data, } 
                => ARModule::value(
                    ar_info,
                    ar_state_ref,
                    sectors_manager,
                    register_client,
                    cmd_header,
                    timestamp,
                    write_rank,
                    sector_data,
                ).await,

            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write, }
                => ARModule::write_proc(
                    ar_info,
                    sectors_manager,
                    register_client,
                    cmd_header,
                    timestamp,
                    write_rank,
                    data_to_write,
                ).await,

            SystemRegisterCommandContent::Ack
                => ARModule::ack(        
                    ar_info,
                    ar_state_ref, 
                    sectors_manager,
                    register_client,
                    notifier,
                    cmd_header,
                ).await,
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
        // wait until the previous client command is handled
        
        tokio::spawn(ARModule::handle_client(
            self.info.clone(), 
            self.state.clone(), 
            self.sectors_manager.clone(), 
            self.register_client.clone(), 
            self.notifier.clone(), 
            cmd, 
            operation_complete,
        ));
    }

    /// Send system command to the register.
    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        tokio::spawn(ARModule::handle_system(
            self.info.clone(), 
            self.state.clone(), 
            self.sectors_manager.clone(), 
            self.register_client.clone(), 
            self.notifier.clone(), 
            cmd,
        ));
    }
}

fn is_none<T>(x: Option<T>) -> bool {
    match x {
        Some(_) => false,
        None => true,
    }
}


fn cmd_type(content: &SystemRegisterCommandContent) -> String {
    match content {
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
