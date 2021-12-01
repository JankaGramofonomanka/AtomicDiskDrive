
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
enum ARState {
    Reading,
    Writing,
    Idle,
}

struct ARModule {
    metadata:           Box<dyn StableStorage>,
    register_client:    Arc<dyn RegisterClient>,
    sectors_manager:    Arc<dyn SectorsManager>,
    processes_count:    u8,

    process_id:         u8,
    read_id:            u64,
    readlist:           Vec<SectorData>,
    acks:               u8,
    state:              ARState,
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


impl ARModule {

    fn prepare_cmd(&self, 
        header: SystemCommandHeader,
        content: SystemRegisterCommandContent,
    ) -> SystemRegisterCommand {
        
        // TODO: leave the uuid the same or create unique?
        let mut prep_header = header.clone();

        // TODO: are you sure `self_ident` (param of `build_atomic_register`) 
        // is the id of the process, not of the AR?
        prep_header.process_identifier = self.process_id;

        let cmd = SystemRegisterCommand {
            header: prep_header,
            content: content,
        };

        cmd

    }

    async fn broadcast(
        &self, 
        header: SystemCommandHeader,
        content: SystemRegisterCommandContent,
    ) {
        let broadcast_cmd = self.prepare_cmd(header, content);

        let msg = register_client_public::Broadcast {
            cmd: Arc::new(broadcast_cmd),
        };
        self.register_client.broadcast(msg).await
    }

    async fn respond(
        &self, 
        request_header: SystemCommandHeader, 
        content: SystemRegisterCommandContent, 
    ) {
        let response_cmd = self.prepare_cmd(request_header, content);

        let msg = register_client_public::Send {
            cmd: Arc::new(response_cmd),
            target: request_header.process_identifier as usize,
        };

        self.register_client.send(msg).await;
    }

    async fn callback(&mut self, op_return: OperationReturn) {
        let request_id = match self.request_id {
            Some(id) => id,
            None => panic!("`request_id` not defined"),
        };

        let op_complete = OperationComplete {
            status_code: StatusCode::Ok,
            request_identifier: request_id,
            op_return: op_return,
        };
        
        let some_callback = self.callback_op.take();
        match some_callback {
            None => panic!("`operation_complete` undefined"),
            Some(callb) => callb(op_complete).await,
        }
    }


    /*
    upon event < sbeb, Deliver | p [READ_PROC, r] > do
        trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;
    */
    async fn read_proc(&self, cmd_header: SystemCommandHeader) {
        let (ts, wr) = self.sectors_manager.read_metadata(cmd_header.sector_idx).await;
        let val = self.sectors_manager.read_data(cmd_header.sector_idx).await;

        let content = SystemRegisterCommandContent::Value {
            timestamp: ts,
            write_rank: wr,
            sector_data: val,
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
        &mut self, 
        cmd_header:     SystemCommandHeader,
        timestamp:      u64,
        write_rank:     u8,
        sector_data:    SectorVec,
    ) {
        if self.write_phase { return; }
        if cmd_header.read_ident != self.read_id { return; }

        self.readlist.push((timestamp, write_rank, sector_data));

        if self.state != ARState::Idle 
        && self.readlist.len() as u8 > self.processes_count / 2 
        {
            let (ts, wr) = self.sectors_manager.read_metadata(cmd_header.sector_idx).await;
            let val = self.sectors_manager.read_data(cmd_header.sector_idx).await;

            self.readlist.push((ts, wr, val));

            let readlist: Vec<SectorData> = self.readlist.drain(..).collect();
            let (maxts, maxwr, readval) = highest(&readlist);
            self.readval = Some(readval.clone());
            self.acks = 0;

            self.write_phase = true;

            match self.state {
                ARState::Idle => panic!("implossible case"),

                ARState::Reading => {

                    let content = SystemRegisterCommandContent::WriteProc {
                        timestamp:      *maxts,
                        write_rank:     *maxwr,
                        data_to_write:  readval.clone(),
                    };

                    self.broadcast(cmd_header, content).await
                }

                ARState::Writing => {
                    
                    let writeval = match self.writeval.take() {
                        Some(val) => val,
                        None => panic!("`writeval` is `None`"),
                    };

                    self.sectors_manager.write(
                        cmd_header.sector_idx, 
                        &(writeval.clone(), maxts + 1, self.process_id)
                    ).await;

                    let content = SystemRegisterCommandContent::WriteProc {
                        timestamp:      *maxts + 1,

                        // TODO: Are you sure `self.process_id` is the same as "rank(self)"?
                        write_rank:     self.process_id, 
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
        &mut self,
        cmd_header: SystemCommandHeader,
    ) {
        if cmd_header.read_ident != self.read_id { return; }

        self.acks += 1;
        if self.state != ARState::Idle && self.acks > self.processes_count / 2 {
            self.acks = 0;
            self.write_phase = false;

            match self.state {
                ARState::Idle => panic!("implossible case"),

                ARState::Reading => {
                    self.state = ARState::Idle;

                    // TODO: shouldn't you panic if `self.readval == None`?
                    let read_ret = ReadReturn { read_data: self.readval.take() } ;
                    self.callback(OperationReturn::Read(read_ret)).await;
                }

                ARState::Writing => {
                    self.state = ARState::Idle;

                    self.callback(OperationReturn::Write).await;
                }
            }
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
        &mut self,
        cmd_header: ClientCommandHeader,
    ) {
        self.read_id += 1;
        self.readlist = vec![];
        self.acks = 0;
        self.state = ARState::Reading;

        let broadcast_header = SystemCommandHeader {
            process_identifier: self.process_id,

            // TODO: what to do with this?
            msg_ident: Uuid::nil(),
            read_ident: self.read_id,
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
        &mut self,
        cmd_header: ClientCommandHeader,
        data:       SectorVec,
    ) {
        self.read_id += 1;
        self.writeval = Some(data);
        self.acks = 0;
        self.readlist = vec![];
        self.state = ARState::Writing;
        
        let broadcast_header = SystemCommandHeader {
            process_identifier: self.process_id,

            // TODO: what to do with this?
            msg_ident: Uuid::nil(),
            read_ident: self.read_id,
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
            processes_count:    processes_count as u8,
        
            process_id:         self_ident,
            read_id:            0,
            readlist:           vec![],
            acks:               0,
            state:              ARState::Idle,
            writeval:           None,
            readval:            None,
            write_phase:        false,
        
            callback_op:        None,
            request_id:         None,
            
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
        self.callback_op = Some(operation_complete);
        self.request_id = Some(cmd.header.request_identifier);

        let cmd_header = cmd.header;
        match cmd.content {
            ClientRegisterCommandContent::Read
                => self.read(cmd_header).await,

            ClientRegisterCommandContent::Write { data }
                => self.write(cmd_header, data).await,
        }
    }

    /// Send system command to the register.
    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let cmd_header = cmd.header;
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => self.read_proc(cmd_header).await,
            
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data, } 
                => self.value(cmd_header, timestamp, write_rank, sector_data).await,

            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write, }
                => self.write_proc(cmd_header, timestamp, write_rank, data_to_write).await,

            SystemRegisterCommandContent::Ack
                => self.ack(cmd_header).await,
        }
    }
}


