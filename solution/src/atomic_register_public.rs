
use crate::{
    ClientRegisterCommand, OperationComplete, RegisterClient, SectorsManager, StableStorage,
    SystemRegisterCommand, 
    SectorVec, SystemCommandHeader, SystemRegisterCommandContent
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::cmp::{Eq, Ord, PartialOrd, PartialEq, Ordering};

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
    unimplemented!()
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
    writeval:           Vec<u8>,
    write_phase:        bool,
    
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
                    let writeval = SectorVec(self.writeval.drain(..).collect());
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
        unimplemented!();
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

            SystemRegisterCommandContent::Ack => unimplemented!(),
        }
    }
}


