
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


    async fn respond(
        &self, 
        request_header: SystemCommandHeader, 
        content: SystemRegisterCommandContent, 
    ) {
        // TODO: leave the uuid the same or create unique?
        let mut response_header = request_header.clone();

        // TODO: are you sure `self_ident` (param of `build_atomic_register`) 
        // is the id of the process, not of the AR?
        response_header.process_identifier = self.process_id;

        let response_cmd = SystemRegisterCommand {
            header: response_header,
            content: content,
        };

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
                => unimplemented!(),

            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write, }
                => self.write_proc(cmd_header, timestamp, write_rank, data_to_write).await,

            SystemRegisterCommandContent::Ack => unimplemented!(),
        }
    }
}


