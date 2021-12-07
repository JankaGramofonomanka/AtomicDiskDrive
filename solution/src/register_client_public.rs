
use crate::SystemRegisterCommand;
use std::sync::Arc;

use std::collections::HashMap;
use tokio::net::TcpStream;
use crate::transfer_public::serialize_system_command;
use crate::RegisterCommand;
use tokio::task::JoinHandle;


#[async_trait::async_trait]
/// We do not need any public implementation of this trait. It is there for use
/// in AtomicRegister. In our opinion it is a safe bet to say some structure of
/// this kind must appear in your solution.
pub trait RegisterClient: core::marker::Send + core::marker::Sync {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send);

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast);
}

pub struct Broadcast {
    pub cmd: Arc<SystemRegisterCommand>,
}

pub struct Send {
    pub cmd: Arc<SystemRegisterCommand>,
    /// Identifier of the target process. Those start at 1.
    pub target: usize,
}



pub struct RCModule {
    tcp_locations: HashMap<usize, (String, u16)>,
    hmac_key: [u8; 64],
}

impl RCModule {
    pub fn new(tcp_locations: Vec<(String, u16)>, hmac_key: [u8; 64]) -> Self {
        
        let mut tcp_locs = HashMap::new();

        let mut proc_id: usize = 1;
        for (host, port) in tcp_locations {

            tcp_locs.insert(proc_id, (host, port));

            proc_id += 1;
        }

        RCModule {
            tcp_locations: tcp_locs,
            hmac_key: hmac_key,
        }
    }

}


async fn send_msg(
    cmd: Arc<SystemRegisterCommand>, 
    host: String, 
    port: u16, 
    hmac_key: [u8; 64]
) {

    let address = format!("{}:{}", host, port);

    // TODO: how to handle errors?
    let mut stream = TcpStream::connect(address).await.unwrap();

    // TODO: how to handle errors?
    serialize_system_command(&cmd, &mut stream, &hmac_key).await.unwrap();

}

#[async_trait::async_trait]
impl RegisterClient for RCModule {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send) {
        
        let (host, port) = &self.tcp_locations[&msg.target];
        send_msg(msg.cmd, host.clone(), *port, self.hmac_key).await

    }

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast) {

        let mut send_handles: Vec<JoinHandle<()>> = vec![];
        for (_, (host, port)) in self.tcp_locations.iter() {
            
            let handle = tokio::spawn(send_msg(
                msg.cmd.clone(),
                host.clone(),
                port.clone(),
                self.hmac_key,
            ));

            send_handles.push(handle);
        }

        for handle in send_handles.drain(..) {

            // TODO: errors?
            handle.await.unwrap();
        }
    }
}
