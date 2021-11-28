
use crate::{
    ClientRegisterCommand, OperationComplete, RegisterClient, SectorsManager, StableStorage,
    SystemRegisterCommand,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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
