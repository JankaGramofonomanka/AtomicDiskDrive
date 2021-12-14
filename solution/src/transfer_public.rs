
use crate::RegisterCommand;
use std::io::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::domain::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;
use uuid::Uuid;
use std::convert::TryInto;
use crate::constants::*;



#[allow(non_snake_case)]
fn INVALID(msg_type: u8) -> bool {
    msg_type > MAX_CMD_TYPE || MIN_CMD_TYPE > msg_type
}

#[allow(non_snake_case)]
fn CLIENT_TYPE(msg_type: u8) -> bool {
    msg_type == READ_TYPE || msg_type == WRITE_TYPE
}

#[allow(non_snake_case)]
fn SYSTEM_TYPE(msg_type: u8) -> bool {
    READ_PROC_TYPE <= msg_type && msg_type <= ACK_TYPE
}

// Copied from dslab05 --------------------------------------------------------
type HmacSha256 = Hmac<Sha256>;
fn verify_hmac_tag(tag: &[u8], message: &[u8], secret_key: &[u8]) -> bool {
    // Initialize a new MAC instance from the secret key:
    let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

    // Calculate MAC for the data (one can provide it in multiple portions):
    mac.update(message);

    // Verify the tag:
    mac.verify(tag).is_ok()
}

fn calculate_hmac_tag(message: &[u8], secret_key: &[u8]) -> [u8; 32] {
    // Initialize a new MAC instance from the secret key:
    let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

    // Calculate MAC for the data (one can provide it in multiple portions):
    mac.update(message);

    // Finalize the computations of MAC and obtain the resulting tag:
    let tag = mac.finalize().into_bytes();

    tag.into()
}



// deserialization ------------------------------------------------------------
pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {

    loop {
        let mut magic_num: [u8; 4] = [0; 4];

        data.read_exact(&mut magic_num).await?;
        while magic_num != MAGIC_NUMBER {
            
            for i in 0..3 { magic_num[i] = magic_num[i + 1]; }
            data.read_exact(&mut magic_num[3..4]).await?;
        }

        let mut after_magic_num: [u8; 4] = [0; 4];
        data.read_exact(&mut after_magic_num).await?;
        
        let msg_type = after_magic_num[3];
        if INVALID(msg_type) {
            continue
        };

        if CLIENT_TYPE(msg_type) {

            let mut req_id_buff: [u8; 8] = [0; 8];
            data.read_exact(&mut req_id_buff).await?;
            
            let mut sec_id_buff: [u8; 8] = [0; 8];
            data.read_exact(&mut sec_id_buff).await?;
            
            let mut content_buff: Vec<u8> = vec![];
            if msg_type == WRITE_TYPE {
                content_buff = vec![0; CONTENT_LENGTH];
                data.read_exact(&mut content_buff[..]).await?;
            }

            let mut hmac_tag: [u8; 32] = [0; 32];
            data.read_exact(&mut hmac_tag).await?;


            let message = [
                &magic_num[..], 
                &after_magic_num[..], 
                &req_id_buff[..], 
                &sec_id_buff[..],
                &content_buff[..],
            ].concat();

            

            let msg_valid = verify_hmac_tag(&hmac_tag, &message, hmac_client_key);
            
            
            let request_identifier: u64 = u64::from_be_bytes(req_id_buff);
            let sector_idx: SectorIdx = u64::from_be_bytes(sec_id_buff);

            let header = ClientCommandHeader {
                request_identifier: request_identifier,
                sector_idx:         sector_idx,
            };
            let content = match msg_type {
                READ_TYPE
                    => ClientRegisterCommandContent::Read,

                WRITE_TYPE
                    => ClientRegisterCommandContent::Write { data: SectorVec(content_buff) },
                
                _type
                    => panic!("impossible case"),
            };

            let cmd = ClientRegisterCommand {
                header:     header,
                content:    content,
            };

            // TODO, Are you sure, that the `bool` in the tuple is supposed 
            // to be what it is?
            return Ok((RegisterCommand::Client(cmd), msg_valid));
              
        } else if SYSTEM_TYPE(msg_type) {
            

            let mut uuid_buff: [u8; 16] = [0; 16];
            data.read_exact(&mut uuid_buff).await?;

            let mut read_id_buff: [u8; 8] = [0; 8];
            data.read_exact(&mut read_id_buff).await?;
            
            let mut sec_id_buff: [u8; 8] = [0; 8];
            data.read_exact(&mut sec_id_buff).await?;

            let mut content_buff: Vec<u8> = vec![];
            if msg_type == VALUE_TYPE || msg_type == WRITE_PROC_TYPE {
                content_buff = vec![0; SYSTEM_CONTENT_LENGTH];
                data.read_exact(&mut content_buff[..]).await?;
            }

            let mut hmac_tag: [u8; 32] = [0; 32];
            data.read_exact(&mut hmac_tag).await?;

            let message = [
                &magic_num[..], 
                &after_magic_num[..], 
                &uuid_buff[..], 
                &read_id_buff[..], 
                &sec_id_buff[..],
                &content_buff[..],
            ].concat();

            let msg_valid = verify_hmac_tag(&hmac_tag, &message, hmac_system_key);

            let process_rank = after_magic_num[2];
            let uuid = Uuid::from_bytes(uuid_buff);
            let read_ident = u64::from_be_bytes(read_id_buff);
            let sector_idx = u64::from_be_bytes(sec_id_buff);

            let header = SystemCommandHeader {
                process_identifier:     process_rank,
                msg_ident:              uuid,
                read_ident:             read_ident,
                sector_idx:             sector_idx,
            };

            let content = match msg_type {
                READ_PROC_TYPE  => SystemRegisterCommandContent::ReadProc,
                ACK_TYPE        => SystemRegisterCommandContent::Ack,
                
                _value_or_write_proc => {
                    let timestamp_buff: [u8; 8] = content_buff[..8].try_into().unwrap();
                    let timestamp = u64::from_be_bytes(timestamp_buff);

                    let write_rank: u8 = content_buff[16];

                    let sector_data = SectorVec(content_buff[16..].to_vec());

                    if msg_type == VALUE_TYPE {
                        SystemRegisterCommandContent::Value {
                            timestamp:      timestamp,
                            write_rank:     write_rank,
                            sector_data:    sector_data,
                        }

                    } else if msg_type == WRITE_PROC_TYPE {
                        SystemRegisterCommandContent::WriteProc {
                            timestamp:      timestamp,
                            write_rank:     write_rank,
                            data_to_write:  sector_data,
                        }

                    } else {
                        panic!("impossible case");
                    }
                },
            
            };

            let cmd = SystemRegisterCommand {
                header:     header,
                content:    content,
            };

            // TODO, Are you sure, that the `bool` in the tuple is supposed 
            // to be what it is?
            return Ok((RegisterCommand::System(cmd), msg_valid));

        }

    }
}



// serialization --------------------------------------------------------------
pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {


    match cmd {
        RegisterCommand::Client(cmd)
            => serialize_client_command(cmd, writer, hmac_key).await?,

        RegisterCommand::System(cmd)
            => serialize_system_command(cmd, writer, hmac_key).await?,
    }

    Ok(())
}



pub async fn serialize_client_command(
    cmd: &ClientRegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {

    let mut after_magic_num = [0; 4];
    let msg_type = match cmd.content {
        ClientRegisterCommandContent::Read              => READ_TYPE,
        ClientRegisterCommandContent::Write { data: _ } => WRITE_TYPE,
    };
    after_magic_num[3] = msg_type;

    let empty_vec = vec![];
    let content = match &cmd.content {
        ClientRegisterCommandContent::Read              => &empty_vec,

        // TODO: check if legth of `content.length` equals `CONTENT_LENGTH`?
        ClientRegisterCommandContent::Write { data }    => &data.0,
    };

    let message = [
        &MAGIC_NUMBER[..],
        &after_magic_num[..],

        &cmd.header.request_identifier.to_be_bytes()[..],
        &cmd.header.sector_idx.to_be_bytes()[..],

        &content[..],
    ].concat();

    let hmac_tag = calculate_hmac_tag(&message, hmac_key);

    let msg = [&message[..], &hmac_tag[..]].concat();
    writer.write_all(&msg).await
}

pub async fn serialize_system_command(
    cmd: &SystemRegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {


    let mut after_magic_num = [0; 4];
    let msg_type = match &cmd.content {
        SystemRegisterCommandContent::ReadProc => READ_PROC_TYPE,
        SystemRegisterCommandContent::Value {
                timestamp: _,
                write_rank: _,
                sector_data: _,
            } => VALUE_TYPE,

        SystemRegisterCommandContent::WriteProc {
                timestamp: _,
                write_rank: _,
                data_to_write: _,
            } => WRITE_PROC_TYPE,

        SystemRegisterCommandContent::Ack => ACK_TYPE,
    };
    after_magic_num[2] = cmd.header.process_identifier;
    after_magic_num[3] = msg_type;

    let content = match &cmd.content {
        SystemRegisterCommandContent::ReadProc => vec![],

        // TODO: check if legth of `sectr_data` equals `CONTENT_LENGTH`?
        SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => system_content(timestamp, write_rank, sector_data),

        // TODO: check if legth of `data_to_write` equals `CONTENT_LENGTH`?
        SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => system_content(timestamp, write_rank, data_to_write),

        SystemRegisterCommandContent::Ack => vec![],
    };

    let message = [
        &MAGIC_NUMBER[..],
        &after_magic_num[..],

        // According to documentation Uuid::as_bytes() returns &[u8; 16]
        &cmd.header.msg_ident.as_bytes()[..],
        &cmd.header.read_ident.to_be_bytes()[..],
        &cmd.header.sector_idx.to_be_bytes()[..],

        &content[..],
    ].concat();
    

    let hmac_tag = calculate_hmac_tag(&message, hmac_key);

    let msg = [&message[..], &hmac_tag[..]].concat();
    writer.write_all(&msg).await

}

fn system_content(timestamp: &u64, write_rank: &u8, sector_data: &SectorVec) -> Vec<u8> {

    let mut after_timestamp = [0; 8];
    after_timestamp[7] = *write_rank;

    [
        &timestamp.to_be_bytes()[..],
        &after_timestamp[..],

        &sector_data.0[..]

    ].concat()
}

pub async fn serialize_response(
    response: &OperationComplete,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    
    let mut after_magic_num = [0; 4];
    let msg_type = match response.op_return {
        OperationReturn::Read(_)    => READ_RETURN_TYPE,
        OperationReturn::Write      => WRITE_RETURN_TYPE,
    };
    after_magic_num[2] = response.status_code as u8;
    after_magic_num[3] = msg_type;

    let empty_vec = vec![];
    let content = match &response.op_return {

        // TODO: check if legth of `sectr_data` equals `CONTENT_LENGTH`?
        OperationReturn::Read(read_ret) => match &read_ret.read_data {
            Some(data)  => &data.0,

            // TODO: is this ok?
            None        => &empty_vec,
        },

        OperationReturn::Write      => &empty_vec,

    };

    let message = [
        &MAGIC_NUMBER[..],
        &after_magic_num[..],
        &response.request_identifier.to_be_bytes(),

        &content[..],
    ].concat();

    let hmac_tag = calculate_hmac_tag(&message, hmac_key);

    let msg = [&message[..], &hmac_tag[..]].concat();
    writer.write_all(&msg).await
}
