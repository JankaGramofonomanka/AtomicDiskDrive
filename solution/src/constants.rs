

pub const CLIENT_REQUEST_PADDING_LENGTH:    usize = 8 - 4 - 1;
pub const CLIENT_RESPONSE_PADDING_LENGTH:   usize = 8 - 4 - 2;
pub const SYSTEM_PADDING_LENGTH:            usize = 8 - 4 - 2;
pub const READ_WRITE_PROC_PADDING_LENGTH:   usize = 8 - 1;


pub const SECTOR_SIZE:              usize = 4096;
pub const CONTENT_LENGTH:           usize = SECTOR_SIZE;
pub const SYSTEM_CONTENT_LENGTH:    usize = CONTENT_LENGTH + 16;

pub const READ_TYPE:            u8 = 0x01;
pub const WRITE_TYPE:           u8 = 0x02;

pub const READ_PROC_TYPE:       u8 = 0x03;
pub const VALUE_TYPE:           u8 = 0x04;
pub const WRITE_PROC_TYPE:      u8 = 0x05;
pub const ACK_TYPE:             u8 = 0x06;

pub const READ_RETURN_TYPE:     u8 = 0x40 + 0x01;
pub const WRITE_RETURN_TYPE:    u8 = 0x40 + 0x02;

pub const MAX_CMD_TYPE:         u8 = ACK_TYPE;
pub const MIN_CMD_TYPE:         u8 = READ_TYPE;


