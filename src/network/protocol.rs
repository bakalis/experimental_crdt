//! Length-delimited protobuf framing over raw `AsyncRead + AsyncWrite`.
//!
//! Wire format:
//!   [4 bytes big-endian length][protobuf payload …]
//!
//! This keeps the transport layer independent of any specific protobuf
//! message so it can be reused for future message types.

use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::common::error::{Error, Result};
use crate::proto::Envelope;

const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024; // 16 MiB safety cap

/// Write a single `Envelope` to the writer, length-prefixed.
pub async fn write_envelope<W: AsyncWrite + Unpin>(
    writer: &mut W,
    envelope: &Envelope,
) -> Result<()> {
    let len = envelope.encoded_len();
    let mut buf = BytesMut::with_capacity(4 + len);
    buf.put_u32(len as u32);
    envelope.encode(&mut buf)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a single `Envelope` from the reader.
///
/// Returns `Ok(None)` on clean EOF.
pub async fn read_envelope<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Envelope>> {
    // --- read length prefix ---
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes"),
        )));
    }

    // --- read payload ---
    let mut payload = BytesMut::zeroed(len as usize);
    reader.read_exact(&mut payload).await?;
    let envelope = Envelope::decode(payload.chunk())?;
    Ok(Some(envelope))
}
