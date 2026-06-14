use core::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error};
use tokio::task::JoinHandle;
use tokio::net::TcpListener;

use crate::crdt::or_set::OrSetOp;
use crate::proto;
use crate::engine::crdt_engine::CrdtEngineRequest;
use crate::server::types::CrdtType;

pub async fn send_engine_request(engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>, request: CrdtEngineRequest<CrdtType>) -> String {
    if let Err(e) = engine_tx.send(request).await {
        warn!(%e, "failed to send client operation to engine");
        format!("error: {e}")
    } else {
        "ok".to_string()
    }
}

pub async fn send_engine_request_and_wait_response(engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>, 
    request: CrdtEngineRequest<CrdtType>,
    response_rx: tokio::sync::oneshot::Receiver<String>
) -> String {
    if let Err(e) = engine_tx.send(request).await {
        warn!(%e, "failed to send request to engine");
        format!("error: {e}")
    } else {
        match response_rx.await {
            Ok(state_str) => state_str,
            Err(e) => {
                warn!(%e, "failed to receive response from engine");
                format!("error: {e}")
            }
        }
    }
}

pub fn start_client_handle(client_port: Option<String>, engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>) -> Option<JoinHandle<()>> {
    client_port.map(|port| tokio::spawn(async move {
        let client_addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();  
        let listener = match TcpListener::bind(client_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(%client_addr, %e, "failed to bind client listener");
                return;
            }
        };
        info!(%client_addr, "client op listener started");
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    info!(%peer, "client connected");
                    let eng_tx = engine_tx.clone();
                    tokio::spawn(async move {
                        let (mut read_half, mut write_half) = stream.into_split();
                        loop {
                            // Read 4-byte big-endian length prefix.
                            let mut len_buf = [0u8; 4];
                            if read_half.read_exact(&mut len_buf).await.is_err() {
                                break; // connection closed
                            }
                            let msg_len = u32::from_be_bytes(len_buf) as usize;
                            let mut msg_buf = vec![0u8; msg_len];
                            if read_half.read_exact(&mut msg_buf).await.is_err() {
                                break;
                            }

                            use prost::Message as _;
                            let response = match proto::ProtoClientCommand::decode(msg_buf.as_slice()) {
                                Ok(cmd) => match cmd.command {
                                    Some(proto::proto_client_command::Command::Add(value)) => {
                                        send_engine_request(eng_tx.clone(), CrdtEngineRequest::ClientOperation(OrSetOp::Add(value))).await
                                    }
                                    Some(proto::proto_client_command::Command::Remove(value)) => {
                                        send_engine_request(eng_tx.clone(), CrdtEngineRequest::ClientOperation(OrSetOp::Remove(value))).await
                                    }
                                    Some(proto::proto_client_command::Command::RemoveRandom(_)) => {
                                        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                                        let random_element = send_engine_request_and_wait_response(eng_tx.clone(), CrdtEngineRequest::GetRandomElement(response_tx), response_rx).await;
                                        if random_element.starts_with("error:") {
                                            random_element
                                        } else {
                                            send_engine_request(eng_tx.clone(), CrdtEngineRequest::ClientOperation(OrSetOp::Remove(random_element))).await
                                        }
                                    }
                                    Some(proto::proto_client_command::Command::PrintState(_)) => {
                                        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                                        send_engine_request_and_wait_response(eng_tx.clone(), CrdtEngineRequest::PrintState(response_tx), response_rx).await
                                    }
                                    Some(proto::proto_client_command::Command::PrintInternals(_)) => {
                                        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                                        send_engine_request_and_wait_response(eng_tx.clone(), CrdtEngineRequest::PrintInternals(response_tx), response_rx).await
                                    }
                                    Some(proto::proto_client_command::Command::PrintMatrix(_)) => {
                                        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                                        send_engine_request_and_wait_response(eng_tx.clone(), CrdtEngineRequest::PrintMatrix(response_tx), response_rx).await
                                    }
                                    Option::None => "error: empty command".to_string(),
                                },
                                Err(e) => {
                                    warn!(%peer, %e, "invalid client command; failed to decode protobuf");
                                    format!("error: {e}")
                                }
                            };

                            // Send response: 4-byte length prefix + UTF-8 bytes.
                            let resp_bytes = response.into_bytes();
                            let resp_len = resp_bytes.len() as u32;
                            if write_half.write_all(&resp_len.to_be_bytes()).await.is_err()
                            {
                                break;
                            }
                            if write_half.write_all(&resp_bytes).await.is_err() {
                                break;
                            }
                        }
                        info!(%peer, "client disconnected");
                    });
                }
                Err(e) => error!(%e, "client accept failed"),
            }
        }
    }))
}
