// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// use crate::{NodeId, KV};

use super::*;
use async_std::net::TcpListener;
use async_std::net::ToSocketAddrs;
use async_std::task;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::io::{AsyncRead, AsyncWrite};
use futures::{select, FutureExt, StreamExt};
use protocol_driver::{
    DriverInboundChannelSender, DriverInboundData, DriverInboundMessage,
    DriverOutboundChannelReceiver, DriverOutboundChannelSender, MessageId,
};
use std::io::Error;
use tungstenite::protocol::Message;

/// websocket connection is long running connection, it easier
/// to handle with an actor
pub(crate) struct Connection {
    protocol_driver: DriverInboundChannelSender,
    tx: DriverOutboundChannelSender,
    rx: DriverOutboundChannelReceiver,
    ws_rx: Receiver<Message>,
    ws_tx: Sender<Message>,
}

impl Connection {
    pub(crate) fn new(
        protocol_driver: DriverInboundChannelSender,
        ws_rx: Receiver<Message>,
        ws_tx: Sender<Message>,
    ) -> Self {
        let (tx, rx) = channel(crate::CHANNEL_SIZE);
        Self {
            rx,
            tx,
            protocol_driver,
            ws_rx,
            ws_tx,
        }
    }

    pub async fn msg_loop(mut self, _logger: Logger) {
        loop {
            select! {
                msg = self.ws_rx.next().fuse() => {
                    if let Some(msg) = msg {
                        self.handle_ws_msg(msg).await;
                    } else {
                        dbg!("snot");
                        break
                    }
                }
                msg = self.rx.next() => {
                    if let Some(msg) = msg {
                        match dbg!(msg.data) {
                            Ok(data) => {
                                self.ws_tx.send(Message::Text(String::from_utf8(data).unwrap())).await.unwrap();
                            },
                            Err(e) => {
                                let error = serde_json::to_string(&e).unwrap();
                                println!("Something went wrong: {}", error);
                                self.ws_tx.send(Message::Text(error)).await.unwrap();

                            }
                        }
                    } else {
                        dbg!("snot");
                        break
                    }

                }
                complete => {
                    dbg!("done");
                    break

                }
            };
        }
    }
    async fn handle_ws_msg(&mut self, msg: Message) {
        dbg!(&msg);
        let data = msg.into_data();
        let data = if let Ok(data) = serde_json::from_slice(&data) {
            data
        } else {
            DriverInboundData::Message(data)
        };
        dbg!(&data);
        let msg = DriverInboundMessage {
            data,
            outbound_channel: self.tx.clone(),
            id: MessageId::new(0, 0),
        };
        self.protocol_driver.send(msg).await.unwrap();
    }
}

pub(crate) async fn accept_connection<S>(
    logger: Logger,
    driver: DriverInboundChannelSender,
    stream: S,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut ws_stream = if let Ok(ws_stream) = async_tungstenite::accept_async(stream).await {
        ws_stream
    } else {
        error!(logger, "Error during the websocket handshake occurred");
        return;
    };

    // Create a channel for our stream, which other sockets will use to
    // send us messages. Then register our address with the stream to send
    // data to us.
    let (mut msg_tx, msg_rx) = channel(crate::CHANNEL_SIZE);
    let (response_tx, mut response_rx) = channel(crate::CHANNEL_SIZE);

    let c = Connection::new(driver, msg_rx, response_tx);
    task::spawn(c.msg_loop(logger.clone()));

    loop {
        select! {
            message = ws_stream.next().fuse() => {
                if let Some(Ok(message)) = message {
                    msg_tx
                    .send(message).await
                    .expect("Failed to forward request");
                } else {
                    error!(logger, "Client connection down.", );
                    break;
                }
            }
            resp = response_rx.next() => {
                if let Some(resp) = resp {
                    println!("we got here");
                    ws_stream.send(dbg!(resp)).await.expect("Failed to send response");
                } else {
                    error!(logger, "Client connection down.", );
                    break;
                }

            }
            complete => {
                error!(logger, "Client connection down.", );
                break;
            }
        }
    }
}

pub(crate) async fn run(
    logger: Logger,
    driver: DriverInboundChannelSender,
    addr: String,
) -> Result<(), Error> {
    let addr = addr
        .to_socket_addrs()
        .await
        .expect("Not a valid address")
        .next()
        .expect("Not a socket address");

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    info!(logger, "Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        task::spawn(accept_connection(logger.clone(), driver.clone(), stream));
    }

    Ok(())
}
