use protocol_driver::{HandlerInboundChannelSender, HandlerInboundChannelReceiver, HandlerInboundMessage, HandlerOutboundMessage, DriverErrorType};
use futures::channel::mpsc::SendError;
use futures::{select, SinkExt, StreamExt};
use serde_derive::{Deserialize, Serialize};


struct KVHandler {
    tx: HandlerInboundChannelSender,
    rx: HandlerInboundChannelReceiver
}


#[derive(Deserialize, Serialize)]
enum KVMessage {
    Get{key: String}
}

impl KVHandler {
    pub async fn run_loop(&mut self) -> Result<(), SendError> {
        while let Some(HandlerInboundMessage{data, mut outbound_channel, id} )= self.rx.next().await {
            match serde_json::from_slice(&data) {
                Ok(KVMessage::Get{key}) => {
                    outbound_channel.send(HandlerOutboundMessage::ok(id, vec![])).await?;
                }
                Err(_) => {
                    outbound_channel.send(HandlerOutboundMessage::error(id, DriverErrorType::BadInput, "Not a valid KV message")).await?;
                }

            }

        }
        Ok(())
    }
}