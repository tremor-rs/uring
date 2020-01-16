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

use crate::service::status;
use async_trait::async_trait;
use protocol_driver::{interceptor, DriverErrorType, HandlerInboundMessage, RequestId};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Serialize, Debug)]
enum Request {
    Get { rid: RequestId },
}

#[derive(Default)]
pub struct Handler {
    ids: HashMap<RequestId, RequestId>,
}

#[async_trait]
impl interceptor::Intercept for Handler {
    async fn inbound(&mut self, mut msg: HandlerInboundMessage) -> interceptor::Reply {
        use status::Event;
        msg.service_id = Some(status::ID);
        msg.data = match dbg!(serde_json::from_slice(&msg.data)) {
            Ok(Request::Get { rid }) => {
                self.ids.insert(msg.id, rid);
                Event::get()
            }
            Err(_) => return interceptor::Reply::Err(DriverErrorType::BadInput),
        };
        interceptor::Reply::Ok(msg)
    }

    fn result_id_map(&mut self, id: RequestId) -> Option<RequestId> {
        self.ids.remove(&id)
    }
}

/*
{"Connect": ["status"]}
{"Select": "status"}
{"Get": {"rid":42}}
*/
