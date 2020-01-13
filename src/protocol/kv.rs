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

use crate::service::kv;
use async_trait::async_trait;
use protocol_driver::{interceptor, DriverErrorType, HandlerInboundMessage};
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
enum Request {
    Get {
        key: String,
    },
    Put {
        key: String,
        store: String,
    },
    Delete {
        key: String,
    },
    Cas {
        key: String,
        check: Option<String>,
        store: String,
    },
}

struct Handler {}

#[async_trait]
impl interceptor::Intercept for Handler {
    async fn inbound(&mut self, mut msg: HandlerInboundMessage) -> interceptor::Reply {
        use kv::Event;
        msg.service_id = Some(kv::ID);
        msg.data = match serde_json::from_slice(&msg.data) {
            Ok(Request::Get { key }) => Event::get(key.into_bytes()),
            Ok(Request::Put { key, store }) => Event::put(key.into_bytes(), store.into_bytes()),
            Ok(Request::Delete { key }) => Event::delete(key.into_bytes()),
            Ok(Request::Cas { key, check, store }) => kv::Event::cas(
                key.into_bytes(),
                check.map(String::into_bytes),
                store.into_bytes(),
            ),
            Err(_) => return interceptor::Reply::Err(DriverErrorType::BadInput),
        };
        interceptor::Reply::Ok(msg)
    }
}
