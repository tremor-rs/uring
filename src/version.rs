// Copyright 2018-2019, Wayfair GmbH
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

use slog::Logger;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn print() {
    eprintln!("uring version: {}", VERSION);
}

pub fn log(logger: &Logger) {
    info!(logger, "uring version: {}", VERSION);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn for_coverage_only() {
        print();
        log(&slog::Logger::root(slog::Discard, o!()));
    }
}
