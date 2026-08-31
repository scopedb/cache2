// Copyright 2026 ScopeDB, Inc.
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

//! Region storage facade.
//!
//! `core` owns bounded steady-state operations. `file_backend` composes that
//! core with the runtime data plane and persistent recovery lifecycle. The
//! backend-independent shutdown state machine remains in `region_store`.

pub(crate) mod core;
mod file_backend;

pub(crate) use file_backend::{FileRegionBackend, RegionFiles, SystemRegionFileSystem};
