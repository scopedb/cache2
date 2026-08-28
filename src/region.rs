//! Region storage facade.
//!
//! `core` owns bounded steady-state operations. `file_backend` composes that
//! core with the runtime data plane and persistent recovery lifecycle. The
//! backend-independent shutdown state machine remains in `region_store`.

pub(crate) mod core;
mod file_backend;

pub(crate) use file_backend::{FileRegionBackend, RegionFiles, SystemRegionFileSystem};
