//! Standard owned storage for L1 key/value bytes.
//!
//! `MemoryArena` remains the shard-local allocation boundary, while each
//! value uses ordinary `Arc` ownership. The cache charge lives in the shared
//! allocation and is released exactly once after the final returned handle.

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use crate::memory::{MEMORY_ENTRY_OVERHEAD_BYTES, MemoryBudget, MemoryCharge};

pub(crate) struct MemoryArena;

impl MemoryArena {
    pub(crate) fn new(_budget: Arc<MemoryBudget>) -> io::Result<Self> {
        Ok(Self)
    }

    pub(crate) fn allocate(
        &mut self,
        key: &[u8],
        value: &[u8],
        charge: MemoryCharge,
    ) -> Result<MemoryValue, MemoryCharge> {
        let Some(length) = key.len().checked_add(value.len()) else {
            return Err(charge);
        };
        let mut bytes = Vec::new();
        if bytes.try_reserve_exact(length).is_err() {
            return Err(charge);
        }
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        Ok(MemoryValue(Arc::new(MemoryValueInner {
            bytes: bytes.into_boxed_slice(),
            key_length: key.len(),
            _charge: charge,
        })))
    }
}

struct MemoryValueInner {
    bytes: Box<[u8]>,
    key_length: usize,
    _charge: MemoryCharge,
}

#[derive(Clone)]
pub(crate) struct MemoryValue(Arc<MemoryValueInner>);

impl MemoryValue {
    pub(crate) fn charged_bytes(&self) -> usize {
        MEMORY_ENTRY_OVERHEAD_BYTES + self.0.bytes.len()
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.0.bytes[..self.0.key_length]
    }

    fn value(&self) -> &[u8] {
        &self.0.bytes[self.0.key_length..]
    }
}

impl Deref for MemoryValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl AsRef<[u8]> for MemoryValue {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
