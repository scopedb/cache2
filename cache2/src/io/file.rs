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

//! Positioned reads and writes, owned cache files, and durability operations.
//!
//! Persistence points are carried through the trait so tests can fail an exact
//! record, superblock, or barrier operation without changing the cache
//! algorithm.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::config::runtime::IoMode;
use crate::snapshot::CacheIoPathSnapshot;

pub const DIRECT_IO_ALIGNMENT: usize = 4096;
pub const MAX_INTERRUPTED_RETRIES: usize = 4;
#[cfg(target_os = "linux")]
const LINUX_EINTR: i32 = 4;
#[cfg(unix)]
const SAFE_CACHE_OPEN_FLAGS: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileIoPath {
    Buffered,
    Direct,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileIoDirection {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileIoDirectionStats {
    pub buffered: CacheIoPathSnapshot,
    pub direct: CacheIoPathSnapshot,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileIoStats {
    pub direct_active: bool,
    pub read: FileIoDirectionStats,
    pub write: FileIoDirectionStats,
}

#[derive(Clone)]
pub struct FileIoStatsHandle {
    inner: Arc<FileIoCounters>,
}

struct FileIoCounters {
    direct_active: bool,
    activity_counters_enabled: AtomicBool,
    read: FileIoDirectionCounters,
    write: FileIoDirectionCounters,
}

struct FileIoDirectionCounters {
    buffered: FileIoPathCounters,
    direct: FileIoPathCounters,
}

struct FileIoPathCounters {
    operations: AtomicU64,
    bytes: AtomicU64,
}

impl FileIoPathCounters {
    fn new() -> Self {
        Self {
            operations: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> CacheIoPathSnapshot {
        CacheIoPathSnapshot {
            operations: self.operations.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

impl FileIoDirectionCounters {
    fn new() -> Self {
        Self {
            buffered: FileIoPathCounters::new(),
            direct: FileIoPathCounters::new(),
        }
    }

    fn snapshot(&self) -> FileIoDirectionStats {
        FileIoDirectionStats {
            buffered: self.buffered.snapshot(),
            direct: self.direct.snapshot(),
        }
    }
}

impl FileIoStatsHandle {
    pub fn new(direct_active: bool) -> Self {
        Self {
            inner: Arc::new(FileIoCounters {
                direct_active,
                activity_counters_enabled: AtomicBool::new(true),
                read: FileIoDirectionCounters::new(),
                write: FileIoDirectionCounters::new(),
            }),
        }
    }

    fn record(&self, direction: FileIoDirection, path: FileIoPath, length: usize) {
        if !self.inner.activity_counters_enabled.load(Ordering::Relaxed) {
            return;
        }
        let bytes = u64::try_from(length).unwrap_or(u64::MAX);
        let direction = match direction {
            FileIoDirection::Read => &self.inner.read,
            FileIoDirection::Write => &self.inner.write,
        };
        let path = match path {
            FileIoPath::Buffered => &direction.buffered,
            FileIoPath::Direct => &direction.direct,
        };
        path.operations.fetch_add(1, Ordering::Relaxed);
        path.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn set_activity_counters_enabled(&self, enabled: bool) {
        self.inner
            .activity_counters_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FileIoStats {
        FileIoStats {
            direct_active: self.inner.direct_active,
            read: self.inner.read.snapshot(),
            write: self.inner.write.snapshot(),
        }
    }
}

/// Engine-owned descriptors for one cache file. `buffered` is duplicated from
/// the descriptor that owns flock, so retaining this set also retains the
/// cache lock if an issued write or flush cannot be fenced. `direct`, when present,
/// is a separate O_DIRECT open used only for aligned runtime data requests.
pub struct DataFileHandles {
    buffered: File,
    direct: Option<File>,
    stats: FileIoStatsHandle,
}

impl DataFileHandles {
    #[cfg(test)]
    pub fn buffered(file: File) -> Self {
        Self {
            buffered: file,
            direct: None,
            stats: FileIoStatsHandle::new(false),
        }
    }

    #[cfg(test)]
    pub fn new(buffered: File, direct: Option<File>) -> Self {
        Self::with_direct(buffered, direct)
    }

    fn with_direct(buffered: File, direct: Option<File>) -> Self {
        let direct_active = direct.is_some();
        Self {
            buffered,
            direct,
            stats: FileIoStatsHandle::new(direct_active),
        }
    }

    pub fn select_path(
        &self,
        buffer: *const u8,
        length: usize,
        offset: u64,
        allow_direct: bool,
    ) -> FileIoPath {
        if !allow_direct || self.direct.is_none() {
            return FileIoPath::Buffered;
        }
        // Never issue malformed O_DIRECT. Unaligned record fragments and an
        // unaligned remainder after a positive short completion use the
        // buffered compatibility path. Direct mode requires the direct
        // descriptor but does not make 32-byte-aligned records unreadable.
        if direct_io_aligned(buffer, length, offset) {
            FileIoPath::Direct
        } else {
            FileIoPath::Buffered
        }
    }

    pub fn record(&self, direction: FileIoDirection, path: FileIoPath, length: usize) {
        self.stats.record(direction, path, length);
    }

    pub fn stats_handle(&self) -> FileIoStatsHandle {
        self.stats.clone()
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            buffered: self.buffered.try_clone()?,
            direct: self.direct.as_ref().map(File::try_clone).transpose()?,
            stats: self.stats.clone(),
        })
    }

    #[cfg(unix)]
    pub fn file_for(&self, path: FileIoPath) -> &File {
        match path {
            FileIoPath::Buffered => &self.buffered,
            FileIoPath::Direct => self
                .direct
                .as_ref()
                .expect("direct path requires a direct descriptor"),
        }
    }
}

fn direct_io_aligned(buffer: *const u8, length: usize, offset: u64) -> bool {
    !buffer.is_null()
        && (buffer as usize).is_multiple_of(DIRECT_IO_ALIGNMENT)
        && length != 0
        && length.is_multiple_of(DIRECT_IO_ALIGNMENT)
        && offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WritePoint {
    Record,
    DataSuperblock,
    State,
    RecoveryImageHeader,
    RecoveryImageIndex,
    RecoveryImageMetadata,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPoint {
    FormatTruncate,
    FormatData,
    StateReset,
    RunningState,
    WarmData,
    RecoveryImage,
    CleanState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    Data,
    All,
}

/// Synchronous reads and writes at explicit byte offsets.
pub trait PositionedIo: Send + Sync {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize>;
    /// Reads into storage that may not yet be initialized.
    ///
    /// # Safety
    ///
    /// `buffer` must be valid for writes of `length` bytes and remain live for
    /// the duration of the call. On success, the first returned byte count must
    /// have been initialized by the implementation.
    unsafe fn read_at_uninit(
        &self,
        buffer: *mut u8,
        length: usize,
        offset: u64,
    ) -> io::Result<usize> {
        // Slice-based implementations require initialized storage. Data-file
        // handles override this method to let the kernel initialize it directly.
        // SAFETY: upheld by the caller; zeroing establishes initialized bytes
        // before constructing the mutable slice required by `read_at`.
        unsafe {
            buffer.write_bytes(0, length);
            self.read_at(slice::from_raw_parts_mut(buffer, length), offset)
        }
    }
    fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize>;
}

/// File ownership, extent management, and durability for cache persistence.
///
/// Recovery reads through [`PositionedIo`], synchronizes through this interface,
/// and clones the same validated descriptor for an immutable private mapping. File identity is
/// intentionally descriptor-based so callers never need to reopen a path between validation and
/// `mmap`.
pub trait StorageFile: PositionedIo {
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.set_len(len)
    }
    fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()>;
    fn try_lock_exclusive(&self) -> io::Result<()>;
    fn unlock(&self) -> io::Result<()>;

    fn try_clone_mapping_file(&self) -> io::Result<File>;

    fn try_clone_data_handles(&self) -> io::Result<DataFileHandles>;

    fn identity(&self) -> io::Result<FileIdentity>;

    fn is_same_file(&self, other: &dyn StorageFile) -> io::Result<bool> {
        Ok(self.identity()? == other.identity()?)
    }
}

/// Stable identity of one open regular file within the running system.
///
/// The fields remain opaque: recovery code only needs equality to reject
/// aliased data, state, and image descriptors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileIdentity {
    device: u64,
    inode: u64,
}

pub struct CacheFile {
    /// Buffered control descriptor and flock owner.
    file: File,
    /// Separate Linux O_DIRECT descriptor for aligned runtime data I/O.
    direct: Option<File>,
}

impl CacheFile {
    #[cfg(test)]
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_io_mode(path, IoMode::Buffered)
    }

    pub fn open_with_io_mode(path: &Path, mode: IoMode) -> io::Result<Self> {
        Self::open_with_io_mode_and_create(path, mode, true)
    }

    pub fn open_existing_with_io_mode(path: &Path, mode: IoMode) -> io::Result<Self> {
        Self::open_with_io_mode_and_create(path, mode, false)
    }

    /// Atomically creates a new buffered control file without following a
    /// symbolic link or opening an existing recovery-image target.
    pub fn create_new_buffered(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(SAFE_CACHE_OPEN_FLAGS)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache path must be a regular file",
            ));
        }
        Ok(Self { file, direct: None })
    }

    fn open_with_io_mode_and_create(path: &Path, mode: IoMode, create: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .custom_flags(SAFE_CACHE_OPEN_FLAGS)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache path must be a regular file",
            ));
        }
        if mode == IoMode::Buffered {
            return Ok(Self { file, direct: None });
        }

        #[cfg(target_os = "linux")]
        {
            let direct = open_direct(path, &file)?;
            Ok(Self {
                file,
                direct: Some(direct),
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "O_DIRECT is supported only on Linux",
            ))
        }
    }
}

#[cfg(target_os = "macos")]
fn preallocate_macos(file: &File, len: i64) -> io::Result<()> {
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len,
        fst_bytesalloc: 0,
    };
    // SAFETY: `file` owns a valid regular-file descriptor and `store` remains
    // live and writable for the duration of the call. F_ALLOCATEALL requires
    // the complete request to be allocated atomically.
    let allocated = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
    if allocated == -1 {
        return Err(io::Error::last_os_error());
    }
    if store.fst_bytesalloc < len {
        return Err(io::Error::other(
            "macOS physical preallocation completed with a short extent",
        ));
    }
    Ok(())
}

#[cfg(unix)]
impl StorageFile for CacheFile {
    fn try_clone_data_handles(&self) -> io::Result<DataFileHandles> {
        let buffered = self.file.try_clone()?;
        let direct = self.direct.as_ref().map(File::try_clone).transpose()?;
        Ok(DataFileHandles::with_direct(buffered, direct))
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn preallocate(&self, len: u64) -> io::Result<()> {
        if len == 0 {
            return self.file.set_len(0);
        }
        #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
        let linux_len = i64::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation length exceeds Linux off_t",
            )
        })?;
        #[cfg(target_os = "macos")]
        let macos_len = i64::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation length exceeds macOS off_t",
            )
        })?;
        #[cfg(target_os = "macos")]
        {
            self.file.set_len(0)?;
            preallocate_macos(&self.file, macos_len)?;
            self.file.set_len(len)
        }
        #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
        {
            // Preserve set_len's exact truncate/extend behavior before
            // allocating physical blocks for the final extent.
            self.file.set_len(len)?;
            let mut interrupted_retries = 0;
            let error = loop {
                // SAFETY: `file` owns a valid regular-file descriptor and
                // both offsets are representable non-negative off_t values.
                let error = unsafe { posix_fallocate(self.file.as_raw_fd(), 0, linux_len) };
                if error != LINUX_EINTR || interrupted_retries == MAX_INTERRUPTED_RETRIES {
                    break error;
                }
                interrupted_retries += 1;
            };
            if error == 0 {
                return Ok(());
            }
            Err(io::Error::from_raw_os_error(error))
        }
        #[cfg(not(any(
            all(target_os = "linux", target_pointer_width = "64"),
            target_os = "macos"
        )))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "physical cache-file preallocation is unsupported on this platform",
            ))
        }
    }

    fn sync(&self, #[expect(unused_variables)] point: SyncPoint, mode: SyncMode) -> io::Result<()> {
        match mode {
            SyncMode::Data => self.file.sync_data(),
            SyncMode::All => self.file.sync_all(),
        }
    }

    fn try_lock_exclusive(&self) -> io::Result<()> {
        const LOCK_EX: i32 = 2;
        const LOCK_NB: i32 = 4;
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        let result = unsafe { flock(self.file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn unlock(&self) -> io::Result<()> {
        const LOCK_UN: i32 = 8;
        // SAFETY: `file` owns a valid descriptor for the duration of this call.
        let result = unsafe { flock(self.file.as_raw_fd(), LOCK_UN) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn try_clone_mapping_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    fn identity(&self) -> io::Result<FileIdentity> {
        let metadata = self.file.metadata()?;
        Ok(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[cfg(unix)]
impl PositionedIo for CacheFile {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.read_at(buffer, offset)
    }

    fn write_at(
        &self,
        #[expect(unused_variables)] point: WritePoint,
        buffer: &[u8],
        offset: u64,
    ) -> io::Result<usize> {
        self.file.write_at(buffer, offset)
    }
}

#[cfg(unix)]
impl PositionedIo for DataFileHandles {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        let path = self.select_path(buffer.as_ptr(), buffer.len(), offset, true);
        let result = self.file_for(path).read_at(buffer, offset);
        if let Ok(bytes) = result
            && bytes != 0
        {
            self.record(FileIoDirection::Read, path, bytes);
        }
        result
    }

    unsafe fn read_at_uninit(
        &self,
        buffer: *mut u8,
        length: usize,
        offset: u64,
    ) -> io::Result<usize> {
        let path = self.select_path(buffer.cast_const(), length, offset, true);
        // SAFETY: the caller supplies a writable destination for `length`
        // bytes; the selected descriptor is held by `self` for this call.
        let result = unsafe { read_file_at_uninit(self.file_for(path), buffer, length, offset) };
        if let Ok(bytes) = result
            && bytes != 0
        {
            self.record(FileIoDirection::Read, path, bytes);
        }
        result
    }

    fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
        let path = self.select_path(
            buffer.as_ptr(),
            buffer.len(),
            offset,
            point == WritePoint::Record,
        );
        let result = self.file_for(path).write_at(buffer, offset);
        if let Ok(bytes) = result
            && bytes != 0
        {
            self.record(FileIoDirection::Write, path, bytes);
        }
        result
    }
}

#[cfg(target_os = "linux")]
fn open_direct(path: &Path, buffered: &File) -> io::Result<File> {
    // OpenOptions supplies the access mode and O_CLOEXEC. libc supplies the
    // architecture-correct Linux O_DIRECT value.
    let direct = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let buffered_metadata = buffered.metadata()?;
    let direct_metadata = direct.metadata()?;
    if buffered_metadata.dev() != direct_metadata.dev()
        || buffered_metadata.ino() != direct_metadata.ino()
    {
        return Err(io::Error::other(
            "cache path changed while opening its O_DIRECT descriptor",
        ));
    }
    Ok(direct)
}

pub fn read_exact_at(io: &dyn PositionedIo, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    read_exact_at_with_progress(io, buffer, offset).0
}

pub fn read_at_bounded(io: &dyn PositionedIo, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    retry_interrupted(|| io.read_at(buffer, offset))
}

fn read_exact_at_with_progress(
    io: &dyn PositionedIo,
    mut buffer: &mut [u8],
    mut offset: u64,
) -> (io::Result<()>, usize) {
    let mut transferred = 0_usize;
    while !buffer.is_empty() {
        let read = match read_at_bounded(io, buffer, offset) {
            Err(error) => return (Err(error), transferred),
            Ok(read) => read,
        };
        if read == 0 {
            return (
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "short positioned read",
                )),
                transferred,
            );
        }
        if read > buffer.len() {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positioned read exceeds the supplied buffer",
                )),
                transferred,
            );
        }
        transferred += read;
        offset = match offset.checked_add(read as u64) {
            Some(offset) => offset,
            None => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow",
                    )),
                    transferred,
                );
            }
        };
        buffer = &mut buffer[read..];
    }
    (Ok(()), transferred)
}

pub fn read_exact_at_uninit_with_progress(
    io: &dyn PositionedIo,
    buffer: *mut u8,
    length: usize,
    mut offset: u64,
) -> (io::Result<()>, usize) {
    let mut transferred = 0_usize;
    while transferred < length {
        let remaining = length - transferred;
        let read = match retry_interrupted(|| {
            // SAFETY: the caller owns a destination valid for `length` bytes,
            // and the unchanged suffix bounds remain valid across retries.
            unsafe { io.read_at_uninit(buffer.add(transferred), remaining, offset) }
        }) {
            Err(error) => return (Err(error), transferred),
            Ok(read) => read,
        };
        if read == 0 {
            return (
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "short positioned read",
                )),
                transferred,
            );
        }
        if read > remaining {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positioned read exceeds the supplied buffer",
                )),
                transferred,
            );
        }
        transferred += read;
        offset = match offset.checked_add(read as u64) {
            Some(offset) => offset,
            None => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow",
                    )),
                    transferred,
                );
            }
        };
    }
    (Ok(()), transferred)
}

#[cfg(unix)]
unsafe fn read_file_at_uninit(
    file: &File,
    buffer: *mut u8,
    length: usize,
    offset: u64,
) -> io::Result<usize> {
    if length == 0 {
        return Ok(0);
    }
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "read offset exceeds off_t"))?;
    // SAFETY: the caller guarantees that `buffer` is writable for `length`
    // bytes, and `file` owns a valid descriptor for the duration of the call.
    let result = unsafe { libc::pread(file.as_raw_fd(), buffer.cast(), length, offset) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

pub fn write_all_at(
    io: &dyn PositionedIo,
    point: WritePoint,
    buffer: &[u8],
    offset: u64,
) -> io::Result<()> {
    write_all_at_with_progress(io, point, buffer, offset).0
}

pub fn write_all_at_with_progress(
    io: &dyn PositionedIo,
    point: WritePoint,
    mut buffer: &[u8],
    mut offset: u64,
) -> (io::Result<()>, usize) {
    let mut transferred = 0_usize;
    while !buffer.is_empty() {
        let written = match retry_interrupted(|| io.write_at(point, buffer, offset)) {
            Err(error) => return (Err(error), transferred),
            Ok(written) => written,
        };
        if written == 0 {
            return (
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short positioned write",
                )),
                transferred,
            );
        }
        if written > buffer.len() {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positioned write exceeds the supplied buffer",
                )),
                transferred,
            );
        }
        transferred += written;
        offset = match offset.checked_add(written as u64) {
            Some(offset) => offset,
            None => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow",
                    )),
                    transferred,
                );
            }
        };
        buffer = &buffer[written..];
    }
    (Ok(()), transferred)
}

pub fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut retries = 0_usize;
    loop {
        match operation() {
            Err(error)
                if error.kind() == io::ErrorKind::Interrupted
                    && retries < MAX_INTERRUPTED_RETRIES =>
            {
                retries += 1;
            }
            result => return result,
        }
    }
}

#[cfg(unix)]
unsafe extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
unsafe extern "C" {
    fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::fixtures::TestFile;

    #[repr(align(4096))]
    struct AlignedBytes([u8; 2 * DIRECT_IO_ALIGNMENT]);

    #[derive(Default)]
    struct InterruptedIo {
        calls: AtomicUsize,
    }

    impl PositionedIo for InterruptedIo {
        fn read_at(
            &self,
            #[expect(unused_variables)] buffer: &mut [u8],
            #[expect(unused_variables)] offset: u64,
        ) -> io::Result<usize> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "interrupted read",
            ))
        }

        fn write_at(
            &self,
            #[expect(unused_variables)] point: WritePoint,
            #[expect(unused_variables)] buffer: &[u8],
            #[expect(unused_variables)] offset: u64,
        ) -> io::Result<usize> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "interrupted write",
            ))
        }
    }

    #[test]
    fn exact_io_stops_after_the_interrupted_retry_budget() {
        let mut initialized = [0_u8; 1];
        let io = InterruptedIo::default();
        let (result, transferred) = read_exact_at_with_progress(&io, &mut initialized, 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(transferred, 0);
        assert_eq!(
            io.calls.load(Ordering::Relaxed),
            MAX_INTERRUPTED_RETRIES + 1
        );

        let mut uninitialized = std::mem::MaybeUninit::<u8>::uninit();
        let io = InterruptedIo::default();
        let (result, transferred) =
            read_exact_at_uninit_with_progress(&io, uninitialized.as_mut_ptr(), 1, 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(transferred, 0);
        assert_eq!(
            io.calls.load(Ordering::Relaxed),
            MAX_INTERRUPTED_RETRIES + 1
        );

        let io = InterruptedIo::default();
        let (result, transferred) = write_all_at_with_progress(&io, WritePoint::Record, &[1], 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(transferred, 0);
        assert_eq!(
            io.calls.load(Ordering::Relaxed),
            MAX_INTERRUPTED_RETRIES + 1
        );
    }

    #[test]
    fn direct_alignment_requires_pointer_length_and_offset() {
        let bytes = AlignedBytes([0; 2 * DIRECT_IO_ALIGNMENT]);
        let pointer = bytes.0.as_ptr();
        assert!(direct_io_aligned(pointer, DIRECT_IO_ALIGNMENT, 0));
        assert!(direct_io_aligned(
            pointer,
            2 * DIRECT_IO_ALIGNMENT,
            DIRECT_IO_ALIGNMENT as u64
        ));
        assert!(!direct_io_aligned(
            pointer.wrapping_add(1),
            DIRECT_IO_ALIGNMENT,
            0
        ));
        assert!(!direct_io_aligned(pointer, DIRECT_IO_ALIGNMENT - 1, 0));
        assert!(!direct_io_aligned(pointer, DIRECT_IO_ALIGNMENT, 1));
        assert!(!direct_io_aligned(pointer, 0, 0));
    }

    #[test]
    fn data_handles_route_only_fully_aligned_data_to_direct() {
        let buffered = TestFile::new("buffered-route");
        let direct = TestFile::new("direct-route");
        let files = DataFileHandles::new(buffered.open(), Some(direct.open()));
        let bytes = AlignedBytes([0; 2 * DIRECT_IO_ALIGNMENT]);
        let pointer = bytes.0.as_ptr();

        assert_eq!(
            files.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, true),
            FileIoPath::Direct
        );
        assert_eq!(
            files.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, false),
            FileIoPath::Buffered
        );
        assert_eq!(
            files.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, true),
            FileIoPath::Buffered
        );

        let buffered_only = DataFileHandles::buffered(buffered.open());
        assert_eq!(
            buffered_only.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, true),
            FileIoPath::Buffered
        );

        let required = DataFileHandles::with_direct(buffered.open(), Some(direct.open()));
        assert_eq!(
            required.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, true),
            FileIoPath::Buffered,
            "required mode must preserve the buffered unaligned-I/O path"
        );
        assert_eq!(
            required.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, false),
            FileIoPath::Buffered,
            "metadata remains on the buffered control descriptor"
        );
    }

    #[test]
    fn file_io_statistics_are_shared_and_can_be_disabled() {
        let buffered = TestFile::new("buffered-stats");
        let direct = TestFile::new("direct-stats");
        let files = DataFileHandles::new(buffered.open(), Some(direct.open()));
        let cloned = files.try_clone().unwrap();

        cloned.record(
            FileIoDirection::Read,
            FileIoPath::Direct,
            DIRECT_IO_ALIGNMENT,
        );
        cloned.record(FileIoDirection::Write, FileIoPath::Buffered, 32);
        let stats = files.stats_handle().snapshot();
        assert_eq!(stats.read.direct.operations, 1);
        assert_eq!(stats.read.direct.bytes, DIRECT_IO_ALIGNMENT as u64);
        assert_eq!(stats.write.buffered.operations, 1);
        assert_eq!(stats.write.buffered.bytes, 32);

        cloned.stats_handle().set_activity_counters_enabled(false);
        files.record(
            FileIoDirection::Read,
            FileIoPath::Direct,
            DIRECT_IO_ALIGNMENT,
        );
        files.record(FileIoDirection::Write, FileIoPath::Buffered, 32);
        assert_eq!(files.stats_handle().snapshot(), stats);
    }

    #[test]
    fn data_handles_route_record_data_and_reports_bytes() {
        let buffered = TestFile::new("posix-buffered-data");
        let direct = TestFile::new("posix-direct-data");
        let buffered_file = buffered.open();
        let direct_file = direct.open();
        buffered_file
            .set_len(2 * DIRECT_IO_ALIGNMENT as u64)
            .unwrap();
        direct_file.set_len(2 * DIRECT_IO_ALIGNMENT as u64).unwrap();
        let io = DataFileHandles::new(
            buffered_file.try_clone().unwrap(),
            Some(direct_file.try_clone().unwrap()),
        );
        let record = AlignedBytes([0x5a; 2 * DIRECT_IO_ALIGNMENT]);

        assert_eq!(
            io.write_at(WritePoint::Record, &record.0[..DIRECT_IO_ALIGNMENT], 0,)
                .unwrap(),
            DIRECT_IO_ALIGNMENT
        );
        assert_eq!(
            io.write_at(
                WritePoint::DataSuperblock,
                &record.0[..DIRECT_IO_ALIGNMENT],
                DIRECT_IO_ALIGNMENT as u64,
            )
            .unwrap(),
            DIRECT_IO_ALIGNMENT
        );

        let mut observed = vec![0_u8; DIRECT_IO_ALIGNMENT];
        direct_file.read_at(&mut observed, 0).unwrap();
        assert!(observed.iter().all(|byte| *byte == 0x5a));
        observed.fill(0);
        buffered_file
            .read_at(&mut observed, DIRECT_IO_ALIGNMENT as u64)
            .unwrap();
        assert!(observed.iter().all(|byte| *byte == 0x5a));

        assert_eq!(io.read_at(&mut [], 0).unwrap(), 0);
        assert_eq!(io.write_at(WritePoint::Record, &[], 0).unwrap(), 0);

        assert_eq!(
            io.stats_handle().snapshot(),
            FileIoStats {
                direct_active: true,
                write: FileIoDirectionStats {
                    buffered: CacheIoPathSnapshot {
                        operations: 1,
                        bytes: DIRECT_IO_ALIGNMENT as u64,
                    },
                    direct: CacheIoPathSnapshot {
                        operations: 1,
                        bytes: DIRECT_IO_ALIGNMENT as u64,
                    },
                },
                ..FileIoStats::default()
            }
        );
    }

    #[cfg(any(
        target_os = "macos",
        all(target_os = "linux", target_pointer_width = "64")
    ))]
    #[test]
    fn preallocate_sets_the_exact_file_extent() {
        let file = TestFile::new("preallocate");
        let io = CacheFile::open(file.path()).unwrap();
        let len = 2 * DIRECT_IO_ALIGNMENT as u64;
        io.preallocate(len).unwrap();
        assert_eq!(io.len().unwrap(), len);
        #[cfg(target_os = "macos")]
        assert!(io.file.metadata().unwrap().blocks() * 512 >= len);
    }

    #[cfg(not(any(
        target_os = "macos",
        all(target_os = "linux", target_pointer_width = "64")
    )))]
    #[test]
    fn unsupported_physical_preallocation_fails_closed() {
        let file = TestFile::new("preallocate-unsupported");
        let io = CacheFile::open(file.path()).unwrap();
        let error = io.preallocate(DIRECT_IO_ALIGNMENT as u64).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(io.len().unwrap(), 0);
    }

    #[test]
    fn storage_file_clones_exact_file_and_detects_aliases() {
        let primary = TestFile::new("control-primary");
        let alias = TestFile::new("control-alias");
        let other = TestFile::new("control-other");
        drop(primary.open());
        std::fs::hard_link(primary.path(), alias.path()).unwrap();

        let primary = CacheFile::open(primary.path()).unwrap();
        let alias = CacheFile::open(alias.path()).unwrap();
        let other = CacheFile::open(other.path()).unwrap();
        let cloned = StorageFile::try_clone_mapping_file(&primary).unwrap();

        primary
            .write_at(WritePoint::RecoveryImageHeader, b"image-ok", 0)
            .unwrap();
        let mut observed = [0_u8; 8];
        cloned.read_at(&mut observed, 0).unwrap();
        assert_eq!(&observed, b"image-ok");
        assert!(StorageFile::is_same_file(&primary, &alias).unwrap());
        assert!(!StorageFile::is_same_file(&primary, &other).unwrap());
    }

    #[test]
    fn recovery_temp_creation_never_reopens_an_existing_target() {
        let image = TestFile::new("recovery-create-new");
        let io = CacheFile::create_new_buffered(image.path()).unwrap();
        io.write_at(WritePoint::RecoveryImageMetadata, b"metadata", 0)
            .unwrap();

        let error = CacheFile::create_new_buffered(image.path())
            .err()
            .expect("create_new must reject an existing recovery target");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn one_fault_handle_controls_multiple_recovery_files() {
        use crate::io::file::testing::FaultAction;
        use crate::io::file::testing::FaultEvent;
        use crate::io::file::testing::FaultFile;
        use crate::io::file::testing::FaultHandle;

        let state = TestFile::new("shared-fault-state");
        let image = TestFile::new("shared-fault-image");
        let temp = TestFile::new("shared-fault-temp");
        let faults = FaultHandle::default();
        let state = FaultFile::open_with_handle(state.path(), faults.clone()).unwrap();
        let image = FaultFile::open_with_handle(image.path(), faults.clone()).unwrap();
        let temp = FaultFile::create_new_buffered_with_handle(temp.path(), faults.clone()).unwrap();

        faults.arm(
            FaultEvent::Write(WritePoint::State),
            2,
            FaultAction::Error(5),
        );
        assert_eq!(state.write_at(WritePoint::State, b"running", 0).unwrap(), 7);
        assert_eq!(
            image
                .write_at(WritePoint::State, b"clean", 0)
                .unwrap_err()
                .raw_os_error(),
            Some(5)
        );

        for (point, offset) in [
            (WritePoint::RecoveryImageIndex, 0),
            (WritePoint::RecoveryImageMetadata, 8),
        ] {
            temp.write_at(point, b"12345678", offset).unwrap();
        }
        for point in [
            SyncPoint::StateReset,
            SyncPoint::RunningState,
            SyncPoint::WarmData,
            SyncPoint::RecoveryImage,
            SyncPoint::CleanState,
        ] {
            temp.sync(point, SyncMode::Data).unwrap();
        }
    }

    #[test]
    fn cache_open_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let target = TestFile::new("symlink-target");
        let link = TestFile::new("symlink-link");
        drop(target.open());
        symlink(target.path(), link.path()).unwrap();

        assert!(CacheFile::open(link.path()).is_err());
    }
}

#[cfg(test)]
pub mod testing {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum FaultEvent {
        Read,
        CloneDataHandles,
        Write(WritePoint),
        Sync(SyncPoint),
        Lock,
        Unlock,
    }

    #[derive(Clone, Copy, Debug)]
    pub enum FaultAction {
        Torn { bytes: usize, raw_os_error: i32 },
        Error(i32),
        ErrorAlways(i32),
        KillAfter,
    }

    #[derive(Clone, Copy, Debug)]
    struct FaultSpec {
        event: FaultEvent,
        occurrence: usize,
        seen: usize,
        action: FaultAction,
    }

    #[derive(Default)]
    struct FaultState {
        spec: Option<FaultSpec>,
        events: Vec<FaultEvent>,
    }

    #[derive(Clone, Default)]
    pub struct FaultHandle {
        state: Arc<Mutex<FaultState>>,
    }

    impl FaultHandle {
        pub fn arm(&self, event: FaultEvent, occurrence: usize, action: FaultAction) {
            assert!(occurrence > 0, "fault occurrence is one-based");
            let mut state = self.state.lock().unwrap();
            state.events.clear();
            state.spec = Some(FaultSpec {
                event,
                occurrence,
                seen: 0,
                action,
            });
        }

        pub fn events(&self) -> Vec<FaultEvent> {
            self.state.lock().unwrap().events.clone()
        }

        fn action(&self, event: FaultEvent) -> Option<FaultAction> {
            let mut state = self.state.lock().unwrap();
            state.events.push(event);
            let spec = state.spec.as_mut()?;
            if spec.event != event {
                return None;
            }
            spec.seen += 1;
            if spec.seen < spec.occurrence {
                return None;
            }
            let action = spec.action;
            if !matches!(action, FaultAction::ErrorAlways(_)) {
                state.spec = None;
            }
            Some(action)
        }
    }

    pub struct FaultFile {
        inner: CacheFile,
        handle: FaultHandle,
    }

    impl FaultFile {
        pub fn with_handle(file: CacheFile, handle: FaultHandle) -> Self {
            Self {
                inner: file,
                handle,
            }
        }

        pub fn open(path: &Path) -> io::Result<(Self, FaultHandle)> {
            let handle = FaultHandle::default();
            let io = Self::open_with_handle(path, handle.clone())?;
            Ok((io, handle))
        }

        /// Opens another control file governed by the same fault schedule.
        pub fn open_with_handle(path: &Path, handle: FaultHandle) -> io::Result<Self> {
            Ok(Self {
                inner: CacheFile::open(path)?,
                handle,
            })
        }

        /// Atomically creates a new control file governed by an existing fault
        /// schedule. This is used for unpublished recovery-image temporaries.
        pub fn create_new_buffered_with_handle(
            path: &Path,
            handle: FaultHandle,
        ) -> io::Result<Self> {
            Ok(Self {
                inner: CacheFile::create_new_buffered(path)?,
                handle,
            })
        }
    }

    impl PositionedIo for FaultFile {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            match self.handle.action(FaultEvent::Read) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::Torn { raw_os_error, .. }) => {
                    Err(io::Error::from_raw_os_error(raw_os_error))
                }
                Some(FaultAction::KillAfter) => kill_after(self.inner.read_at(buffer, offset)),
                None => self.inner.read_at(buffer, offset),
            }
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            match self.handle.action(FaultEvent::Write(point)) {
                Some(FaultAction::Torn {
                    bytes,
                    raw_os_error,
                }) => {
                    let limit = bytes.min(buffer.len());
                    if limit != 0 {
                        let _ = self.inner.write_at(point, &buffer[..limit], offset)?;
                    }
                    Err(io::Error::from_raw_os_error(raw_os_error))
                }
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::KillAfter) => {
                    kill_after(self.inner.write_at(point, buffer, offset))
                }
                None => self.inner.write_at(point, buffer, offset),
            }
        }
    }

    #[cfg(unix)]
    impl StorageFile for FaultFile {
        fn try_clone_data_handles(&self) -> io::Result<DataFileHandles> {
            match self.handle.action(FaultEvent::CloneDataHandles) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "torn actions apply only to positioned I/O",
                )),
                Some(FaultAction::KillAfter) => kill_after(self.inner.try_clone_data_handles()),
                None => self.inner.try_clone_data_handles(),
            }
        }

        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }

        fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
            match self.handle.action(FaultEvent::Sync(point)) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "torn actions apply only to positioned I/O",
                )),
                Some(FaultAction::KillAfter) => kill_after(self.inner.sync(point, mode)),
                None => self.inner.sync(point, mode),
            }
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            match self.handle.action(FaultEvent::Lock) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "torn actions apply only to positioned I/O",
                )),
                Some(FaultAction::KillAfter) => kill_after(self.inner.try_lock_exclusive()),
                None => self.inner.try_lock_exclusive(),
            }
        }

        fn unlock(&self) -> io::Result<()> {
            match self.handle.action(FaultEvent::Unlock) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "torn actions apply only to positioned I/O",
                )),
                Some(FaultAction::KillAfter) => kill_after(self.inner.unlock()),
                None => self.inner.unlock(),
            }
        }

        fn try_clone_mapping_file(&self) -> io::Result<File> {
            StorageFile::try_clone_mapping_file(&self.inner)
        }

        fn identity(&self) -> io::Result<FileIdentity> {
            self.inner.identity()
        }
    }

    fn kill_after<T>(result: io::Result<T>) -> io::Result<T> {
        match result {
            Ok(_) => kill_process(),
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    pub fn kill_process() -> ! {
        const SIGKILL: i32 = 9;
        // SAFETY: both functions have no pointer arguments; SIGKILL cannot
        // run user code in the target process.
        if unsafe { kill(getpid(), SIGKILL) } == 0 {
            loop {
                std::thread::park();
            }
        }
        std::process::abort()
    }

    #[cfg(unix)]
    unsafe extern "C" {
        fn getpid() -> i32;
        fn kill(pid: i32, signal: i32) -> i32;
    }
}
