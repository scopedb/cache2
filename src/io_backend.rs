//! Synchronous positioned I/O abstraction used for open, recovery, and the
//! reference runtime path.
//!
//! Persistence points are carried through the trait so tests can fail an exact
//! record, superblock, or barrier operation without changing the cache
//! algorithm.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::runtime_config::IoMode;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub(crate) const DIRECT_IO_ALIGNMENT: usize = 4096;
pub(crate) const MAX_INTERRUPTED_RETRIES: usize = 4;
#[cfg(target_os = "linux")]
const LINUX_EINTR: i32 = 4;
#[cfg(unix)]
const SAFE_CACHE_OPEN_FLAGS: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeIoPath {
    Buffered,
    Direct,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DirectIoStats {
    pub(crate) direct_active: bool,
    pub(crate) direct_operations: u64,
    pub(crate) direct_bytes: u64,
    pub(crate) buffered_operations: u64,
    pub(crate) buffered_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct DirectIoStatsHandle {
    inner: Arc<DirectIoCounters>,
}

struct DirectIoCounters {
    direct_active: AtomicBool,
    statistics_enabled: AtomicBool,
    direct_operations: AtomicU64,
    direct_bytes: AtomicU64,
    buffered_operations: AtomicU64,
    buffered_bytes: AtomicU64,
}

impl DirectIoStatsHandle {
    fn new(direct_active: bool) -> Self {
        Self {
            inner: Arc::new(DirectIoCounters {
                direct_active: AtomicBool::new(direct_active),
                statistics_enabled: AtomicBool::new(true),
                direct_operations: AtomicU64::new(0),
                direct_bytes: AtomicU64::new(0),
                buffered_operations: AtomicU64::new(0),
                buffered_bytes: AtomicU64::new(0),
            }),
        }
    }

    fn record(&self, path: RuntimeIoPath, length: usize) {
        if !self.inner.statistics_enabled.load(Ordering::Relaxed) {
            return;
        }
        let bytes = u64::try_from(length).unwrap_or(u64::MAX);
        match path {
            RuntimeIoPath::Direct => {
                self.inner.direct_operations.fetch_add(1, Ordering::Relaxed);
                self.inner.direct_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            RuntimeIoPath::Buffered => {
                self.inner
                    .buffered_operations
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .buffered_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    fn is_active(&self) -> bool {
        self.inner.direct_active.load(Ordering::Relaxed)
    }

    pub(crate) fn set_statistics_enabled(&self, enabled: bool) {
        self.inner
            .statistics_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> DirectIoStats {
        DirectIoStats {
            direct_active: self.inner.direct_active.load(Ordering::Relaxed),
            direct_operations: self.inner.direct_operations.load(Ordering::Relaxed),
            direct_bytes: self.inner.direct_bytes.load(Ordering::Relaxed),
            buffered_operations: self.inner.buffered_operations.load(Ordering::Relaxed),
            buffered_bytes: self.inner.buffered_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Engine-owned descriptors for one cache file. `buffered` is duplicated from
/// the descriptor that owns flock, so retaining this set also retains the
/// cache lock if an issued write or flush cannot be fenced. `direct`, when present,
/// is a separate O_DIRECT open used only for aligned runtime data requests.
pub(crate) struct RuntimeFileSet {
    buffered: File,
    direct: Option<File>,
    direct_required: bool,
    stats: DirectIoStatsHandle,
}

impl RuntimeFileSet {
    #[cfg(test)]
    pub(crate) fn buffered(file: File) -> Self {
        Self {
            buffered: file,
            direct: None,
            direct_required: false,
            stats: DirectIoStatsHandle::new(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(buffered: File, direct: Option<File>) -> Self {
        Self::with_mode(buffered, direct, IoMode::Auto)
    }

    fn with_mode(buffered: File, direct: Option<File>, mode: IoMode) -> Self {
        let direct_active = direct.is_some();
        Self {
            buffered,
            direct,
            direct_required: mode == IoMode::Direct,
            stats: DirectIoStatsHandle::new(direct_active),
        }
    }

    pub(crate) fn select_path(
        &self,
        buffer: *const u8,
        length: usize,
        offset: u64,
        allow_direct: bool,
    ) -> RuntimeIoPath {
        if !allow_direct || self.direct.is_none() || !self.stats.is_active() {
            return RuntimeIoPath::Buffered;
        }
        // Never issue malformed O_DIRECT. Unaligned record fragments and an
        // unaligned remainder after a positive short completion use the
        // buffered compatibility path in every mode. Direct mode means the
        // direct descriptor must exist and aligned direct errors do not fall
        // back; it does not make 32-byte-aligned records unreadable.
        if direct_io_aligned(buffer, length, offset) {
            RuntimeIoPath::Direct
        } else {
            RuntimeIoPath::Buffered
        }
    }

    pub(crate) fn record(&self, path: RuntimeIoPath, length: usize) {
        self.stats.record(path, length);
    }

    pub(crate) fn should_fallback(&self, path: RuntimeIoPath, error: &io::Error) -> bool {
        let should_fallback =
            path == RuntimeIoPath::Direct && !self.direct_required && direct_io_unavailable(error);
        if should_fallback {
            self.stats
                .inner
                .direct_active
                .store(false, Ordering::Relaxed);
        }
        should_fallback
    }

    #[cfg(any(
        test,
        all(
            feature = "io-uring",
            target_os = "linux",
            any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            )
        )
    ))]
    pub(crate) fn stats_handle(&self) -> DirectIoStatsHandle {
        self.stats.clone()
    }

    pub(crate) fn set_statistics_enabled(&self, enabled: bool) {
        self.stats.set_statistics_enabled(enabled);
    }

    #[cfg_attr(
        not(all(
            feature = "io-uring",
            target_os = "linux",
            any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            )
        )),
        allow(dead_code)
    )]
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            buffered: self.buffered.try_clone()?,
            direct: self.direct.as_ref().map(File::try_clone).transpose()?,
            direct_required: self.direct_required,
            stats: self.stats.clone(),
        })
    }

    #[cfg(unix)]
    pub(crate) fn file_for(&self, path: RuntimeIoPath) -> &File {
        match path {
            RuntimeIoPath::Buffered => &self.buffered,
            RuntimeIoPath::Direct => self
                .direct
                .as_ref()
                .expect("direct path requires a direct descriptor"),
        }
    }
}

pub(crate) fn direct_io_aligned(buffer: *const u8, length: usize, offset: u64) -> bool {
    !buffer.is_null()
        && (buffer as usize).is_multiple_of(DIRECT_IO_ALIGNMENT)
        && length != 0
        && length.is_multiple_of(DIRECT_IO_ALIGNMENT)
        && offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WritePoint {
    Record,
    DataSuperblock,
    State,
    RecoveryImageHeader,
    RecoveryImageIndex,
    RecoveryImageMetadata,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncPoint {
    FormatTruncate,
    FormatData,
    StateReset,
    RunningState,
    ExplicitFlush,
    WarmData,
    RecoveryImage,
    CleanState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncMode {
    Data,
    All,
}

pub(crate) trait IoBackend: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.set_len(len)
    }
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
        // The default keeps fault-injecting and test backends source-compatible.
        // Production runtime files override this method with positioned kernel
        // I/O that can initialize the destination directly.
        // SAFETY: upheld by the caller; zeroing establishes initialized bytes
        // before constructing the mutable slice required by `read_at`.
        unsafe {
            buffer.write_bytes(0, length);
            self.read_at(std::slice::from_raw_parts_mut(buffer, length), offset)
        }
    }
    fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize>;
    fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()>;
    fn try_lock_exclusive(&self) -> io::Result<()>;
    fn unlock(&self) -> io::Result<()>;
    fn direct_io_stats(&self) -> DirectIoStats {
        DirectIoStats::default()
    }
}

/// Buffered descriptor access needed by recovery-control code.
///
/// Recovery uses the [`IoBackend`] methods for injectable positioned I/O and
/// durability barriers, then clones this exact validated descriptor for an
/// immutable private mapping. File identity is intentionally descriptor-based
/// so callers never need to reopen a path between validation and `mmap`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) trait ControlIoBackend: IoBackend {
    fn try_clone_control_file(&self) -> io::Result<File>;

    fn control_file_identity(&self) -> io::Result<ControlFileIdentity>;

    fn is_same_file(&self, other: &dyn ControlIoBackend) -> io::Result<bool> {
        Ok(self.control_file_identity()? == other.control_file_identity()?)
    }
}

/// Stable identity of one open regular file within the running system.
///
/// The fields remain opaque: recovery code only needs equality to reject
/// aliased data, state, and image descriptors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ControlFileIdentity {
    device: u64,
    inode: u64,
}

pub(crate) struct FileBackend {
    /// Buffered control descriptor and flock owner.
    file: File,
    /// Separate Linux O_DIRECT descriptor for aligned runtime data I/O.
    direct: Option<File>,
    direct_mode: IoMode,
}

impl FileBackend {
    #[cfg(test)]
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_io_mode(path, IoMode::Buffered)
    }

    pub(crate) fn open_with_io_mode(path: &Path, mode: IoMode) -> io::Result<Self> {
        Self::open_with_io_mode_and_create(path, mode, true)
    }

    pub(crate) fn open_existing_with_io_mode(path: &Path, mode: IoMode) -> io::Result<Self> {
        Self::open_with_io_mode_and_create(path, mode, false)
    }

    /// Atomically creates a new buffered control file without following a
    /// symbolic link or opening an existing recovery-image target.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn create_new_buffered(path: &Path) -> io::Result<Self> {
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
        Ok(Self {
            file,
            direct: None,
            direct_mode: IoMode::Buffered,
        })
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
            return Ok(Self {
                file,
                direct: None,
                direct_mode: mode,
            });
        }

        #[cfg(target_os = "linux")]
        let direct = open_direct(path, &file);
        #[cfg(not(target_os = "linux"))]
        let direct = Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "O_DIRECT is supported only on Linux",
        ));
        Self::finish_direct_open(file, mode, direct)
    }

    fn finish_direct_open(file: File, mode: IoMode, direct: io::Result<File>) -> io::Result<Self> {
        match direct {
            Ok(direct) => Ok(Self {
                file,
                direct: Some(direct),
                direct_mode: mode,
            }),
            Err(error) if mode == IoMode::Auto && direct_io_unavailable(&error) => Ok(Self {
                file,
                direct: None,
                direct_mode: mode,
            }),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_clone_runtime_files(&self) -> io::Result<RuntimeFileSet> {
        let buffered = self.file.try_clone()?;
        let direct = self.direct.as_ref().map(File::try_clone).transpose()?;
        Ok(RuntimeFileSet::with_mode(
            buffered,
            direct,
            self.direct_mode,
        ))
    }

    pub(crate) const fn direct_active(&self) -> bool {
        self.direct.is_some()
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
impl ControlIoBackend for FileBackend {
    fn try_clone_control_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    fn control_file_identity(&self) -> io::Result<ControlFileIdentity> {
        let metadata = self.file.metadata()?;
        Ok(ControlFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

/// Positioned-I/O backend used by the synchronous reference engine. Control,
/// metadata, locking, and recovery continue to use `FileBackend`; this backend
/// routes only aligned runtime reads and record writes to the direct fd.
pub(crate) struct RuntimeFileBackend {
    files: RuntimeFileSet,
}

impl RuntimeFileBackend {
    pub(crate) fn new(files: RuntimeFileSet) -> Self {
        Self { files }
    }
}

#[cfg(unix)]
impl IoBackend for FileBackend {
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

    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.read_at(buffer, offset)
    }

    fn write_at(&self, _point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
        self.file.write_at(buffer, offset)
    }

    fn sync(&self, _point: SyncPoint, mode: SyncMode) -> io::Result<()> {
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

    fn direct_io_stats(&self) -> DirectIoStats {
        DirectIoStats {
            direct_active: self.direct_active(),
            ..DirectIoStats::default()
        }
    }
}

#[cfg(unix)]
impl IoBackend for RuntimeFileBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(self
            .files
            .file_for(RuntimeIoPath::Buffered)
            .metadata()?
            .len())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.files.file_for(RuntimeIoPath::Buffered).set_len(len)
    }

    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        let path = self
            .files
            .select_path(buffer.as_ptr(), buffer.len(), offset, true);
        match self.files.file_for(path).read_at(buffer, offset) {
            Err(error) if self.files.should_fallback(path, &error) => {
                let result = self
                    .files
                    .file_for(RuntimeIoPath::Buffered)
                    .read_at(buffer, offset);
                if let Ok(bytes) = result
                    && bytes != 0
                {
                    self.files.record(RuntimeIoPath::Buffered, bytes);
                }
                result
            }
            result => {
                if let Ok(bytes) = result
                    && bytes != 0
                {
                    self.files.record(path, bytes);
                }
                result
            }
        }
    }

    unsafe fn read_at_uninit(
        &self,
        buffer: *mut u8,
        length: usize,
        offset: u64,
    ) -> io::Result<usize> {
        let path = self
            .files
            .select_path(buffer.cast_const(), length, offset, true);
        // SAFETY: the caller supplies a writable destination for `length`
        // bytes; the selected descriptor is held by `self` for this call.
        let result =
            unsafe { read_file_at_uninit(self.files.file_for(path), buffer, length, offset) };
        match result {
            Err(error) if self.files.should_fallback(path, &error) => {
                // SAFETY: the failed direct call did not report initialized
                // bytes and the same destination remains valid for fallback.
                let result = unsafe {
                    read_file_at_uninit(
                        self.files.file_for(RuntimeIoPath::Buffered),
                        buffer,
                        length,
                        offset,
                    )
                };
                if let Ok(bytes) = result
                    && bytes != 0
                {
                    self.files.record(RuntimeIoPath::Buffered, bytes);
                }
                result
            }
            result => {
                if let Ok(bytes) = result
                    && bytes != 0
                {
                    self.files.record(path, bytes);
                }
                result
            }
        }
    }

    fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
        let path = self.files.select_path(
            buffer.as_ptr(),
            buffer.len(),
            offset,
            point == WritePoint::Record,
        );
        match self.files.file_for(path).write_at(buffer, offset) {
            Err(error) if self.files.should_fallback(path, &error) => {
                let result = self
                    .files
                    .file_for(RuntimeIoPath::Buffered)
                    .write_at(buffer, offset);
                if let Ok(bytes) = result
                    && bytes != 0
                {
                    self.files.record(RuntimeIoPath::Buffered, bytes);
                }
                result
            }
            result => {
                if let Ok(bytes) = result
                    && bytes != 0
                {
                    self.files.record(path, bytes);
                }
                result
            }
        }
    }

    fn sync(&self, _point: SyncPoint, mode: SyncMode) -> io::Result<()> {
        let file = self.files.file_for(RuntimeIoPath::Buffered);
        match mode {
            SyncMode::Data => file.sync_data(),
            SyncMode::All => file.sync_all(),
        }
    }

    fn try_lock_exclusive(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "runtime file backend does not own cache locking",
        ))
    }

    fn unlock(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "runtime file backend does not own cache locking",
        ))
    }

    fn direct_io_stats(&self) -> DirectIoStats {
        self.files.stats.snapshot()
    }
}

fn direct_io_unavailable(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied
    ) {
        return true;
    }
    #[cfg(target_os = "linux")]
    {
        error.raw_os_error().is_some_and(|code| {
            code == libc::EINVAL || code == libc::ENOSYS || code == libc::EOPNOTSUPP
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
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

pub(crate) fn read_exact_at(
    backend: &dyn IoBackend,
    buffer: &mut [u8],
    offset: u64,
) -> io::Result<()> {
    read_exact_at_with_progress(backend, buffer, offset).0
}

pub(crate) fn read_at_bounded(
    backend: &dyn IoBackend,
    buffer: &mut [u8],
    offset: u64,
) -> io::Result<usize> {
    retry_interrupted(|| backend.read_at(buffer, offset))
}

pub(crate) fn read_exact_at_with_progress(
    backend: &dyn IoBackend,
    mut buffer: &mut [u8],
    mut offset: u64,
) -> (io::Result<()>, usize) {
    let mut transferred = 0_usize;
    while !buffer.is_empty() {
        let read = match read_at_bounded(backend, buffer, offset) {
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

pub(crate) fn read_exact_at_uninit_with_progress(
    backend: &dyn IoBackend,
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
            unsafe { backend.read_at_uninit(buffer.add(transferred), remaining, offset) }
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

pub(crate) fn write_all_at(
    backend: &dyn IoBackend,
    point: WritePoint,
    buffer: &[u8],
    offset: u64,
) -> io::Result<()> {
    write_all_at_with_progress(backend, point, buffer, offset).0
}

pub(crate) fn write_all_at_with_progress(
    backend: &dyn IoBackend,
    point: WritePoint,
    mut buffer: &[u8],
    mut offset: u64,
) -> (io::Result<()>, usize) {
    let mut transferred = 0_usize;
    while !buffer.is_empty() {
        let written = match retry_interrupted(|| backend.write_at(point, buffer, offset)) {
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

fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(label: &str) -> Self {
            let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "cache-rs-{label}-{}-{nonce}.cache",
                std::process::id()
            )))
        }

        fn open(&self) -> File {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.0)
                .unwrap()
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[repr(align(4096))]
    struct AlignedBytes([u8; 2 * DIRECT_IO_ALIGNMENT]);

    #[derive(Default)]
    struct InterruptedBackend {
        calls: AtomicUsize,
    }

    impl IoBackend for InterruptedBackend {
        fn len(&self) -> io::Result<u64> {
            Ok(0)
        }

        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn read_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "interrupted read",
            ))
        }

        fn write_at(&self, _point: WritePoint, _buffer: &[u8], _offset: u64) -> io::Result<usize> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "interrupted write",
            ))
        }

        fn sync(&self, _point: SyncPoint, _mode: SyncMode) -> io::Result<()> {
            Ok(())
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            Ok(())
        }

        fn unlock(&self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn exact_io_stops_after_the_interrupted_retry_budget() {
        let mut initialized = [0_u8; 1];
        let backend = InterruptedBackend::default();
        let (result, transferred) = read_exact_at_with_progress(&backend, &mut initialized, 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(transferred, 0);
        assert_eq!(
            backend.calls.load(Ordering::Relaxed),
            MAX_INTERRUPTED_RETRIES + 1
        );

        let mut uninitialized = std::mem::MaybeUninit::<u8>::uninit();
        let backend = InterruptedBackend::default();
        let (result, transferred) =
            read_exact_at_uninit_with_progress(&backend, uninitialized.as_mut_ptr(), 1, 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(transferred, 0);
        assert_eq!(
            backend.calls.load(Ordering::Relaxed),
            MAX_INTERRUPTED_RETRIES + 1
        );

        let backend = InterruptedBackend::default();
        let (result, transferred) =
            write_all_at_with_progress(&backend, WritePoint::Record, &[1], 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(transferred, 0);
        assert_eq!(
            backend.calls.load(Ordering::Relaxed),
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
    fn runtime_files_route_only_fully_aligned_data_to_direct() {
        let buffered = TestFile::new("buffered-route");
        let direct = TestFile::new("direct-route");
        let files = RuntimeFileSet::new(buffered.open(), Some(direct.open()));
        let bytes = AlignedBytes([0; 2 * DIRECT_IO_ALIGNMENT]);
        let pointer = bytes.0.as_ptr();

        assert_eq!(
            files.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, true),
            RuntimeIoPath::Direct
        );
        assert_eq!(
            files.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, false),
            RuntimeIoPath::Buffered
        );
        assert_eq!(
            files.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, true),
            RuntimeIoPath::Buffered
        );
        let cloned = files.try_clone().unwrap();
        cloned.record(RuntimeIoPath::Direct, DIRECT_IO_ALIGNMENT);
        assert_eq!(files.stats_handle().snapshot().direct_operations, 1);
        cloned.set_statistics_enabled(false);
        files.record(RuntimeIoPath::Direct, DIRECT_IO_ALIGNMENT);
        assert_eq!(files.stats_handle().snapshot().direct_operations, 1);

        let buffered_only = RuntimeFileSet::buffered(buffered.open());
        assert_eq!(
            buffered_only.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, true),
            RuntimeIoPath::Buffered
        );

        let required =
            RuntimeFileSet::with_mode(buffered.open(), Some(direct.open()), IoMode::Direct);
        assert_eq!(
            required.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, true),
            RuntimeIoPath::Buffered,
            "required mode must preserve the buffered unaligned-I/O path"
        );
        assert_eq!(
            required.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, false),
            RuntimeIoPath::Buffered,
            "metadata remains on the buffered control descriptor"
        );
    }

    #[test]
    fn sync_runtime_backend_routes_record_data_and_reports_bytes() {
        let buffered = TestFile::new("sync-buffered-data");
        let direct = TestFile::new("sync-direct-data");
        let buffered_file = buffered.open();
        let direct_file = direct.open();
        buffered_file
            .set_len(2 * DIRECT_IO_ALIGNMENT as u64)
            .unwrap();
        direct_file.set_len(2 * DIRECT_IO_ALIGNMENT as u64).unwrap();
        let backend = RuntimeFileBackend::new(RuntimeFileSet::new(
            buffered_file.try_clone().unwrap(),
            Some(direct_file.try_clone().unwrap()),
        ));
        let record = AlignedBytes([0x5a; 2 * DIRECT_IO_ALIGNMENT]);

        assert_eq!(
            backend
                .write_at(WritePoint::Record, &record.0[..DIRECT_IO_ALIGNMENT], 0,)
                .unwrap(),
            DIRECT_IO_ALIGNMENT
        );
        assert_eq!(
            backend
                .write_at(
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

        assert_eq!(backend.read_at(&mut [], 0).unwrap(), 0);
        assert_eq!(backend.write_at(WritePoint::Record, &[], 0).unwrap(), 0);

        assert_eq!(
            backend.direct_io_stats(),
            DirectIoStats {
                direct_active: true,
                direct_operations: 1,
                direct_bytes: DIRECT_IO_ALIGNMENT as u64,
                buffered_operations: 1,
                buffered_bytes: DIRECT_IO_ALIGNMENT as u64,
            }
        );
    }

    #[test]
    fn auto_falls_back_but_required_reports_unsupported_direct_open() {
        let auto_file = TestFile::new("direct-auto");
        let auto = FileBackend::finish_direct_open(
            auto_file.open(),
            IoMode::Auto,
            Err(io::Error::from_raw_os_error(22)),
        )
        .unwrap();
        assert!(!auto.direct_active());
        let policy_denied = FileBackend::finish_direct_open(
            auto_file.open(),
            IoMode::Auto,
            Err(io::Error::from_raw_os_error(13)),
        )
        .unwrap();
        assert!(!policy_denied.direct_active());

        let required_file = TestFile::new("direct-required");
        let error = FileBackend::finish_direct_open(
            required_file.open(),
            IoMode::Direct,
            Err(io::Error::from_raw_os_error(22)),
        )
        .err()
        .expect("required direct mode must report the open error");
        assert_eq!(error.raw_os_error(), Some(22));

        let buffered = TestFile::new("runtime-fallback-buffered");
        let direct = TestFile::new("runtime-fallback-direct");
        let unavailable = io::Error::from_raw_os_error(22);
        let auto_files =
            RuntimeFileSet::with_mode(buffered.open(), Some(direct.open()), IoMode::Auto);
        assert!(auto_files.should_fallback(RuntimeIoPath::Direct, &unavailable));
        let required_files =
            RuntimeFileSet::with_mode(buffered.open(), Some(direct.open()), IoMode::Direct);
        assert!(!required_files.should_fallback(RuntimeIoPath::Direct, &unavailable));
    }

    #[cfg(any(
        target_os = "macos",
        all(target_os = "linux", target_pointer_width = "64")
    ))]
    #[test]
    fn preallocate_sets_the_exact_file_extent() {
        let file = TestFile::new("preallocate");
        let backend = FileBackend::open(&file.0).unwrap();
        let len = 2 * DIRECT_IO_ALIGNMENT as u64;
        backend.preallocate(len).unwrap();
        assert_eq!(backend.len().unwrap(), len);
        #[cfg(target_os = "macos")]
        assert!(backend.file.metadata().unwrap().blocks() * 512 >= len);
    }

    #[cfg(not(any(
        target_os = "macos",
        all(target_os = "linux", target_pointer_width = "64")
    )))]
    #[test]
    fn unsupported_physical_preallocation_fails_closed() {
        let file = TestFile::new("preallocate-unsupported");
        let backend = FileBackend::open(&file.0).unwrap();
        let error = backend.preallocate(DIRECT_IO_ALIGNMENT as u64).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(backend.len().unwrap(), 0);
    }

    #[test]
    fn recovery_control_backend_clones_exact_file_and_detects_aliases() {
        let primary = TestFile::new("control-primary");
        let alias = TestFile::new("control-alias");
        let other = TestFile::new("control-other");
        drop(primary.open());
        std::fs::hard_link(&primary.0, &alias.0).unwrap();

        let primary = FileBackend::open(&primary.0).unwrap();
        let alias = FileBackend::open(&alias.0).unwrap();
        let other = FileBackend::open(&other.0).unwrap();
        let cloned = ControlIoBackend::try_clone_control_file(&primary).unwrap();

        primary
            .write_at(WritePoint::RecoveryImageHeader, b"image-ok", 0)
            .unwrap();
        let mut observed = [0_u8; 8];
        cloned.read_at(&mut observed, 0).unwrap();
        assert_eq!(&observed, b"image-ok");
        assert!(ControlIoBackend::is_same_file(&primary, &alias).unwrap());
        assert!(!ControlIoBackend::is_same_file(&primary, &other).unwrap());
    }

    #[test]
    fn recovery_temp_creation_never_reopens_an_existing_target() {
        let image = TestFile::new("recovery-create-new");
        let backend = FileBackend::create_new_buffered(&image.0).unwrap();
        backend
            .write_at(WritePoint::RecoveryImageMetadata, b"metadata", 0)
            .unwrap();

        let error = FileBackend::create_new_buffered(&image.0)
            .err()
            .expect("create_new must reject an existing recovery target");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn one_fault_handle_controls_multiple_recovery_files() {
        use super::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};

        let state = TestFile::new("shared-fault-state");
        let image = TestFile::new("shared-fault-image");
        let temp = TestFile::new("shared-fault-temp");
        let faults = FaultHandle::default();
        let state = FaultBackend::open_with_handle(&state.0, faults.clone()).unwrap();
        let image = FaultBackend::open_with_handle(&image.0, faults.clone()).unwrap();
        let temp = FaultBackend::create_new_buffered_with_handle(&temp.0, faults.clone()).unwrap();

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
        let target = TestFile::new("symlink-target");
        let link = TestFile::new("symlink-link");
        drop(target.open());
        std::os::unix::fs::symlink(&target.0, &link.0).unwrap();

        assert!(FileBackend::open(&link.0).is_err());
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum FaultEvent {
        Read,
        Write(WritePoint),
        Sync(SyncPoint),
        Lock,
        Unlock,
    }

    #[derive(Clone, Copy, Debug)]
    pub(crate) enum FaultAction {
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
    pub(crate) struct FaultHandle {
        state: Arc<Mutex<FaultState>>,
    }

    impl FaultHandle {
        pub(crate) fn arm(&self, event: FaultEvent, occurrence: usize, action: FaultAction) {
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

        pub(crate) fn events(&self) -> Vec<FaultEvent> {
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

    pub(crate) struct FaultBackend {
        inner: FileBackend,
        handle: FaultHandle,
    }

    impl FaultBackend {
        pub(crate) fn open(path: &Path) -> io::Result<(Self, FaultHandle)> {
            let handle = FaultHandle::default();
            let backend = Self::open_with_handle(path, handle.clone())?;
            Ok((backend, handle))
        }

        /// Opens another control file governed by the same fault schedule.
        pub(crate) fn open_with_handle(path: &Path, handle: FaultHandle) -> io::Result<Self> {
            Ok(Self {
                inner: FileBackend::open(path)?,
                handle,
            })
        }

        /// Opens an existing control file without creating a missing path.
        pub(crate) fn open_existing_with_handle(
            path: &Path,
            handle: FaultHandle,
        ) -> io::Result<Self> {
            Ok(Self {
                inner: FileBackend::open_existing_with_io_mode(path, IoMode::Buffered)?,
                handle,
            })
        }

        /// Atomically creates a new control file governed by an existing fault
        /// schedule. This is used for unpublished recovery-image temporaries.
        pub(crate) fn create_new_buffered_with_handle(
            path: &Path,
            handle: FaultHandle,
        ) -> io::Result<Self> {
            Ok(Self {
                inner: FileBackend::create_new_buffered(path)?,
                handle,
            })
        }
    }

    impl IoBackend for FaultBackend {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }

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
    }

    #[cfg(unix)]
    impl ControlIoBackend for FaultBackend {
        fn try_clone_control_file(&self) -> io::Result<File> {
            ControlIoBackend::try_clone_control_file(&self.inner)
        }

        fn control_file_identity(&self) -> io::Result<ControlFileIdentity> {
            self.inner.control_file_identity()
        }
    }

    fn kill_after<T>(result: io::Result<T>) -> io::Result<T> {
        match result {
            Ok(_) => kill_process(),
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    pub(crate) fn kill_process() -> ! {
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
