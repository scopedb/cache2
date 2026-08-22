//! Synchronous positioned I/O abstraction used for open, recovery, and the
//! reference runtime path.
//!
//! Persistence points are carried through the trait so tests can fail an exact
//! record, region-header, superblock, or barrier operation without changing the
//! cache algorithm.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub(crate) const DIRECT_IO_ALIGNMENT: usize = 4096;
#[cfg(target_os = "linux")]
const LINUX_EINTR: i32 = 4;
#[cfg(target_os = "linux")]
const SAFE_CACHE_OPEN_FLAGS: i32 = 0o400_000 | 0o4_000; // O_NOFOLLOW | O_NONBLOCK
#[cfg(any(target_os = "macos", target_os = "ios"))]
const SAFE_CACHE_OPEN_FLAGS: i32 = 0x0100 | 0x0004; // O_NOFOLLOW | O_NONBLOCK
#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "macos", target_os = "ios"))
))]
const SAFE_CACHE_OPEN_FLAGS: i32 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectIoMode {
    Buffered,
    Auto,
    Required,
}

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
                direct_operations: AtomicU64::new(0),
                direct_bytes: AtomicU64::new(0),
                buffered_operations: AtomicU64::new(0),
                buffered_bytes: AtomicU64::new(0),
            }),
        }
    }

    fn record(&self, path: RuntimeIoPath, length: usize) {
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
/// cache lock if an issued mutation cannot be fenced. `direct`, when present,
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
        Self::with_mode(buffered, direct, DirectIoMode::Auto)
    }

    fn with_mode(buffered: File, direct: Option<File>, mode: DirectIoMode) -> Self {
        let direct_active = direct.is_some();
        Self {
            buffered,
            direct,
            direct_required: mode == DirectIoMode::Required,
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
        // Never issue malformed O_DIRECT. Unaligned Format V1 records and an
        // unaligned remainder after a positive short completion use the
        // buffered compatibility path in every mode. Required mode means the
        // direct descriptor must exist and aligned direct errors do not fall
        // back; it does not make legacy 32-byte records unreadable.
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
    pub(crate) fn stats_handle(&self) -> DirectIoStatsHandle {
        self.stats.clone()
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
        && buffer as usize % DIRECT_IO_ALIGNMENT == 0
        && length != 0
        && length % DIRECT_IO_ALIGNMENT == 0
        && offset % DIRECT_IO_ALIGNMENT as u64 == 0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WritePoint {
    Record,
    RegionHeader,
    Superblock,
    HybridManifest,
    HybridJournal,
    CheckpointDirectory,
    CheckpointPayload,
    CheckpointHeader,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncPoint {
    FormatDirty,
    FormatTruncate,
    FormatRegions,
    FormatClean,
    DirtyMarker,
    RegionRotation,
    ClearBarrier,
    CheckpointPayload,
    CheckpointHeader,
    CheckpointDirectory,
    CheckpointData,
    CheckpointClean,
    HybridManifestDirty,
    HybridJournal,
    HybridManifestClean,
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
    fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize>;
    fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()>;
    fn try_lock_exclusive(&self) -> io::Result<()>;
    fn unlock(&self) -> io::Result<()>;
    fn direct_io_stats(&self) -> DirectIoStats {
        DirectIoStats::default()
    }
}

pub(crate) struct FileBackend {
    /// Buffered control descriptor and flock owner.
    file: File,
    /// Separate Linux O_DIRECT descriptor for aligned runtime data I/O.
    direct: Option<File>,
    direct_mode: DirectIoMode,
}

impl FileBackend {
    #[cfg(test)]
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_io_mode(path, DirectIoMode::Buffered)
    }

    pub(crate) fn open_with_io_mode(path: &Path, mode: DirectIoMode) -> io::Result<Self> {
        Self::open_with_io_mode_and_create(path, mode, true)
    }

    pub(crate) fn open_existing_with_io_mode(path: &Path, mode: DirectIoMode) -> io::Result<Self> {
        Self::open_with_io_mode_and_create(path, mode, false)
    }

    fn open_with_io_mode_and_create(
        path: &Path,
        mode: DirectIoMode,
        create: bool,
    ) -> io::Result<Self> {
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
        if mode == DirectIoMode::Buffered {
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

    fn finish_direct_open(
        file: File,
        mode: DirectIoMode,
        direct: io::Result<File>,
    ) -> io::Result<Self> {
        match direct {
            Ok(direct) => Ok(Self {
                file,
                direct: Some(direct),
                direct_mode: mode,
            }),
            Err(error) if mode == DirectIoMode::Auto && direct_io_unavailable(&error) => Ok(Self {
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
        #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
        let linux_len = i64::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocation length exceeds Linux off_t",
            )
        })?;
        // Preserve set_len's exact truncate/extend behavior before allocating
        // physical blocks for the final extent.
        self.file.set_len(len)?;
        #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
        {
            let error = loop {
                // SAFETY: `file` owns a valid regular-file descriptor and
                // both offsets are representable non-negative off_t values.
                let error = unsafe { posix_fallocate(self.file.as_raw_fd(), 0, linux_len) };
                if error != LINUX_EINTR {
                    break error;
                }
            };
            if error == 0 {
                return Ok(());
            }
            let error = io::Error::from_raw_os_error(error);
            if direct_io_unavailable(&error) {
                return Ok(());
            }
            Err(error)
        }
        #[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
        {
            let _ = self.direct_mode;
            Ok(())
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
                if let Ok(bytes) = result {
                    if bytes != 0 {
                        self.files.record(RuntimeIoPath::Buffered, bytes);
                    }
                }
                result
            }
            result => {
                if let Ok(bytes) = result {
                    if bytes != 0 {
                        self.files.record(path, bytes);
                    }
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
                if let Ok(bytes) = result {
                    if bytes != 0 {
                        self.files.record(RuntimeIoPath::Buffered, bytes);
                    }
                }
                result
            }
            result => {
                if let Ok(bytes) = result {
                    if bytes != 0 {
                        self.files.record(path, bytes);
                    }
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

pub(crate) fn read_exact_at_with_progress(
    backend: &dyn IoBackend,
    mut buffer: &mut [u8],
    mut offset: u64,
) -> (io::Result<()>, usize) {
    let mut transferred = 0_usize;
    while !buffer.is_empty() {
        let read = match backend.read_at(buffer, offset) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
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
        let written = match backend.write_at(point, buffer, offset) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
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
    use std::sync::atomic::{AtomicU64, Ordering};

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

        let buffered_only = RuntimeFileSet::buffered(buffered.open());
        assert_eq!(
            buffered_only.select_path(pointer, DIRECT_IO_ALIGNMENT, 0, true),
            RuntimeIoPath::Buffered
        );

        let required =
            RuntimeFileSet::with_mode(buffered.open(), Some(direct.open()), DirectIoMode::Required);
        assert_eq!(
            required.select_path(pointer, DIRECT_IO_ALIGNMENT - 1, 0, true),
            RuntimeIoPath::Buffered,
            "required mode must preserve the buffered Format V1 compatibility path"
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
                    WritePoint::RegionHeader,
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
            DirectIoMode::Auto,
            Err(io::Error::from_raw_os_error(22)),
        )
        .unwrap();
        assert!(!auto.direct_active());
        let policy_denied = FileBackend::finish_direct_open(
            auto_file.open(),
            DirectIoMode::Auto,
            Err(io::Error::from_raw_os_error(13)),
        )
        .unwrap();
        assert!(!policy_denied.direct_active());

        let required_file = TestFile::new("direct-required");
        let error = FileBackend::finish_direct_open(
            required_file.open(),
            DirectIoMode::Required,
            Err(io::Error::from_raw_os_error(22)),
        )
        .err()
        .expect("required direct mode must report the open error");
        assert_eq!(error.raw_os_error(), Some(22));

        let buffered = TestFile::new("runtime-fallback-buffered");
        let direct = TestFile::new("runtime-fallback-direct");
        let unavailable = io::Error::from_raw_os_error(22);
        let auto_files =
            RuntimeFileSet::with_mode(buffered.open(), Some(direct.open()), DirectIoMode::Auto);
        assert!(auto_files.should_fallback(RuntimeIoPath::Direct, &unavailable));
        let required_files =
            RuntimeFileSet::with_mode(buffered.open(), Some(direct.open()), DirectIoMode::Required);
        assert!(!required_files.should_fallback(RuntimeIoPath::Direct, &unavailable));
    }

    #[test]
    fn preallocate_sets_the_exact_file_extent() {
        let file = TestFile::new("preallocate");
        let backend = FileBackend::open(&file.0).unwrap();
        backend.preallocate(2 * DIRECT_IO_ALIGNMENT as u64).unwrap();
        assert_eq!(backend.len().unwrap(), 2 * DIRECT_IO_ALIGNMENT as u64);
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
        Short(usize),
        Torn { bytes: usize, raw_os_error: i32 },
        Error(i32),
        ErrorAlways(i32),
        KillBefore,
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
            Ok((
                Self {
                    inner: FileBackend::open(path)?,
                    handle: handle.clone(),
                },
                handle,
            ))
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
                Some(FaultAction::Short(bytes)) => {
                    let limit = bytes.min(buffer.len());
                    self.inner.read_at(&mut buffer[..limit], offset)
                }
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::KillBefore) => kill_self(),
                Some(FaultAction::KillAfter) => {
                    let result = self.inner.read_at(buffer, offset);
                    kill_self_after(result)
                }
                Some(FaultAction::Torn { raw_os_error, .. }) => {
                    Err(io::Error::from_raw_os_error(raw_os_error))
                }
                None => self.inner.read_at(buffer, offset),
            }
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            match self.handle.action(FaultEvent::Write(point)) {
                Some(FaultAction::Short(bytes)) => {
                    let limit = bytes.min(buffer.len());
                    self.inner.write_at(point, &buffer[..limit], offset)
                }
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
                Some(FaultAction::KillBefore) => kill_self(),
                Some(FaultAction::KillAfter) => {
                    let result = self.inner.write_at(point, buffer, offset);
                    kill_self_after(result)
                }
                None => self.inner.write_at(point, buffer, offset),
            }
        }

        fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
            match self.handle.action(FaultEvent::Sync(point)) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::KillBefore) => kill_self(),
                Some(FaultAction::KillAfter) => {
                    let result = self.inner.sync(point, mode);
                    kill_self_after(result)
                }
                Some(FaultAction::Short(_) | FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "short/torn actions apply only to positioned I/O",
                )),
                None => self.inner.sync(point, mode),
            }
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            match self.handle.action(FaultEvent::Lock) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::KillBefore) => kill_self(),
                Some(FaultAction::KillAfter) => {
                    let result = self.inner.try_lock_exclusive();
                    kill_self_after(result)
                }
                Some(FaultAction::Short(_) | FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "short/torn actions apply only to positioned I/O",
                )),
                None => self.inner.try_lock_exclusive(),
            }
        }

        fn unlock(&self) -> io::Result<()> {
            match self.handle.action(FaultEvent::Unlock) {
                Some(FaultAction::Error(code) | FaultAction::ErrorAlways(code)) => {
                    Err(io::Error::from_raw_os_error(code))
                }
                Some(FaultAction::KillBefore) => kill_self(),
                Some(FaultAction::KillAfter) => {
                    let result = self.inner.unlock();
                    kill_self_after(result)
                }
                Some(FaultAction::Short(_) | FaultAction::Torn { .. }) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "short/torn actions apply only to positioned I/O",
                )),
                None => self.inner.unlock(),
            }
        }
    }

    fn kill_self_after<T>(_result: io::Result<T>) -> io::Result<T> {
        kill_self()
    }

    fn kill_self<T>() -> T {
        #[cfg(unix)]
        {
            const SIGKILL: i32 = 9;
            // SAFETY: both functions have no pointer arguments; SIGKILL cannot
            // run user code in the target process.
            if unsafe { kill(getpid(), SIGKILL) } == 0 {
                loop {
                    std::thread::park();
                }
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
