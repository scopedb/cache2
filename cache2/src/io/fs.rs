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

//! Path operations used to open cache files and atomically install recovery images.

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use crate::config::runtime::IoMode;
use crate::io::file::CacheFile;
use crate::io::file::StorageFile;

pub trait FileSystem {
    type File: StorageFile;

    fn open(&self, path: &Path, create: bool, mode: IoMode) -> io::Result<Self::File>;

    fn create_new(&self, path: &Path) -> io::Result<Self::File>;

    fn remove_file(&self, path: &Path) -> io::Result<()>;

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()>;

    fn sync_parent(&self, path: &Path) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OsFileSystem;

impl FileSystem for OsFileSystem {
    type File = CacheFile;

    fn open(&self, path: &Path, create: bool, mode: IoMode) -> io::Result<Self::File> {
        if create {
            CacheFile::open_with_io_mode(path, mode)
        } else {
            CacheFile::open_existing_with_io_mode(path, mode)
        }
    }

    fn create_new(&self, path: &Path) -> io::Result<Self::File> {
        CacheFile::create_new_buffered(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        fs::rename(source, destination)
    }

    fn sync_parent(&self, path: &Path) -> io::Result<()> {
        File::open(parent_directory(path))?.sync_all()
    }
}

pub fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
