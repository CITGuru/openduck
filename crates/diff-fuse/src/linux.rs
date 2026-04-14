use std::ffi::OsStr;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use diff_core::{LogicalRange, ReadContext, StorageBackend};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::ENOENT;

const FILE_INO: u64 = 2;
const TTL: Duration = Duration::from_secs(1);
/// Logical size exposed to DuckDB (sparse); reads hit backend, unwritten ranges zero-filled.
const LOGICAL_SIZE: u64 = 1 << 30;

pub struct MountOptions<'a> {
    pub mountpoint: &'a Path,
}

/// Mount a single file `database.duckdb` at `mountpoint` backed by `backend` (blocking).
pub fn mount_backend(
    backend: Arc<dyn StorageBackend>,
    opts: MountOptions<'_>,
) -> Result<(), std::io::Error> {
    create_dir_all(opts.mountpoint)?;
    let fs = OpenDuckFs { backend };
    fuser::mount2(
        fs,
        opts.mountpoint,
        &[MountOption::FSName("openduck".into())],
    )
}

struct OpenDuckFs {
    backend: Arc<dyn StorageBackend>,
}

fn file_attr(size: u64) -> FileAttr {
    FileAttr {
        ino: FILE_INO,
        size,
        blocks: (size + 511) / 512,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::RegularFile,
        perm: 0o644,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

impl Filesystem for OpenDuckFs {
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match ino {
            FUSE_ROOT_ID => {
                reply.attr(
                    &TTL,
                    &FileAttr {
                        ino: FUSE_ROOT_ID,
                        size: 4096,
                        blocks: 8,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: unsafe { libc::getuid() },
                        gid: unsafe { libc::getgid() },
                        rdev: 0,
                        blksize: 4096,
                        flags: 0,
                    },
                );
            }
            FILE_INO => reply.attr(&TTL, &file_attr(LOGICAL_SIZE)),
            _ => reply.error(ENOENT),
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }
        if name == "database.duckdb" {
            reply.entry(&TTL, &file_attr(LOGICAL_SIZE), 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        if ino != FILE_INO {
            reply.error(ENOENT);
            return;
        }
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let data = match self.backend.read(
            LogicalRange {
                offset: offset as u64,
                len: size as u64,
            },
            ReadContext { snapshot_id: None },
        ) {
            Ok(b) => b,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };
        reply.data(&data);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyWrite,
    ) {
        if ino != FILE_INO {
            reply.error(ENOENT);
            return;
        }
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        if self.backend.write(offset as u64, data).is_err() {
            reply.error(libc::EIO);
            return;
        }
        reply.written(data.len() as u32);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if ino != FILE_INO {
            reply.error(ENOENT);
            return;
        }
        reply.opened(0, 0);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        if ino != FILE_INO {
            reply.error(ENOENT);
            return;
        }
        let _ = self.backend.flush();
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        if ino != FILE_INO {
            reply.error(ENOENT);
            return;
        }
        let _ = self.backend.fsync();
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _fh: Option<u64>,
        _ctime: Option<fuser::TimeOrNow>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        ctime: Option<Duration>,
        _chgtime: Option<fuser::TimeOrNow>,
        _bkuptime: Option<fuser::TimeOrNow>,
        reply: ReplyAttr,
    ) {
        if ino != FILE_INO {
            reply.error(ENOENT);
            return;
        }
        if let Some(s) = size {
            if self.backend.truncate(s).is_err() {
                reply.error(libc::EIO);
                return;
            }
        }
        let mut attr = file_attr(LOGICAL_SIZE);
        if let Some(size) = size {
            attr.size = size;
        }
        if let Some(ct) = ctime {
            attr.ctime = UNIX_EPOCH + ct;
        }
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }
        let entries: [(u64, FileType, &str); 3] = [
            (FUSE_ROOT_ID, FileType::Directory, "."),
            (FUSE_ROOT_ID, FileType::Directory, ".."),
            (FILE_INO, FileType::RegularFile, "database.duckdb"),
        ];
        for (i, (e_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*e_ino, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }
}
