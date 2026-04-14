//! Local append-only segment files for the active differential layer.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum LayerFsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// Append-only writer for one layer segment on disk.
pub struct AppendSegment {
    path: PathBuf,
    file: File,
    len: u64,
}

impl AppendSegment {
    /// Create a new segment at an exact path (parent directories created).
    pub fn create_at(path: impl AsRef<Path>) -> Result<Self, LayerFsError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let len = file.metadata()?.len();
        Ok(Self { path, file, len })
    }

    pub fn create_in_dir(dir: impl AsRef<Path>) -> Result<Self, LayerFsError> {
        let id = Uuid::new_v4();
        let path = dir.as_ref().join(format!("active-{id}.layer"));
        Self::create_at(path)
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, LayerFsError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        let len = file.metadata()?.len();
        Ok(Self { path, file, len })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append bytes; returns physical offset where data starts.
    pub fn append(&mut self, data: &[u8]) -> Result<u64, LayerFsError> {
        let off = self.len;
        self.file.write_all(data)?;
        self.file.sync_data()?;
        self.len += data.len() as u64;
        Ok(off)
    }

    pub fn flush(&mut self) -> Result<(), LayerFsError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), LayerFsError> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }
}

/// Read-only random access to a sealed segment path (shared by many readers).
pub fn read_segment_bytes(path: &Path, phys_off: u64, buf: &mut [u8]) -> Result<(), LayerFsError> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(phys_off))?;
    file.read_exact(buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn append_roundtrip() {
        let dir = std::env::temp_dir().join(format!("openduck-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let mut seg = AppendSegment::create_in_dir(&dir).unwrap();
        let o = seg.append(&[1, 2, 3]).unwrap();
        assert_eq!(o, 0);
        let o2 = seg.append(&[4]).unwrap();
        assert_eq!(o2, 3);
        let mut b = [0u8; 4];
        seg.read_at(0, &mut b).unwrap();
        assert_eq!(b, [1, 2, 3, 4]);
    }
}
