//! In-memory append-only layers + extent overlay (reference implementation).

use std::sync::RwLock;

use bytes::Bytes;
use uuid::Uuid;

use crate::{DiffCoreError, LogicalRange, ReadContext, SnapshotId, StorageBackend};

#[derive(Clone, Debug)]
struct Extent {
    logical_start: u64,
    length: u64,
    /// Index into `sealed` (0..sealed.len()) or `usize::MAX` for active buffer.
    layer_index: usize,
    phys_start: u64,
}

/// `layer_index == usize::MAX` means active layer.
const ACTIVE: usize = usize::MAX;

/// Single-writer (external lock), multi-reader in-memory backend.
pub struct InMemoryBackend {
    inner: RwLock<Inner>,
}

#[derive(Clone, Debug)]
struct Inner {
    sealed: Vec<Vec<u8>>,
    active: Vec<u8>,
    extents: Vec<Extent>,
    /// Monotonic seal count; snapshot k corresponds to sealed prefix length k (simplification).
    snapshots: Vec<Uuid>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                sealed: Vec::new(),
                active: Vec::new(),
                extents: Vec::new(),
                snapshots: Vec::new(),
            }),
        }
    }

    fn layer_data<'a>(&'a self, inner: &'a Inner, layer_index: usize) -> &'a [u8] {
        if layer_index == ACTIVE {
            &inner.active
        } else {
            &inner.sealed[layer_index]
        }
    }

    /// Last matching extent in `extents` wins (newest write on same layer).
    /// For snapshot reads, `max_sealed`: only extents with `layer_index <= max_sealed` (sealed layers 0..=k); excludes ACTIVE.
    fn read_byte(&self, inner: &Inner, offset: u64, max_sealed: Option<usize>) -> u8 {
        for e in inner.extents.iter().rev() {
            if offset < e.logical_start || offset >= e.logical_start + e.length {
                continue;
            }
            if let Some(ms) = max_sealed {
                if e.layer_index == ACTIVE || e.layer_index > ms {
                    continue;
                }
            }
            let within = offset - e.logical_start;
            let phys = e.phys_start + within;
            let blob = self.layer_data(inner, e.layer_index);
            return blob.get(phys as usize).copied().unwrap_or(0);
        }
        0
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for InMemoryBackend {
    fn read(&self, logical: LogicalRange, ctx: ReadContext) -> Result<Bytes, DiffCoreError> {
        let inner = self.inner.read().map_err(|_| DiffCoreError::Busy)?;
        let max_sealed = match ctx.snapshot_id {
            None => None,
            Some(SnapshotId(id)) => {
                let idx = inner.snapshots.iter().position(|s| *s == id);
                match idx {
                    Some(i) => Some(i),
                    None => return Err(DiffCoreError::SnapshotNotFound),
                }
            }
        };

        let mut out = vec![0u8; logical.len as usize];
        for (i, b) in out.iter_mut().enumerate() {
            let off = logical
                .offset
                .checked_add(i as u64)
                .ok_or(DiffCoreError::Eof)?;
            *b = self.read_byte(&inner, off, max_sealed);
        }
        Ok(Bytes::from(out))
    }

    fn write(&self, logical_offset: u64, data: &[u8]) -> Result<(), DiffCoreError> {
        let mut inner = self.inner.write().map_err(|_| DiffCoreError::Busy)?;
        let phys_start = inner.active.len() as u64;
        inner.active.extend_from_slice(data);
        inner.extents.push(Extent {
            logical_start: logical_offset,
            length: data.len() as u64,
            layer_index: ACTIVE,
            phys_start,
        });
        Ok(())
    }

    fn flush(&self) -> Result<(), DiffCoreError> {
        Ok(())
    }

    fn fsync(&self) -> Result<(), DiffCoreError> {
        Ok(())
    }

    fn seal(&self) -> Result<SnapshotId, DiffCoreError> {
        let mut inner = self.inner.write().map_err(|_| DiffCoreError::Busy)?;
        let active = std::mem::take(&mut inner.active);
        let new_idx = inner.sealed.len();
        inner.sealed.push(active);
        // Repoint extents that pointed at ACTIVE to new sealed index
        for e in &mut inner.extents {
            if e.layer_index == ACTIVE {
                e.layer_index = new_idx;
            }
        }
        let id = Uuid::new_v4();
        inner.snapshots.push(id);
        Ok(SnapshotId(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_after_write() {
        let b = InMemoryBackend::new();
        b.write(10, &[1, 2, 3]).unwrap();
        let r = b
            .read(
                LogicalRange { offset: 10, len: 3 },
                ReadContext { snapshot_id: None },
            )
            .unwrap();
        assert_eq!(&r[..], &[1, 2, 3]);
    }

    #[test]
    fn seal_freezes_then_new_writes() {
        let b = InMemoryBackend::new();
        b.write(0, &[9]).unwrap();
        let snap = b.seal().unwrap();
        b.write(0, &[7]).unwrap();
        let tip = b
            .read(
                LogicalRange { offset: 0, len: 1 },
                ReadContext { snapshot_id: None },
            )
            .unwrap();
        assert_eq!(tip[0], 7);
        let old = b
            .read(
                LogicalRange { offset: 0, len: 1 },
                ReadContext {
                    snapshot_id: Some(snap),
                },
            )
            .unwrap();
        assert_eq!(old[0], 9);
    }

    #[test]
    fn overlapping_writes_latest_wins() {
        let b = InMemoryBackend::new();
        b.write(0, &[1, 2, 3, 4]).unwrap();
        b.write(2, &[9, 9]).unwrap();
        let r = b
            .read(
                LogicalRange { offset: 0, len: 4 },
                ReadContext { snapshot_id: None },
            )
            .unwrap();
        assert_eq!(&r[..], &[1, 2, 9, 9]);
    }
}
