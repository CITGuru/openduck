//! FUSE adapter: present a logical single file backed by [`diff_core::StorageBackend`] (Linux).

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::{mount_backend, MountOptions};

#[cfg(target_os = "linux")]
use diff_core::InMemoryBackend;
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[cfg(target_os = "linux")]
pub fn mount_memory_backend(
    backend: Arc<InMemoryBackend>,
    opts: MountOptions<'_>,
) -> Result<(), std::io::Error> {
    mount_backend(backend, opts)
}

#[cfg(not(target_os = "linux"))]
mod other;

#[cfg(not(target_os = "linux"))]
pub use other::mount_memory_backend_disabled;
