use diff_core::InMemoryBackend;

/// FUSE is only built for Linux in this milestone; call sites should `cfg` gate.
pub fn mount_memory_backend_disabled(
    _backend: std::sync::Arc<InMemoryBackend>,
    _mountpoint: &std::path::Path,
) -> ! {
    panic!("diff-fuse: FUSE is only supported on Linux in this build");
}
