//! `openduck-fuse mount --db … --postgres … --data-dir … [--mountpoint …]`

#[cfg(target_os = "linux")]
include!("openduck_fuse_main_linux.inc");

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("openduck-fuse requires Linux with FUSE3.");
    std::process::exit(2);
}
