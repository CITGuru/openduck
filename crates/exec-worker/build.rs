//! Workaround for `libduckdb-sys` missing Windows system-library link
//! directives when building DuckDB v1.5.x in `bundled` mode.
//!
//! DuckDB v1.5 added `duckdb::AdditionalLockInfo`
//! (`duckdb/src/common/local_file_system.cpp`), which uses the Windows
//! Restart Manager API to tell the user *which* process is holding a
//! file lock. The corresponding upstream CMake adds `ws2_32`,
//! `rstrtmgr`, and `bcrypt` to `DUCKDB_SYSTEM_LIBS`
//! (`duckdb/src/CMakeLists.txt`), but `libduckdb-sys`'s
//! `build_bundled_cc.rs` (which uses the `cc` crate, not CMake) never
//! emits the corresponding `cargo:rustc-link-lib` directives.
//!
//! The MSVC linker then can't resolve `RmStartSession`, `RmEndSession`,
//! `RmRegisterResources`, `RmGetList` and the entire workspace fails
//! to link on `windows-latest` GitHub-Actions runners.
//!
//! Emitting the same directives from this build script propagates them
//! to every binary / example / test that links `exec_worker` (and,
//! transitively, to anything that depends on `exec-worker`). Once
//! `libduckdb-sys` upstream ships the fix, this whole file can be
//! deleted.

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();

    // Only the MSVC ABI requires us to spell out the system libs;
    // GNU / mingw resolves them differently and does not need this
    // workaround.
    if target_os == "windows" && target_env == "msvc" {
        // Mirror DuckDB's `src/CMakeLists.txt` Windows branch:
        //   set(DUCKDB_SYSTEM_LIBS ${DUCKDB_SYSTEM_LIBS} ws2_32 rstrtmgr)
        //   set(DUCKDB_SYSTEM_LIBS ${DUCKDB_SYSTEM_LIBS} bcrypt)
        println!("cargo:rustc-link-lib=dylib=ws2_32");
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
        println!("cargo:rustc-link-lib=dylib=bcrypt");
    }

    // Re-run only when the target tuple changes — the body otherwise
    // depends on no files.
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ENV");
}
