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

    // Both MSVC and mingw need the Windows system libraries spelled
    // out — DuckDB's `local_file_system.cpp` calls into the Restart
    // Manager API (`RmStartSession` etc., from `Rstrtmgr.lib`) and
    // also pulls in `ws2_32` and `bcrypt`. Apple/Linux don't need
    // any of this.
    if target_os == "windows" {
        // `cargo:warning=...` shows up in CI logs; useful for
        // confirming this build script ran at all on the Windows
        // runner. Remove once we're confident.
        println!(
            "cargo:warning=exec-worker/build.rs: emitting Windows native link libs \
             (target_os={target_os}, target_env={target_env}); see file header for context"
        );
        // Mirror DuckDB's `src/CMakeLists.txt` Windows branch:
        //   set(DUCKDB_SYSTEM_LIBS ${DUCKDB_SYSTEM_LIBS} ws2_32 rstrtmgr)
        //   set(DUCKDB_SYSTEM_LIBS ${DUCKDB_SYSTEM_LIBS} bcrypt)
        // Use lowercase names — MSVC's library lookup is case-insensitive,
        // but mingw's is sometimes not.
        println!("cargo:rustc-link-lib=dylib=ws2_32");
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
        println!("cargo:rustc-link-lib=dylib=bcrypt");
    }

    // Re-run only when the target tuple changes — the body otherwise
    // depends on no files.
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ENV");
    println!("cargo:rerun-if-changed=build.rs");
}
