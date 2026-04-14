use criterion::{black_box, criterion_group, criterion_main, Criterion};
use diff_core::{InMemoryBackend, LogicalRange, ReadContext, StorageBackend};

fn seal_read(c: &mut Criterion) {
    let b = InMemoryBackend::new();
    b.write(0, &[0u8; 4096]).unwrap();
    let _ = black_box(b.seal().unwrap());
    c.bench_function("read_4k_tip", |bench| {
        bench.iter(|| {
            let _ = black_box(
                b.read(
                    LogicalRange {
                        offset: 0,
                        len: 4096,
                    },
                    ReadContext { snapshot_id: None },
                )
                .unwrap(),
            );
        });
    });
}

criterion_group!(benches, seal_read);
criterion_main!(benches);
