use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use sstables::SSTableReader;
use std::io::Result;
use testing::setup;

const PATH_DIR: &str = ".tmp/benches";

#[allow(dead_code)]
async fn cbor_sstable_write_read(n: usize) -> Result<()> {
  setup::create_dir_all(PATH_DIR)?;
  let full_path = setup::join_path(PATH_DIR, &format!("cbor_sstable_write_read_{}", n));
  setup::remove_file(&full_path)?;

  {
    let mut reader = SSTableReader::from_path(&full_path)?;
    for _ in 0..n {
      reader.read_next().unwrap().unwrap();
    }
  }
  Ok(())
}

#[allow(dead_code)]
async fn cbor_indexed_sstable_write_read(n: usize) -> Result<()> {
  setup::create_dir_all(PATH_DIR)?;
  let full_path = setup::join_path(PATH_DIR, &format!("cbor_indexed_sstable_write_read_{}", n));
  setup::remove_file(&full_path)?;

  {
    let mut reader = SSTableReader::from_path(&full_path)?;
    for _ in 0..n {
      reader.read_next().unwrap().unwrap();
    }
  }
  Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
  static N: usize = 10;

  let mut group = c.benchmark_group("file_readers");
  for i in (0..N).step_by(2) {
    let size = i * 10000;

    // Set to "Flat" if a long-running test
    group.sampling_mode(SamplingMode::Auto);

    // Set to the amount of bytes, if relevant
    group.throughput(Throughput::Bytes((size * 10) as u64));

    group.bench_with_input(BenchmarkId::new("sstable", size), &size, |b, &n| {
      b.to_async(FuturesExecutor).iter(|| cbor_sstable_write_read(n));
    });

    group.bench_with_input(BenchmarkId::new("indexed_sstable", size), &size, |b, &n| {
      b.to_async(FuturesExecutor).iter(|| cbor_sstable_write_read(n));
    });
  }
  group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
