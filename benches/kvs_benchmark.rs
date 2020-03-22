use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kvs::error::KvStoreError;
use kvs::{KvStore, SledEngine, KvsEngine};
use tempfile::TempDir;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rand::rngs::ThreadRng;
use kvs::error::KvStoreError::SledError;

pub fn get_random_string(gen: &mut ThreadRng) -> String {
    let sz = gen.gen_range(1, 100000);
    gen.sample_iter(&Alphanumeric).take(sz).collect()
}

pub fn criterion_benchmark_kvs(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).expect("unable to create KvStore");
    let mut gen = rand::thread_rng();
    let mut keys = vec![];
    c.bench_function("kvs_write", |b| b.iter(|| {
        let key = get_random_string(&mut gen);
        let value = get_random_string(&mut gen);
        keys.push(key.clone());
        store.set(key, value).expect("failed to set value");
    }));
    c.bench_function("kvs_read", |b| b.iter(|| {
        let key = keys[gen.gen_range(0, keys.len())].clone();
        store.get(key).expect("failed to get key");
    }));
}

pub fn criterion_benchmark_sled(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = SledEngine::open(temp_dir.path()).expect("unable to create KvStore");
    let mut gen = rand::thread_rng();
    let mut keys = vec![];
    c.bench_function("sled_write", |b| b.iter(|| {
        let key = get_random_string(&mut gen);
        let value = get_random_string(&mut gen);
        keys.push(key.clone());
        store.set(key, value).expect("failed to set value");
    }));
    c.bench_function("sled_read", |b| b.iter(|| {
        let key = keys[gen.gen_range(0, keys.len())].clone();
        store.get(key).expect("failed to get key");
    }));
}

criterion_group!(benches, criterion_benchmark_kvs, criterion_benchmark_sled);
criterion_main!(benches);
