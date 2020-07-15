use snarkos_utilities::{bits_to_bytes, bytes_to_bits};

use bitvec::prelude::*;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

fn byte_conversion_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("byte_conversion");
    group.sample_size(100);

    let rng = &mut XorShiftRng::seed_from_u64(1231275789u64);

    // Bytes to Bits

    let mut bytes: [u8; 64] = [0u8; 64];
    rng.fill(&mut bytes);

    group.bench_function("native_bytes_to_bits", |b| {
        b.iter(|| {
            let _bits = bytes_to_bits(&bytes);
        });
    });

    group.bench_function("bitvec_bytes_to_bits", |b| {
        b.iter(|| {
            let _bv = BitVec::<Lsb0, u8>::from_slice(&bytes[..]);
        });
    });

    // Bits to Bytes

    let mut bits: Vec<bool> = Vec::with_capacity(503);
    for _ in 0..bits.capacity() {
        bits.push(rng.gen());
    }

    let bv: BitVec<Lsb0, u8> = (&bits[..]).into();

    group.bench_function("native_bits_to_bytes", |b| {
        b.iter(|| {
            let _bytes = bits_to_bytes(&bits);
        });
    });

    group.bench_function("bitvec_bits_to_bytes", |b| {
        b.iter(|| {
            let _bytes = bv.clone().into_vec();
        });
    });
}

criterion_group!(benches, byte_conversion_bench);

criterion_main!(benches);
