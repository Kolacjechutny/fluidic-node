use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use fluidic::consensus::Oscillator;
use fluidic::crypto::keys::KeyPair;
use fluidic::crypto::{AccountId, CommutativeShift, Signal, StatefulShift, VectorClock, DEFAULT_DEX_DOMAIN};
use fluidic::field::coordinates::Coordinate;
use fluidic::value::metabolic::{DEFAULT_DEX_LAMBDA_PPM, MetabolicDecayEngine, MetabolicStream};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const COMMUTATIVE_WORKLOAD: usize = 10_000;
const STATEFUL_WORKLOAD: usize = 2_000;

fn seed_streams(engine: &MetabolicDecayEngine, count: usize) {
    let owner = KeyPair::generate().account_id();
    for i in 0..count {
        let mut id = [0u8; 32];
        id[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        let stream = MetabolicStream::new(id, owner, 1_000_000_000_000, DEFAULT_DEX_LAMBDA_PPM);
        engine.add_stream(stream);
    }
}

fn build_workload() -> (
    Vec<Signal>,
    HashMap<AccountId, ed25519_dalek::VerifyingKey>,
    Vec<KeyPair>,
) {
    let mut keypairs = Vec::with_capacity(STATEFUL_WORKLOAD + 1);
    for _ in 0..(STATEFUL_WORKLOAD + 1) {
        keypairs.push(KeyPair::generate());
    }
    let registry: HashMap<_, _> = keypairs
        .iter()
        .map(|kp| (kp.account_id(), kp.public_key()))
        .collect();

    let mut shifts = Vec::with_capacity(COMMUTATIVE_WORKLOAD + STATEFUL_WORKLOAD);

    // Commutative deltas.
    let sender = &keypairs[0];
    let pool = [0xAB; 32];
    for i in 0..COMMUTATIVE_WORKLOAD {
        shifts.push(Signal::Commutative(CommutativeShift::new(
            sender,
            DEFAULT_DEX_DOMAIN,
            Coordinate::from_scalar(i as u64),
            1_000_000,
            pool,
            i as u64,
            0,
        )));
    }

    // Stateful transfers requiring signature verification.
    let mut vc = VectorClock::new();
    vc.tick([1u8; 32]);
    for i in 0..STATEFUL_WORKLOAD {
        let from = &keypairs[i % keypairs.len()];
        let to = keypairs[(i + 1) % keypairs.len()].account_id();
        shifts.push(Signal::Stateful(StatefulShift::new(
            from,
            DEFAULT_DEX_DOMAIN,
            to,
            1_000_000,
            vc.clone(),
            vec![],
            i as u64,
            0,
        )));
    }

    (shifts, registry, keypairs)
}

fn create_oscillator(stream_count: usize, keypairs: &[KeyPair]) -> Oscillator {
    let osc = Oscillator::new([0u8; 32], 4096);
    for kp in keypairs {
        osc.seed_account(kp.account_id(), 1_000_000_000_000_000);
    }
    seed_streams(&osc.metabolic_engine, stream_count);
    osc
}

fn bench_metabolic_decay(c: &mut Criterion) {
    let mut group = c.benchmark_group("metabolic_decay");
    group.measurement_time(Duration::from_secs(5));

    let (shifts, registry, keypairs) = build_workload();

    // Raw decay engine cost at several loads.
    for count in [1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("engine_only", count),
            &count,
            |b, &count| {
                let engine = MetabolicDecayEngine::new();
                seed_streams(&engine, count);
                let tick = AtomicU64::new(1);
                b.iter(|| {
                    let t = tick.fetch_add(1, Ordering::Relaxed);
                    let burned = engine.process_metabolic_degradation(t);
                    black_box(burned);
                });
            },
        );
    }

    // Full oscillator synthesis with and without metabolic load.
    // A fresh oscillator is created per iteration so signature deduplication
    // does not cause subsequent iterations to become no-ops.
    for count in [0, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("synthesize_with_decay", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    || create_oscillator(count, &keypairs),
                    |osc| {
                        for shift in &shifts {
                            osc.ingest(shift.clone()).unwrap();
                        }
                        let result = osc.synthesize(&registry);
                        black_box(result);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();

    eprintln!("\n=== Metabolic Decay Overhead Invariant ===");
    eprintln!("Compare 'synthesize_with_decay/0' with 'synthesize_with_decay/10000'.");
    eprintln!("Target: incremental decay time < 1% of total synthesis time.");
    eprintln!("============================================\n");
}

criterion_group!(benches, bench_metabolic_decay);
criterion_main!(benches);
