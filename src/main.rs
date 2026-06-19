use fluidic::consensus::Oscillator;
use fluidic::crypto::keys::KeyPair;
use fluidic::crypto::{CommutativeShift, Signal, VectorClock, DEFAULT_DEX_DOMAIN};
use fluidic::field::coordinates::Coordinate;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let oscillator = Oscillator::new([1u8; 32], 1024);
    let alice = KeyPair::generate();
    let bob = KeyPair::generate();
    oscillator.seed_account(alice.account_id(), 1_000_000_000_000_000);

    // Inject a few commutative pool shifts.
    let pool = [42u8; 32];
    for i in 0..100 {
        let shift = CommutativeShift::new(
            &alice,
            DEFAULT_DEX_DOMAIN,
            Coordinate::from_scalar(i as u64),
            1_000_000,
            pool,
            i as u64,
            0,
        );
        oscillator
            .ingest(Signal::Commutative(shift))
            .expect("ingest commutative");
    }

    // Stateful transfer.
    let mut vc = VectorClock::new();
    vc.tick(oscillator.id);
    let st = fluidic::crypto::StatefulShift::new(
        &alice,
        DEFAULT_DEX_DOMAIN,
        bob.account_id(),
        500_000_000_000,
        vc,
        vec![],
        1,
        0,
    );
    oscillator
        .ingest(Signal::Stateful(st))
        .expect("insert stateful");

    let mut registry = HashMap::new();
    registry.insert(alice.account_id(), alice.public_key());
    let result = oscillator.synthesize(&registry);

    println!("Commutative applied: {}", result.commutative_applied);
    println!("Stateful applied: {}", result.stateful_applied);
    println!(
        "Final Alice balance: {}",
        result.final_balances[&alice.account_id()]
    );
    println!(
        "Final Bob balance: {}",
        result.final_balances[&bob.account_id()]
    );

    let field = oscillator.wave_field.lock().unwrap();
    println!("Pool balance: {}", field.pool_balance(pool).units);
}
