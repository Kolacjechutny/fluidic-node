//! # Fluidic: Continuous-Wave State Engine (Research Prototype)
//!
//! Fluidic models decentralized state as a wave-field. It uses:
//!
//! - **Number Theoretic Transforms (NTT)** to aggregate *commutative*,
//!   state-independent operations (liquidity-pool shifts, micro-payment streams,
//!   routing) in a way that is agnostic to arrival order.
//! - **Vector-clock DAG ordering** to enforce strict causal consistency for
//!   *state-dependent* operations such as unique balance exhaustion.
//!
//! > **Important:** This is a testnet-ready research implementation. It provides
//! > persistent snapshots and a permissioned operator model, but it has not yet
//! > been audited or deployed to a permissionless mainnet.

pub mod api;
pub mod consensus;
pub mod crypto;
pub mod evm;
pub mod field;
pub mod network;
pub mod operator;
pub mod persistence;
pub mod state;
pub mod value;

pub use consensus::{DagError, NttEngine, Oscillator, SynthesisResult, VectorClockDag};
pub use crypto::{
    AccountId, CommutativeShift, KeyPair, Signal, PoolId, StatefulShift, TxHash, VectorClock,
    WaveAddress,
};
pub use field::{Coordinate, FrequencyVector, WAVE_PRECISION, WaveField};
pub use network::{NetworkNode, Transport, encode_packet};
pub use value::{Spectrum, StreamState, ValueStream};

/// Fixed-point sub-units per WAVE token (10^12).
pub const WAVE_SUBUNIT: u128 = WAVE_PRECISION;
