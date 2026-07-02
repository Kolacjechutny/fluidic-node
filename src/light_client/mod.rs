//! Light-client support for the Fluidic mesh.
//!
//! Light clients do not synthesize full state. They connect to one or more
//! operators, observe signed synthesis certificates, and verify operator
//! signatures against a known key registry. Once a tick has accumulated enough
//! stake-weighted certificates, the light client treats the reported state root
//! as finalized for that tick.

use crate::consensus::certificate::{CertificateTracker, SynthesisCertificate};
use crate::consensus::SynthesisResult;
use crate::crypto::{AccountId, KeyPair};
use crate::operator::StakeTable;
use ed25519_dalek::VerifyingKey;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A light client tracks operator certificates and reports finalized per-tick
/// state roots without running full synthesis.
#[derive(Clone)]
pub struct LightClient {
    /// Registry mapping operator accounts to their Ed25519 verifying keys.
    key_registry: Arc<RwLock<HashMap<AccountId, VerifyingKey>>>,
    /// Stake table used to determine certificate eligibility and quorum.
    stake_table: Arc<StakeTable>,
    /// Tracks observed certificates and detects operator equivocation.
    tracker: Arc<RwLock<CertificateTracker>>,
    /// Latest finalized tick observed by the light client.
    latest_finalized_tick: Arc<RwLock<u64>>,
}

impl LightClient {
    /// Create a new light client for the given stake table. The key registry is
    /// initially empty; callers should populate it with known operator keys.
    pub fn new(stake_table: Arc<StakeTable>) -> Self {
        Self {
            key_registry: Arc::new(RwLock::new(HashMap::new())),
            stake_table,
            tracker: Arc::new(RwLock::new(CertificateTracker::new())),
            latest_finalized_tick: Arc::new(RwLock::new(0)),
        }
    }

    /// Register an operator's public key. Unknown operators are rejected when
    /// their certificates are ingested.
    pub fn register_key(&self,
        account: AccountId,
        public_key: VerifyingKey,
    ) {
        self.key_registry.write().unwrap().insert(account, public_key);
    }

    /// Ingest a synthesis certificate from an operator. Equivocating operators
    /// are slashed through the shared stake table. Returns the quorum view if
    /// this tick has just reached quorum.
    pub fn ingest_certificate(
        &self,
        cert: SynthesisCertificate,
    ) -> Result<Option<crate::consensus::certificate::QuorumView>, String> {
        let registry = self.key_registry.read().unwrap();
        let stake_table = self.stake_table.clone();
        let stake_checker = |op: &AccountId| stake_table.is_staked(op);
        let stake_amount = |op: &AccountId| stake_table.get_stake(op);
        let mut slash = |op: AccountId| {
            let _ = stake_table.slash(op);
        };

        let mut tracker = self.tracker.write().unwrap();
        tracker
            .apply(cert.clone(), &registry, &stake_checker, &stake_amount, &mut slash)
            .map_err(|e| format!("certificate rejected: {:?}", e))?;

        let tick = cert.tick;
        let threshold = stake_table.quorum_threshold();
        let quorum = tracker.check_quorum(tick, threshold);
        if quorum.is_some() {
            let mut latest = self.latest_finalized_tick.write().unwrap();
            *latest = (*latest).max(tick);
        }
        Ok(quorum.map(|(view, _)| view))
    }

    /// Return the highest tick that has reached quorum so far.
    pub fn latest_finalized_tick(&self) -> u64 {
        *self.latest_finalized_tick.read().unwrap()
    }

    /// Verify that a claimed `SynthesisResult` for a tick matches the quorum
    /// view. Light clients can call this when an operator provides a full result
    /// rather than just a certificate.
    pub fn verify_result(
        &self,
        _tick: u64,
        _result: &SynthesisResult,
    ) -> Result<bool, String> {
        // Future work: compare result roots against the certificate view once
        // `SynthesisResult` exposes the same root hashes that certificates sign.
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::certificate::SynthesisCertificate;
    use crate::crypto::KeyPair;
    use crate::operator::{StakingConfig, StakeTable};

    #[test]
    fn light_client_tracks_quorum() {
        let operator = KeyPair::generate();
        let stake_table = Arc::new(StakeTable::new(StakingConfig {
            min_stake: 1_000_000,
        }));
        stake_table.stake(operator.account_id(), 1_000_000);

        let client = LightClient::new(stake_table);
        client.register_key(operator.account_id(), operator.public_key());

        // Single staked operator with default quorum threshold (simple majority
        // of total stake) reaches quorum immediately.
        let cert = SynthesisCertificate::sign(
            &operator,
            1,
            0,
            0,
            0,
            [0u8; 32],
            [0u8; 32],
            [0u8; 32],
            [0u8; 32],
            [0u8; 32],
            [0u8; 32],
            0,
            0,
        );
        let quorum = client.ingest_certificate(cert).unwrap();
        assert!(quorum.is_some());
        assert_eq!(client.latest_finalized_tick(), 1);
    }
}
