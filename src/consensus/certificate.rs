use crate::crypto::{AccountId, KeyPair};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A signed attestation produced by an operator after each synthesis tick.
///
/// Certificates bind the operator's identity to the observable result of a
/// synthesis cycle.  Peer nodes can verify the signature and compare roots to
/// detect equivocation or deterministic divergence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SynthesisCertificate {
    pub operator: AccountId,
    pub tick: u64,
    pub commutative_applied: usize,
    pub stateful_applied: usize,
    pub evm_applied: usize,
    /// Root hash over the ordered list of commutative deltas applied this tick.
    pub commutative_root: [u8; 32],
    /// Root hash over the ordered list of stateful shift hashes applied this tick.
    pub stateful_root: [u8; 32],
    /// Root hash over the final account balances after this tick.
    pub balances_root: [u8; 32],
    /// Root hash over the current stake table.
    pub stake_root: [u8; 32],
    /// Root hash over the current reward pool.
    pub reward_root: [u8; 32],
    /// Root hash over applied EVM transactions this tick.
    pub evm_root: [u8; 32],
    pub metabolic_burned: u128,
    pub timestamp_ns: u64,
    pub signature: Vec<u8>,
}

impl SynthesisCertificate {
    /// Build a certificate and sign it with the operator's keypair.
    pub fn sign(
        keypair: &KeyPair,
        tick: u64,
        commutative_applied: usize,
        stateful_applied: usize,
        evm_applied: usize,
        commutative_root: [u8; 32],
        stateful_root: [u8; 32],
        balances_root: [u8; 32],
        stake_root: [u8; 32],
        reward_root: [u8; 32],
        evm_root: [u8; 32],
        metabolic_burned: u128,
        timestamp_ns: u64,
    ) -> Self {
        let operator = keypair.account_id();
        let mut cert = Self {
            operator,
            tick,
            commutative_applied,
            stateful_applied,
            evm_applied,
            commutative_root,
            stateful_root,
            balances_root,
            stake_root,
            reward_root,
            evm_root,
            metabolic_burned,
            timestamp_ns,
            signature: Vec::new(),
        };
        let sig = keypair.sign(&cert.signing_bytes());
        cert.signature = sig.to_bytes().to_vec();
        cert
    }

    /// Return the canonical byte string that the operator signature covers.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(b"FLUIDIC:SYNTHESIS:v3");
        bytes.extend_from_slice(&self.operator.0);
        bytes.extend_from_slice(&self.tick.to_le_bytes());
        bytes.extend_from_slice(&self.commutative_applied.to_le_bytes());
        bytes.extend_from_slice(&self.stateful_applied.to_le_bytes());
        bytes.extend_from_slice(&self.evm_applied.to_le_bytes());
        bytes.extend_from_slice(&self.commutative_root);
        bytes.extend_from_slice(&self.stateful_root);
        bytes.extend_from_slice(&self.balances_root);
        bytes.extend_from_slice(&self.stake_root);
        bytes.extend_from_slice(&self.reward_root);
        bytes.extend_from_slice(&self.evm_root);
        bytes.extend_from_slice(&self.metabolic_burned.to_le_bytes());
        bytes.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        bytes
    }

    /// Verify the operator signature.
    pub fn verify(&self, public_key: &ed25519_dalek::VerifyingKey) -> bool {
        let Ok(sig) = ed25519_dalek::Signature::from_slice(&self.signature) else {
            return false;
        };
        KeyPair::verify(public_key, &self.signing_bytes(), &sig)
    }

    /// Deterministic content hash of the certificate.
    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fluidic:certificate:v1");
        hasher.update(&self.signing_bytes());
        hasher.finalize().into()
    }
}

/// Compute the commutative-root for a list of applied deltas.
///
/// Deltas are sorted canonically before hashing so that nodes which receive
/// the same commutative batch in different network orders still agree on the
/// same root.
pub fn commutative_root(
    tick: u64,
    deltas: &[(crate::field::coordinates::Coordinate, i128, crate::crypto::PoolId)],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:commutative-root:v2");
    hasher.update(&tick.to_le_bytes());
    hasher.update(&deltas.len().to_le_bytes());

    let mut ordered: Vec<_> = deltas.iter().collect();
    ordered.sort_by(|(c1, d1, p1), (c2, d2, p2)| {
        p1.cmp(p2)
            .then_with(|| c1.to_bytes().cmp(&c2.to_bytes()))
            .then_with(|| d1.cmp(d2))
    });

    for (coord, delta, pool) in ordered {
        hasher.update(&coord.to_bytes());
        hasher.update(&delta.to_le_bytes());
        hasher.update(pool);
    }
    hasher.finalize().into()
}

/// Compute the stateful-root for an ordered list of applied shift hashes.
pub fn stateful_root(tick: u64, hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:stateful-root:v1");
    hasher.update(&tick.to_le_bytes());
    hasher.update(&hashes.len().to_le_bytes());
    for h in hashes {
        hasher.update(h);
    }
    hasher.finalize().into()
}

/// Compute the evm-root for an ordered list of applied EVM transaction hashes.
pub fn evm_root(tick: u64, hashes: &[ethers_core::types::H256]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:evm-root:v1");
    hasher.update(&tick.to_le_bytes());
    hasher.update(&hashes.len().to_le_bytes());
    for h in hashes {
        hasher.update(h.as_bytes());
    }
    hasher.finalize().into()
}

/// Compute a deterministic root over a balance map.
pub fn balances_root(balances: &HashMap<AccountId, u128>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:balances-root:v1");
    let mut items: Vec<_> = balances.iter().collect();
    items.sort_by(|(a, _), (b, _)| a.0.cmp(&b.0));
    for (account, balance) in items {
        hasher.update(&account.0);
        hasher.update(&balance.to_le_bytes());
    }
    hasher.finalize().into()
}

/// The set of result roots that operators must agree on to form a quorum.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QuorumView {
    pub commutative_root: [u8; 32],
    pub stateful_root: [u8; 32],
    pub evm_root: [u8; 32],
    pub balances_root: [u8; 32],
    pub stake_root: [u8; 32],
    pub reward_root: [u8; 32],
}

impl From<&SynthesisCertificate> for QuorumView {
    fn from(cert: &SynthesisCertificate) -> Self {
        Self {
            commutative_root: cert.commutative_root,
            stateful_root: cert.stateful_root,
            evm_root: cert.evm_root,
            balances_root: cert.balances_root,
            stake_root: cert.stake_root,
            reward_root: cert.reward_root,
        }
    }
}

/// Reason a certificate was rejected and the operator slashed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlashingReason {
    InvalidSignature,
    UnstakedOperator,
    ConflictingCertificate,
}

/// Tracks observed synthesis certificates, detects equivocation, and tallies
/// stake-weighted agreement per synthesis tick.
#[derive(Debug, Default)]
pub struct CertificateTracker {
    by_operator_tick: dashmap::DashMap<(AccountId, u64), SynthesisCertificate>,
    /// Per-tick stake-weighted aggregation of certificates grouped by their
    /// result-root view.
    by_tick: dashmap::DashMap<u64, dashmap::DashMap<QuorumView, u128>>,
}

impl CertificateTracker {
    pub fn new() -> Self {
        Self {
            by_operator_tick: dashmap::DashMap::new(),
            by_tick: dashmap::DashMap::new(),
        }
    }

    /// Validate a certificate, register it, and slash the operator if it
    /// conflicts with a previously accepted certificate for the same tick.
    ///
    /// Returns `Ok(())` if the certificate is accepted (or identical to an
    /// existing one).  Returns `Err(SlashingReason)` if the certificate is
    /// invalid, from an unstaked operator, or conflicts with a prior one.
    pub fn apply(
        &self,
        cert: SynthesisCertificate,
        key_registry: &HashMap<AccountId, ed25519_dalek::VerifyingKey>,
        stake_checker: &dyn Fn(&AccountId) -> bool,
        stake_amount: &dyn Fn(&AccountId) -> u128,
        slash: &mut dyn FnMut(AccountId),
    ) -> Result<(), SlashingReason> {
        let Some(pk) = key_registry.get(&cert.operator) else {
            return Err(SlashingReason::InvalidSignature);
        };
        if !cert.verify(pk) {
            return Err(SlashingReason::InvalidSignature);
        }
        if !stake_checker(&cert.operator) {
            return Err(SlashingReason::UnstakedOperator);
        }

        let key = (cert.operator, cert.tick);
        match self.by_operator_tick.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if entry.get().signing_bytes() != cert.signing_bytes() {
                    slash(cert.operator);
                    return Err(SlashingReason::ConflictingCertificate);
                }
                // Identical certificate: idempotent.
                Ok(())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let stake = stake_amount(&cert.operator);
                let view = QuorumView::from(&cert);
                self.by_tick
                    .entry(cert.tick)
                    .or_default()
                    .entry(view)
                    .and_modify(|s| *s += stake)
                    .or_insert(stake);
                entry.insert(cert);
                Ok(())
            }
        }
    }

    /// Check whether any result-root view for `tick` has reached the stake
    /// threshold. Returns the winning view and its accumulated stake.
    pub fn check_quorum(
        &self,
        tick: u64,
        threshold: u128,
    ) -> Option<(QuorumView, u128)> {
        let groups = self.by_tick.get(&tick)?;
        groups
            .iter()
            .find(|e| *e.value() >= threshold)
            .map(|e| (e.key().clone(), *e.value()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::keys::KeyPair;

    #[test]
    fn certificate_signs_and_verifies() {
        let kp = KeyPair::generate();
        let cert = SynthesisCertificate::sign(
            &kp,
            7,
            10,
            3,
            1,
            [1u8; 32],
            [2u8; 32],
            [3u8; 32],
            [4u8; 32],
            [5u8; 32],
            [6u8; 32],
            1_000_000,
            1_700_000_000_000,
        );
        assert!(cert.verify(&kp.public_key()));
    }

    #[test]
    fn certificate_rejects_wrong_key() {
        let kp = KeyPair::generate();
        let other = KeyPair::generate();
        let cert = SynthesisCertificate::sign(&kp, 0, 0, 0, 0, [0u8; 32], [0u8; 32], [0u8; 32], [0u8; 32], [0u8; 32], [0u8; 32], 0, 0);
        assert!(!cert.verify(&other.public_key()));
    }

    #[test]
    fn balances_root_is_deterministic() {
        let mut balances = HashMap::new();
        balances.insert(AccountId([3u8; 32]), 300);
        balances.insert(AccountId([1u8; 32]), 100);
        balances.insert(AccountId([2u8; 32]), 200);
        let r1 = balances_root(&balances);
        let r2 = balances_root(&balances);
        assert_eq!(r1, r2);
    }
}
