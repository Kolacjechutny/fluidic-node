use crate::crypto::{DEFAULT_DEX_DOMAIN, DomainId};
use std::collections::HashMap;

/// Number of whole WAVE tokens required to reserve/register a new concurrency
/// domain. Paid once per domain and redistributed to operators/LPs.
pub const DOMAIN_RESERVATION_FEE_WAVE: u128 = 100;

/// Reservation fee in sub-units (precision-aware).
pub fn domain_reservation_fee_units() -> u128 {
    DOMAIN_RESERVATION_FEE_WAVE * crate::field::wave_field::WAVE_PRECISION
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderingMode {
    /// Causal, vector-clock DAG ordering (default for state-dependent value).
    /// Maps to the whitepaper's "causal" domain policy.
    Causal,
    /// First-in-first-out ordering across the domain.
    /// Maps to the whitepaper's "commutative" domain policy when combined with
    /// `commutative = true`.
    Commutative,
    /// Strict ordering: every stateful signal requires an explicit operator
    /// quorum certificate before it is applied. Maps to the whitepaper's
    /// "strict" domain policy.
    Strict,
}

/// How a concurrency domain charges for execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeePolicy {
    /// A flat fee in sub-units per signal.
    Flat(u128),
    /// A percentage fee in basis points of the transacted amount.
    Percentage(u64),
    /// No explicit fee; economic pressure comes solely from metabolic decay.
    MetabolicOnly,
}

/// A policy governing one concurrency domain.
///
/// Domains isolate namespaces of execution: a domain may permit commutative
/// signals, stateful signals, both, or neither, and may choose its own
/// finalization depth.  Unknown domains are rejected at ingest time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainPolicy {
    pub domain: DomainId,
    pub commutative: bool,
    pub stateful: bool,
    pub ordering: OrderingMode,
    pub finalization_depth: u64,
    /// Exponential metabolic decay constant λ for this domain, in parts-per-
    /// million per synthesis tick.  Each tick a balance retains
    /// `(1_000_000 - λ)/1_000_000` of its value, so
    /// `B(t) = B(0) * ((1_000_000 - λ)/1_000_000)^t`.  Must be strictly less
    /// than 1_000_000.
    pub metabolic_lambda_ppm: u64,
    /// How this domain charges fees for execution.
    pub fee_policy: FeePolicy,
}

impl DomainPolicy {
    /// The built-in DEX domain: both commutative and stateful signals, causal
    /// (DAG) ordering, a conservative finalization depth, the default DEX decay
    /// constant (λ = 20 ppm/tick), and no explicit fee beyond metabolic decay.
    pub fn dex_default() -> Self {
        Self {
            domain: DEFAULT_DEX_DOMAIN,
            commutative: true,
            stateful: true,
            ordering: OrderingMode::Causal,
            finalization_depth: 3,
            metabolic_lambda_ppm: crate::value::metabolic::DEFAULT_DEX_LAMBDA_PPM,
            fee_policy: FeePolicy::MetabolicOnly,
        }
    }

    /// Build a new domain policy. Validates invariants; returns `Err` if the
    /// policy is invalid (e.g. decay rate out of range).
    pub fn new(
        domain: DomainId,
        commutative: bool,
        stateful: bool,
        ordering: OrderingMode,
        finalization_depth: u64,
        metabolic_lambda_ppm: u64,
        fee_policy: FeePolicy,
    ) -> Result<Self, String> {
        if metabolic_lambda_ppm >= crate::value::metabolic::DECAY_DENOMINATOR {
            return Err(format!(
                "metabolic_lambda_ppm {} must be strictly less than {}",
                metabolic_lambda_ppm,
                crate::value::metabolic::DECAY_DENOMINATOR
            ));
        }
        if finalization_depth == 0 {
            return Err("finalization_depth must be > 0".to_string());
        }
        Ok(Self {
            domain,
            commutative,
            stateful,
            ordering,
            finalization_depth,
            metabolic_lambda_ppm,
            fee_policy,
        })
    }

    /// Convenience builder for domains that want a different metabolic decay
    /// constant λ (in parts-per-million per tick).
    pub fn with_metabolic_lambda(mut self, rate_ppm: u64) -> Self {
        self.metabolic_lambda_ppm = rate_ppm;
        self
    }
}

/// Registry of all known concurrency domains.
#[derive(Clone, Debug, Default)]
pub struct DomainRegistry {
    domains: HashMap<DomainId, DomainPolicy>,
}

impl DomainRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            domains: HashMap::new(),
        };
        reg.register(DomainPolicy::dex_default());
        reg
    }

    pub fn register(&mut self, policy: DomainPolicy) {
        self.domains.insert(policy.domain, policy);
    }

    pub fn get(&self, domain: &DomainId) -> Option<&DomainPolicy> {
        self.domains.get(domain)
    }

    pub fn contains(&self, domain: &DomainId) -> bool {
        self.domains.contains_key(domain)
    }

    /// All registered domain policies, sorted by domain id for stable output.
    pub fn all(&self) -> Vec<DomainPolicy> {
        let mut policies: Vec<DomainPolicy> = self.domains.values().cloned().collect();
        policies.sort_by(|a, b| a.domain.cmp(&b.domain));
        policies
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_seeds_dex_domain() {
        let reg = DomainRegistry::new();
        let policy = reg.get(&DEFAULT_DEX_DOMAIN).unwrap();
        assert!(policy.commutative);
        assert!(policy.stateful);
        assert_eq!(policy.ordering, OrderingMode::Causal);
        assert_eq!(policy.finalization_depth, 3);
        assert_eq!(
            policy.metabolic_lambda_ppm,
            crate::value::metabolic::DEFAULT_DEX_LAMBDA_PPM
        );
        assert_eq!(policy.fee_policy, FeePolicy::MetabolicOnly);
    }
}
