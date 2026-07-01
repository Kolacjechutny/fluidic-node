use crate::crypto::{DEFAULT_DEX_DOMAIN, DomainId};
use std::collections::HashMap;

/// How stateful signals within a concurrency domain are ordered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderingMode {
    /// Causal, vector-clock DAG ordering (default for state-dependent value).
    Dag,
    /// First-in-first-out ordering across the domain.
    Fifo,
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
    /// The built-in DEX domain: both commutative and stateful signals, DAG
    /// ordering, a conservative finalization depth, the default DEX decay
    /// constant (λ = 20 ppm/tick), and no explicit fee beyond metabolic decay.
    pub fn dex_default() -> Self {
        Self {
            domain: DEFAULT_DEX_DOMAIN,
            commutative: true,
            stateful: true,
            ordering: OrderingMode::Dag,
            finalization_depth: 3,
            metabolic_lambda_ppm: crate::value::metabolic::DEFAULT_DEX_LAMBDA_PPM,
            fee_policy: FeePolicy::MetabolicOnly,
        }
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
        assert_eq!(policy.ordering, OrderingMode::Dag);
        assert_eq!(policy.finalization_depth, 3);
        assert_eq!(
            policy.metabolic_lambda_ppm,
            crate::value::metabolic::DEFAULT_DEX_LAMBDA_PPM
        );
        assert_eq!(policy.fee_policy, FeePolicy::MetabolicOnly);
    }
}
