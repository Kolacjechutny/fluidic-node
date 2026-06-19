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
}

impl DomainPolicy {
    /// The built-in DEX domain: both commutative and stateful signals,
    /// DAG ordering, and a conservative finalization depth.
    pub fn dex_default() -> Self {
        Self {
            domain: DEFAULT_DEX_DOMAIN,
            commutative: true,
            stateful: true,
            ordering: OrderingMode::Dag,
            finalization_depth: 3,
        }
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
    }
}
