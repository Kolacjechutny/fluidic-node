//! Bridge support for connecting Fluidic domains to external systems.
//!
//! Bridges observe finality on an external chain, mint wrapped assets on
//! Fluidic, and emit outbound Signals when Fluidic value leaves for the
//! external chain. This module provides the trait definitions and a stub
//! Ethereum adapter. Real chain connectivity (RPC/WebSocket listeners,
//! multisig verification, finality thresholds) is intentionally left to
//! downstream integrations so the core runtime stays chain-agnostic.

use crate::consensus::domain::{DomainPolicy, OrderingMode};
use crate::crypto::{AccountId, DomainId, Signal, StatefulShift, VectorClock};
use crate::field::wave_field::Balance;

/// Unique identifier for an external chain connected by a bridge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalChain {
    Ethereum,
    Solana,
}

/// Event observed on an external chain that should be reflected on Fluidic.
#[derive(Clone, Debug)]
pub struct BridgeInboundEvent {
    /// External chain that produced the event.
    pub chain: ExternalChain,
    /// External transaction or log identifier.
    pub external_ref: String,
    /// Recipient Fluidic account.
    pub recipient: AccountId,
    /// Token account on Fluidic that should be credited.
    pub token_account: AccountId,
    /// Amount in Fluidic sub-units.
    pub amount: u128,
}

/// Request to move value from Fluidic to an external chain.
#[derive(Clone, Debug)]
pub struct BridgeOutboundRequest {
    pub chain: ExternalChain,
    pub sender: AccountId,
    pub token_account: AccountId,
    pub amount: u128,
    pub external_recipient: String,
}

/// A bridge adapter observes one external chain and translates its events into
/// Fluidic Signals. Implementations are chain-specific.
pub trait BridgeAdapter: Send + Sync {
    /// Human-readable name of this bridge instance.
    fn name(&self) -> &str;

    /// Poll for newly finalized inbound events. In a real adapter this would
    /// query an external RPC; the stub returns an empty vector.
    fn poll_inbound(&mut self,
        _since_external_ref: Option<&str>,
    ) -> Result<Vec<BridgeInboundEvent>, String> {
        Ok(Vec::new())
    }

    /// Submit an outbound request to the external chain. The stub always
    /// returns an error because there is no real chain connection.
    fn submit_outbound(
        &mut self,
        _request: BridgeOutboundRequest,
    ) -> Result<String, String> {
        Err("bridge adapter is a stub; no external chain connected".to_string())
    }
}

/// A bridge domain policy preset: strict ordering, no metabolic decay on the
/// bridge token account, and a deep finalization threshold.
pub fn bridge_domain_policy(domain: DomainId) -> DomainPolicy {
    DomainPolicy::new(
        domain,
        false,
        true,
        OrderingMode::Strict,
        10,
        0, // bridge token accounts should not decay
        crate::consensus::domain::FeePolicy::Flat(
            crate::field::wave_field::WAVE_PRECISION,
        ),
    )
    .expect("bridge domain policy is valid")
}

/// Stub Ethereum bridge adapter. Logs observed events but never touches a real
/// chain.
pub struct EthereumBridgeAdapter {
    name: String,
}

impl EthereumBridgeAdapter {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl BridgeAdapter for EthereumBridgeAdapter {
    fn name(&self) -> &str {
        &self.name
    }
}

/// Bridge registry tracks active adapters and exposes a single `tick` hook for
/// the node to poll all bridges.
pub struct BridgeRegistry {
    adapters: Vec<Box<dyn BridgeAdapter>>,
}

impl BridgeRegistry {
    pub fn new() -> Self {
        Self { adapters: Vec::new() }
    }

    pub fn register(&mut self,
        adapter: Box<dyn BridgeAdapter>,
    ) {
        self.adapters.push(adapter);
    }

    /// Poll every adapter for inbound events. Returns all events and any
    /// adapter-specific errors.
    pub fn poll_all(
        &mut self,
    ) -> (Vec<BridgeInboundEvent>, Vec<(String, String)>) {
        let mut events = Vec::new();
        let mut errors = Vec::new();
        for adapter in &mut self.adapters {
            match adapter.poll_inbound(None) {
                Ok(mut e) => events.append(&mut e),
                Err(err) => errors.push((adapter.name().to_string(), err)),
            }
        }
        (events, errors)
    }
}

impl Default for BridgeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a stateful Signal that mints wrapped value into a Fluidic token
/// account. The caller must sign and ingest the returned Signal.
pub fn build_mint_signal(
    event: &BridgeInboundEvent,
    domain: DomainId,
    sender: AccountId,
    vector_clock: VectorClock,
    nonce: u64,
) -> Signal {
    // A bridge mint is represented as a transfer from the bridge's sender
    // account to the recipient's token account. The token account is marked
    // non-decaying by the bridge setup.
    Signal::Stateful(StatefulShift {
        domain,
        from: sender,
        to: event.token_account,
        amount: event.amount,
        vector_clock,
        predecessors: Vec::new(),
        nonce,
        timestamp_ns: 0,
        first_seen_at_ns: 0,
        signature: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_registry_polls_adapters() {
        let mut registry = BridgeRegistry::new();
        registry.register(Box::new(EthereumBridgeAdapter::new("eth-stub")));
        let (events, errors) = registry.poll_all();
        assert!(events.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn bridge_domain_policy_is_strict() {
        let domain = [99u8; 32];
        let policy = bridge_domain_policy(domain);
        assert_eq!(policy.domain, domain);
        assert!(matches!(policy.ordering, OrderingMode::Strict));
        assert!(!policy.commutative);
        assert!(policy.stateful);
    }
}
