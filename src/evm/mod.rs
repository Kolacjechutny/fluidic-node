use crate::crypto::AccountId;
use ethers_core::types::{Address as EvmAddress, H256, U256};
use revm::InMemoryDB;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub mod executor;
pub use executor::{EvmError, EvmExecutor};

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn latency_ms(finalized_at_ns: u64, first_seen_at_ns: u64) -> f64 {
    if finalized_at_ns > first_seen_at_ns {
        ((finalized_at_ns - first_seen_at_ns) as f64) / 1_000_000.0
    } else {
        0.0
    }
}

/// Chain ID used by the Fluidic EVM RPC gateway.
pub const FLUIDIC_EVM_CHAIN_ID: u64 = 0xF1D1C;

/// Status of an EVM transaction observed by the mesh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvmTxStatus {
    Pending,
    Success,
    Failed(String),
}

/// A single EVM log entry exposed through the RPC interface.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvmLog {
    pub address: EvmAddress,
    pub topics: Vec<H256>,
    pub data: Vec<u8>,
}

/// A receipt for an EVM transaction that has been synthesized into a tick.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvmReceipt {
    pub transaction_hash: H256,
    pub transaction_index: u64,
    pub block_number: u64,
    pub block_hash: H256,
    pub from: EvmAddress,
    pub to: Option<EvmAddress>,
    pub contract_address: Option<EvmAddress>,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub effective_gas_price: U256,
    pub status: u64,
    pub logs: Vec<EvmLog>,
}

/// A decoded, verified EVM transaction ready for synthesis.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvmTransaction {
    pub hash: H256,
    pub from: EvmAddress,
    pub to: Option<EvmAddress>,
    pub value: U256,
    pub gas_price: U256,
    pub gas_limit: u64,
    pub data: Vec<u8>,
    pub nonce: u64,
    /// Original signed RLP bytes. Kept so `eth_getTransactionByHash` can return
    /// the raw transaction.
    pub raw: Vec<u8>,
    /// Wall-clock time when the raw transaction was first accepted by this node.
    pub first_seen_at_ns: u64,
}

impl EvmTransaction {
    /// Decode a raw signed Ethereum transaction and recover the sender address.
    pub fn decode_raw(raw: &[u8]) -> Result<Self, String> {
        let tx: ethers_core::types::Transaction = ethers_core::utils::rlp::decode(raw)
            .map_err(|e| format!("invalid RLP transaction: {}", e))?;

        let chain_id = tx.chain_id.map(|c| c.as_u64()).unwrap_or(0);
        if chain_id != 0 && chain_id != FLUIDIC_EVM_CHAIN_ID {
            return Err(format!(
                "wrong chain id: expected {} got {}",
                FLUIDIC_EVM_CHAIN_ID, chain_id
            ));
        }

        let from = tx
            .recover_from()
            .map_err(|e| format!("failed to recover sender: {}", e))?;

        Ok(Self {
            hash: tx.hash(),
            from,
            to: tx.to,
            value: tx.value,
            gas_price: tx.gas_price.unwrap_or_default(),
            gas_limit: if tx.gas.is_zero() { 1_000_000 } else { tx.gas.as_u64() },
            data: tx.input.to_vec(),
            nonce: tx.nonce.as_u64(),
            raw: raw.to_vec(),
            first_seen_at_ns: 0,
        })
    }

    /// Derive a Fluidic account from the sender's EVM address.
    pub fn fluidic_sender(&self) -> AccountId {
        evm_address_to_fluidic(&self.from)
    }

    /// Derive a Fluidic recipient from an EVM address, if any.
    pub fn fluidic_recipient(&self) -> Option<AccountId> {
        self.to.map(|a| evm_address_to_fluidic(&a))
    }
}

/// Derive a Fluidic account deterministically from a 20-byte EVM address.
pub fn evm_address_to_fluidic(addr: &EvmAddress) -> AccountId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:evm-account:v1");
    hasher.update(addr.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(hasher.finalize().as_bytes());
    AccountId(out)
}

/// In-memory pool for EVM transactions.
#[derive(Debug, Default)]
pub struct EvmPool {
    /// Pending transactions waiting for the next synthesis tick.
    pending: Vec<EvmTransaction>,
    /// Observed transaction statuses keyed by Ethereum tx hash.
    pub(crate) statuses: HashMap<H256, EvmTxStatus>,
    /// Last processed nonce per sender EVM address.
    pub(crate) nonces: BTreeMap<EvmAddress, u64>,
    /// Persistent EVM state carried across synthesis ticks. Holds account
    /// balances, nonces, contract bytecodes, and contract storage.
    pub db: InMemoryDB,
    /// Finalized receipts keyed by transaction hash.
    pub(crate) receipts: HashMap<H256, EvmReceipt>,
    /// Index of transactions keyed by hash (for `eth_getTransactionByHash`).
    pub(crate) transactions: HashMap<H256, EvmTransaction>,
}

impl EvmPool {
    pub fn new() -> Self {
        Self {
            db: InMemoryDB::default(),
            ..Default::default()
        }
    }

    /// Queue a verified transaction for synthesis.
    pub fn submit(&mut self, mut tx: EvmTransaction) -> Result<(), String> {
        if self.statuses.contains_key(&tx.hash) {
            return Ok(());
        }
        if tx.first_seen_at_ns == 0 {
            tx.first_seen_at_ns = now_ns();
        }
        self.statuses.insert(tx.hash, EvmTxStatus::Pending);
        self.transactions.insert(tx.hash, tx.clone());
        self.pending.push(tx);
        Ok(())
    }

    /// Drain pending transactions, apply them in sender-nonce order using a
    /// real EVM interpreter against the persistent EVM database, and return the
    /// number successfully processed, total latency observed (ms), and the
    /// hashes of applied transactions.
    pub fn synthesize(
        &mut self,
        balances: &mut HashMap<AccountId, u128>,
        finalized_at_ns: u64,
        tick: u64,
    ) -> (usize, f64, Vec<H256>) {
        // Sort by (sender, nonce) so each sender's transactions are ordered.
        self.pending
            .sort_by(|a, b| a.from.cmp(&b.from).then(a.nonce.cmp(&b.nonce)));

        // Resume from the EVM state left by the previous synthesis tick.
        let mut executor = crate::evm::EvmExecutor::with_db(self.db.clone());
        executor.prepare(&self.pending, balances, &self.nonces);

        let mut applied = 0usize;
        let mut total_latency_ms = 0.0f64;
        let mut applied_hashes = Vec::new();
        let mut cumulative_gas = 0u64;
        for (idx, tx) in self.pending.drain(..).enumerate() {
            let expected = self.nonces.get(&tx.from).copied().unwrap_or(0);
            if tx.nonce != expected {
                self.statuses.insert(
                    tx.hash,
                    EvmTxStatus::Failed(format!("invalid nonce: expected {}", expected)),
                );
                continue;
            }

            let mut contract_address = None;
            let mut gas_used = 0u64;
            let mut status = 0u64;
            let mut logs = Vec::new();

            match executor.execute(&tx) {
                Ok(result) => {
                    gas_used = revm::primitives::ExecutionResult::gas_used(&result);
                    cumulative_gas += gas_used;
                    match result {
                        revm::primitives::ExecutionResult::Success {
                            output: _,
                            logs: result_logs,
                            ..
                        } => {
                            logs = result_logs
                                .into_iter()
                                .map(|l| EvmLog {
                                    address: EvmAddress::from_slice(l.address.as_ref()),
                                    topics: l
                                        .topics()
                                        .iter()
                                        .map(|t| H256::from(t.0))
                                        .collect(),
                                    data: l.data.data.to_vec(),
                                })
                                .collect();
                            status = 1;
                            if tx.to.is_none() {
                                contract_address = Some(ethers_core::utils::get_contract_address(
                                    tx.from,
                                    U256::from(tx.nonce),
                                ));
                            }
                        }
                        revm::primitives::ExecutionResult::Revert { output, .. } => {
                            self.statuses.insert(
                                tx.hash,
                                EvmTxStatus::Failed(format!(
                                    "evm execution reverted: 0x{}",
                                    hex::encode(output)
                                )),
                            );
                        }
                        revm::primitives::ExecutionResult::Halt { reason, .. } => {
                            self.statuses.insert(
                                tx.hash,
                                EvmTxStatus::Failed(format!("evm execution halted: {:?}", reason)),
                            );
                        }
                    }
                }
                Err(e) => {
                    self.statuses
                        .insert(tx.hash, EvmTxStatus::Failed(format!("{:?}", e)));
                }
            }

            if status == 1 {
                self.nonces.insert(tx.from, expected + 1);
                self.statuses.insert(tx.hash, EvmTxStatus::Success);
                total_latency_ms += latency_ms(finalized_at_ns, tx.first_seen_at_ns);
                applied_hashes.push(tx.hash);
                applied += 1;
            }

            let block_hash = block_hash_for(tick);
            self.receipts.insert(
                tx.hash,
                EvmReceipt {
                    transaction_hash: tx.hash,
                    transaction_index: idx as u64,
                    block_number: tick,
                    block_hash,
                    from: tx.from,
                    to: tx.to,
                    contract_address,
                    gas_used,
                    cumulative_gas_used: cumulative_gas,
                    effective_gas_price: tx.gas_price,
                    status,
                    logs,
                },
            );
        }

        executor.sync_balances_back(balances);
        // Persist the updated EVM state (balances, nonces, code, storage).
        self.db = executor.into_db();
        (applied, total_latency_ms, applied_hashes)
    }

    pub fn status(&self, hash: &H256) -> Option<EvmTxStatus> {
        self.statuses.get(hash).cloned()
    }

    pub fn receipt(&self, hash: &H256) -> Option<&EvmReceipt> {
        self.receipts.get(hash)
    }

    pub fn transaction(&self, hash: &H256) -> Option<&EvmTransaction> {
        self.transactions.get(hash)
    }

    /// Return all stored logs, optionally filtered by address and/or up to four
    /// topics.
    pub fn logs(&self, address: Option<EvmAddress>, topics: &[Option<H256>]) -> Vec<EvmLog> {
        self.receipts
            .values()
            .flat_map(|r| r.logs.clone())
            .filter(|log| {
                if let Some(addr) = address {
                    if log.address != addr {
                        return false;
                    }
                }
                for (i, topic) in topics.iter().enumerate() {
                    if let Some(expected) = topic {
                        if log.topics.get(i) != Some(expected) {
                            return false;
                        }
                    }
                }
                true
            })
            .collect()
    }

    /// Return the next valid nonce for an EVM address.
    pub fn nonce(&self, address: &EvmAddress) -> u64 {
        self.nonces.get(address).copied().unwrap_or(0)
    }
}

/// Deterministic block hash for a synthesis tick.
pub fn block_hash_for(tick: u64) -> H256 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fluidic:block:");
    hasher.update(&tick.to_le_bytes());
    let mut arr = [0u8; 32];
    arr.copy_from_slice(hasher.finalize().as_bytes());
    H256::from(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers_core::types::TransactionRequest;
    use ethers_core::types::transaction::eip2718::TypedTransaction;
    use ethers_signers::{LocalWallet, Signer};

    #[tokio::test]
    async fn decodes_signed_transfer() {
        let wallet: LocalWallet = "0x0123456789012345678901234567890123456789012345678901234567890123"
            .parse()
            .unwrap();
        let to: EvmAddress = "0x3535353535353535353535353535353535353535".parse().unwrap();
        let tx: TypedTransaction = TransactionRequest::new()
            .to(to)
            .value(U256::from(1_000_000_000_000_000_000u128))
            .chain_id(FLUIDIC_EVM_CHAIN_ID)
            .nonce(0)
            .gas_price(1)
            .gas(21_000)
            .into();

        let signature = wallet.sign_transaction(&tx).await.unwrap();
        let raw = tx.rlp_signed(&signature);

        let decoded = EvmTransaction::decode_raw(&raw).unwrap();
        assert_eq!(decoded.from, wallet.address());
        assert_eq!(decoded.to, Some(to));
        assert_eq!(decoded.value, U256::from(1_000_000_000_000_000_000u128));
    }

    #[tokio::test]
    async fn executes_signed_transfer_with_revm() {
        let wallet: LocalWallet = "0x0123456789012345678901234567890123456789012345678901234567890123"
            .parse()
            .unwrap();
        let to: EvmAddress = "0x3535353535353535353535353535353535353535".parse().unwrap();
        let tx: TypedTransaction = TransactionRequest::new()
            .to(to)
            .value(U256::from(1_000_000_000_000_000_000u128))
            .chain_id(FLUIDIC_EVM_CHAIN_ID)
            .nonce(0)
            .gas_price(1)
            .gas(21_000)
            .into();

        let signature = wallet.sign_transaction(&tx).await.unwrap();
        let raw = tx.rlp_signed(&signature);
        let decoded = EvmTransaction::decode_raw(&raw).unwrap();
        let tx_hash = decoded.hash;

        let mut pool = EvmPool::new();
        let mut balances = std::collections::HashMap::new();
        balances.insert(evm_address_to_fluidic(&wallet.address()), 10_000_000_000_000_000_000u128);

        pool.submit(decoded).unwrap();
        let (applied, _, _) = pool.synthesize(&mut balances, now_ns(), 1);
        if applied != 1 {
            eprintln!("EVM tx status: {:?}", pool.status(&tx_hash));
        }
        assert_eq!(applied, 1);
        assert_eq!(pool.status(&tx_hash), Some(EvmTxStatus::Success));
        assert!(pool.receipt(&tx_hash).is_some());
        assert_eq!(pool.receipt(&tx_hash).unwrap().status, 1);
        assert_eq!(
            balances.get(&evm_address_to_fluidic(&to)).copied().unwrap_or(0),
            1_000_000_000_000_000_000u128
        );
    }

    #[test]
    fn rejects_wrong_chain_id() {
        let raw_hex = "0xf86c808504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83";
        let raw = hex::decode(raw_hex.trim_start_matches("0x")).unwrap();
        assert!(EvmTransaction::decode_raw(&raw).is_err());
    }
}
