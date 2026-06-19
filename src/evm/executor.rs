use crate::crypto::AccountId;
use crate::evm::{FLUIDIC_EVM_CHAIN_ID, evm_address_to_fluidic, EvmTransaction};
use ethers_core::types::{Address as EvmAddress, U256 as EthersU256};
use revm::{
    Database, Evm,
    InMemoryDB,
    primitives::{AccountInfo, Address, Bytes, TxKind, U256 as RevmU256},
};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Error running an EVM transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvmError {
    Revm(String),
    InvalidTransaction(String),
}

/// Real EVM executor backed by revm.
pub struct EvmExecutor {
    db: InMemoryDB,
    touched: HashSet<EvmAddress>,
}

impl Default for EvmExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmExecutor {
    pub fn new() -> Self {
        Self {
            db: InMemoryDB::default(),
            touched: HashSet::new(),
        }
    }

    /// Seed an EVM account with a balance and nonce before execution.
    pub fn seed_account(&mut self, addr: EvmAddress, balance: u128, nonce: u64) {
        let info = AccountInfo {
            balance: to_revm_u256(balance.into()),
            nonce,
            code_hash: Default::default(),
            code: None,
        };
        self.db.insert_account_info(to_revm_addr(addr), info);
        self.touched.insert(addr);
    }

    /// Prepare the executor by seeding all addresses that appear in the
    /// pending transaction list with their current Fluidic balances.
    pub fn prepare(
        &mut self,
        txs: &[EvmTransaction],
        balances: &HashMap<AccountId, u128>,
        nonces: &BTreeMap<EvmAddress, u64>,
    ) {
        for tx in txs {
            for addr in [Some(tx.from), tx.to].into_iter().flatten() {
                let fluidic = evm_address_to_fluidic(&addr);
                let balance = balances.get(&fluidic).copied().unwrap_or(0);
                let nonce = nonces.get(&addr).copied().unwrap_or(0);
                self.seed_account(addr, balance, nonce);
            }
        }
    }

    /// Execute a single verified transaction and commit the resulting state to
    /// the in-memory database. Returns the revm execution result.
    pub fn execute(
        &mut self,
        tx: &EvmTransaction,
    ) -> Result<revm::primitives::ExecutionResult, EvmError> {
        self.touched.insert(tx.from);
        if let Some(to) = tx.to {
            self.touched.insert(to);
        }

        let mut evm = Evm::builder()
            .with_db(&mut self.db)
            .modify_cfg_env(|cfg| {
                cfg.chain_id = FLUIDIC_EVM_CHAIN_ID;
            })
            .modify_tx_env(|env| {
                env.caller = to_revm_addr(tx.from);
                env.gas_limit = 1_000_000;
                env.gas_price = to_revm_u256(tx.gas_price);
                env.transact_to = match tx.to {
                    Some(addr) => TxKind::Call(to_revm_addr(addr)),
                    None => TxKind::Create,
                };
                env.value = to_revm_u256(tx.value);
                env.data = Bytes::copy_from_slice(&tx.data);
                env.nonce = Some(tx.nonce);
            })
            .build();

        evm.transact_commit()
            .map_err(|e| EvmError::Revm(format!("{:?}", e)))
    }

    /// Read the current EVM balances of all touched accounts and write them
    /// back into the shared Fluidic balance table.
    pub fn sync_balances_back(&mut self, balances: &mut HashMap<AccountId, u128>) {
        for addr in &self.touched {
            let revm_addr = to_revm_addr(*addr);
            let balance = self
                .db
                .basic(revm_addr)
                .ok()
                .flatten()
                .map(|info| info.balance.to::<u128>())
                .unwrap_or(0u128);
            let fluidic = evm_address_to_fluidic(addr);
            balances.insert(fluidic, balance);
        }
    }
}

fn to_revm_addr(addr: EvmAddress) -> Address {
    Address::from_slice(addr.as_bytes())
}

fn to_revm_u256(value: EthersU256) -> RevmU256 {
    let mut bytes = [0u8; 32];
    value.to_big_endian(&mut bytes);
    RevmU256::from_be_bytes(bytes)
}
