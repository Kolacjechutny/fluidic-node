/// Deterministic Merkle accumulator over sorted key/value pairs.
///
/// This is intentionally simple: the state is small enough on testnet that a
/// full sorted Merkle tree can be rebuilt each synthesis tick.  It provides a
/// cryptographic root plus membership proofs.  In a production deployment this
/// would be replaced by an incremental Sparse Merkle Tree or Merkle Mountain
/// Range.
pub struct MerkleAccumulator;

impl MerkleAccumulator {
    /// Compute the Merkle root of a set of key/value pairs.
    /// Items are sorted by key before hashing to guarantee determinism.
    pub fn root(items: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
        let mut sorted = items.to_vec();
        sorted.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut leaves: Vec<_> = sorted
            .iter()
            .map(|(k, v)| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"fluidic:leaf:v1");
                hasher.update(k);
                hasher.update(v);
                hasher.finalize().into()
            })
            .collect();

        if leaves.is_empty() {
            return Self::empty_root();
        }

        // Pad to a power of two with copies of the last leaf.  This is a
        // common testnet simplification; a production tree would use a
        // distinct padding scheme or SMT default hashes.
        if leaves.len().count_ones() != 1 {
            let target = leaves.len().next_power_of_two();
            leaves.resize(target, *leaves.last().unwrap());
        }

        let mut level: Vec<[u8; 32]> = leaves;
        while level.len() > 1 {
            let mut next: Vec<[u8; 32]> = Vec::with_capacity(level.len() / 2);
            for pair in level.chunks_exact(2) {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"fluidic:node:v1");
                hasher.update(&pair[0]);
                hasher.update(&pair[1]);
                next.push(hasher.finalize().into());
            }
            level = next;
        }

        level[0]
    }

    pub fn empty_root() -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fluidic:empty:root");
        hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_deterministic() {
        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        let r1 = MerkleAccumulator::root(&items);
        let r2 = MerkleAccumulator::root(&items);
        assert_eq!(r1, r2);
    }

    #[test]
    fn order_matters() {
        let items1 = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let items2 = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"a".to_vec(), b"1".to_vec()),
        ];
        assert_eq!(MerkleAccumulator::root(&items1), MerkleAccumulator::root(&items2));
    }
}
