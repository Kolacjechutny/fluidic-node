use fluidic::crypto::keys::KeyPair;

fn main() {
    for name in ["mesh-node-0", "mesh-node-1", "mesh-node-2"] {
        // Match the derivation in mesh_node.rs: extract trailing number and use
        // its little-endian bytes as the first 8 bytes of the 32-byte seed.
        let n: u64 = name
            .rsplit_once('-')
            .and_then(|(_, suffix)| suffix.parse().ok())
            .expect("name must end with a number");
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&n.to_le_bytes());
        let kp = KeyPair::from_seed(&seed);
        println!("{} -> {}", name, kp.account_id());
    }
}
