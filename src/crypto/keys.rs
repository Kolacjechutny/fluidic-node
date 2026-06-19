use blake3::Hasher;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::fmt;

/// 32-byte unique account identifier derived by hashing the Ed25519 public key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AccountId(pub [u8; 32]);

impl AccountId {
    pub fn from_public_key(public_key: &VerifyingKey) -> Self {
        Self(blake3::hash(public_key.as_bytes()).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// A wave address is a domain-separated hash of an account public key.
/// It represents the coordinate at which a phase-shift is injected into the field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WaveAddress(pub [u8; 32]);

impl WaveAddress {
    pub fn from_account_id(id: &AccountId) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(b"fluidic:wave-address:v1");
        hasher.update(&id.0);
        Self(hasher.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for WaveAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// In-memory Ed25519 keypair used to sign phase-shifts.
pub struct KeyPair {
    signing_key: SigningKey,
}

impl KeyPair {
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        Self { signing_key }
    }

    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Self {
            signing_key: SigningKey::from_bytes(seed),
        }
    }

    pub fn account_id(&self) -> AccountId {
        AccountId::from_public_key(&self.signing_key.verifying_key())
    }

    pub fn wave_address(&self) -> WaveAddress {
        WaveAddress::from_account_id(&self.account_id())
    }

    pub fn public_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    /// Verify a detached signature against a public key and message.
    pub fn verify(public_key: &VerifyingKey, message: &[u8], signature: &Signature) -> bool {
        public_key.verify(message, signature).is_ok()
    }
}

impl Clone for KeyPair {
    fn clone(&self) -> Self {
        Self {
            signing_key: SigningKey::from_bytes(&self.signing_key.to_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_generation_and_signing() {
        let kp = KeyPair::generate();
        let msg = b"fluidic test message";
        let sig = kp.sign(msg);
        assert!(KeyPair::verify(&kp.public_key(), msg, &sig));
    }

    #[test]
    fn account_id_is_deterministic() {
        let seed = [42u8; 32];
        let kp1 = KeyPair::from_seed(&seed);
        let kp2 = KeyPair::from_seed(&seed);
        assert_eq!(kp1.account_id(), kp2.account_id());
        assert_eq!(kp1.wave_address(), kp2.wave_address());
    }
}
