use serde::{Deserialize, Serialize};
use std::fmt;

/// Dimensionality of a frequency coordinate in the wave-field.
pub const COORD_DIM: usize = 4;

/// A coordinate in the multi-dimensional state wave-field.
/// Each component is a frequency index into the NTT spectrum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Coordinate {
    pub components: [u64; COORD_DIM],
}

impl Coordinate {
    pub fn zero() -> Self {
        Self {
            components: [0; COORD_DIM],
        }
    }

    pub fn from_scalar(scalar: u64) -> Self {
        let mut components = [0; COORD_DIM];
        components[0] = scalar;
        Self { components }
    }

    pub fn new(components: [u64; COORD_DIM]) -> Self {
        Self { components }
    }

    /// Deterministic byte representation used for signing and hashing.
    pub fn to_bytes(&self) -> [u8; COORD_DIM * 8] {
        let mut buf = [0u8; COORD_DIM * 8];
        for (i, c) in self.components.iter().enumerate() {
            buf[i * 8..(i + 1) * 8].copy_from_slice(&c.to_le_bytes());
        }
        buf
    }

    /// Map the coordinate into a single NTT bin index modulo a power-of-two field size.
    pub fn to_ntt_index(&self, size: usize) -> usize {
        let mut acc: u64 = 0;
        for (i, c) in self.components.iter().enumerate() {
            // Mix components with a simple weighted sum; keeps mapping deterministic.
            acc = acc.wrapping_add(c.wrapping_mul(2654435761_u64.wrapping_add(i as u64)));
        }
        (acc as usize) % size
    }
}

impl Default for Coordinate {
    fn default() -> Self {
        Self::zero()
    }
}

impl fmt::Display for Coordinate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, c) in self.components.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", c)?;
        }
        write!(f, "]")
    }
}

/// A frequency vector attached to an account. It is a point in the state-field spectrum
/// and is updated by commutative phase-shifts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrequencyVector {
    pub amplitudes: [u64; COORD_DIM],
}

impl FrequencyVector {
    pub fn zero() -> Self {
        Self {
            amplitudes: [0; COORD_DIM],
        }
    }

    pub fn from_scalar(scalar: u64) -> Self {
        let mut amplitudes = [0; COORD_DIM];
        amplitudes[0] = scalar;
        Self { amplitudes }
    }

    pub fn add(&mut self, delta: &[i64; COORD_DIM]) {
        for (a, d) in self.amplitudes.iter_mut().zip(delta.iter()) {
            *a = (*a as i64).saturating_add(*d).max(0) as u64;
        }
    }
}

impl Default for FrequencyVector {
    fn default() -> Self {
        Self::zero()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinate_to_bytes_roundtrip() {
        let c = Coordinate::new([1, 2, 3, 4]);
        let bytes = c.to_bytes();
        let mut components = [0u64; COORD_DIM];
        for i in 0..COORD_DIM {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes[i * 8..(i + 1) * 8]);
            components[i] = u64::from_le_bytes(arr);
        }
        assert_eq!(c.components, components);
    }

    #[test]
    fn ntt_index_in_bounds() {
        let c = Coordinate::new([12345, 67890, 111, 222]);
        assert!(c.to_ntt_index(1024) < 1024);
    }
}
