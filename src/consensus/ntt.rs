/// Number Theoretic Transform implementation over the prime field
/// P = 998244353 = 119 * 2^23 + 1, a common NTT-friendly prime.
/// Primitive root g = 3.
pub const NTT_MODULUS: u64 = 998_244_353;
pub const NTT_PRIMITIVE_ROOT: u64 = 3;

/// Precomputed NTT engine for a fixed transform size.
pub struct NttEngine {
    pub size: usize,
    roots: Vec<u64>,
    inv_roots: Vec<u64>,
    inv_size: u64,
}

impl NttEngine {
    pub fn new(size: usize) -> Self {
        assert!(
            size.is_power_of_two() && size >= 2,
            "NTT size must be a power of two >= 2"
        );
        assert!(
            ((NTT_MODULUS - 1) as usize) % size == 0,
            "size must divide P-1"
        );

        let exp = ((NTT_MODULUS - 1) as usize / size) as u64;
        let root = mod_pow(NTT_PRIMITIVE_ROOT, exp, NTT_MODULUS);
        let inv_root = mod_inv(root, NTT_MODULUS);

        let mut roots = vec![1u64; size];
        let mut inv_roots = vec![1u64; size];
        for i in 1..size {
            roots[i] = mul_mod(roots[i - 1], root, NTT_MODULUS);
            inv_roots[i] = mul_mod(inv_roots[i - 1], inv_root, NTT_MODULUS);
        }

        Self {
            size,
            roots,
            inv_roots,
            inv_size: mod_inv(size as u64, NTT_MODULUS),
        }
    }

    /// In-place forward Number Theoretic Transform.
    pub fn ntt(&self, a: &mut [u64]) {
        assert_eq!(a.len(), self.size);
        bit_reverse_permute(a);
        let mut len = 2usize;
        while len <= self.size {
            let half = len >> 1;
            let step = self.size / len;
            for i in (0..self.size).step_by(len) {
                let mut j = 0usize;
                while j < half {
                    let w = self.roots[j * step];
                    let u = a[i + j];
                    let v = mul_mod(a[i + j + half], w, NTT_MODULUS);
                    a[i + j] = add_mod(u, v, NTT_MODULUS);
                    a[i + j + half] = sub_mod(u, v, NTT_MODULUS);
                    j += 1;
                }
            }
            len <<= 1;
        }
    }

    /// In-place inverse Number Theoretic Transform.
    pub fn intt(&self, a: &mut [u64]) {
        assert_eq!(a.len(), self.size);
        bit_reverse_permute(a);
        let mut len = 2usize;
        while len <= self.size {
            let half = len >> 1;
            let step = self.size / len;
            for i in (0..self.size).step_by(len) {
                let mut j = 0usize;
                while j < half {
                    let w = self.inv_roots[j * step];
                    let u = a[i + j];
                    let v = mul_mod(a[i + j + half], w, NTT_MODULUS);
                    a[i + j] = add_mod(u, v, NTT_MODULUS);
                    a[i + j + half] = sub_mod(u, v, NTT_MODULUS);
                    j += 1;
                }
            }
            len <<= 1;
        }
        for x in a.iter_mut() {
            *x = mul_mod(*x, self.inv_size, NTT_MODULUS);
        }
    }
}

fn bit_reverse_permute(a: &mut [u64]) {
    let n = a.len();
    let bits = n.trailing_zeros() as usize;
    for i in 0..n {
        let rev = i.reverse_bits() >> (usize::BITS as usize - bits);
        if i < rev {
            a.swap(i, rev);
        }
    }
}

fn add_mod(a: u64, b: u64, m: u64) -> u64 {
    let s = a + b;
    if s >= m { s - m } else { s }
}

fn sub_mod(a: u64, b: u64, m: u64) -> u64 {
    if a >= b { a - b } else { a + m - b }
}

fn mul_mod(a: u64, b: u64, m: u64) -> u64 {
    // 128-bit intermediate avoids overflow for the chosen modulus.
    ((a as u128 * b as u128) % m as u128) as u64
}

fn mod_pow(mut base: u64, mut exp: u64, modulus: u64) -> u64 {
    let mut result = 1u64;
    base %= modulus;
    while exp > 0 {
        if exp & 1 == 1 {
            result = mul_mod(result, base, modulus);
        }
        base = mul_mod(base, base, modulus);
        exp >>= 1;
    }
    result
}

fn mod_inv(a: u64, m: u64) -> u64 {
    // Fermat's little theorem; valid because m is prime and a != 0.
    mod_pow(a, m - 2, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntt_intt_roundtrip() {
        let engine = NttEngine::new(16);
        let original: Vec<u64> = (0..16).map(|x| x * 7 % NTT_MODULUS).collect();
        let mut a = original.clone();
        engine.ntt(&mut a);
        engine.intt(&mut a);
        assert_eq!(a, original);
    }

    #[test]
    fn ntt_linearity() {
        let engine = NttEngine::new(16);
        let x: Vec<u64> = (0..16).map(|i| i as u64).collect();
        let y: Vec<u64> = (0..16).map(|i| (i * 3) as u64).collect();
        let mut a = x.clone();
        let mut b = y.clone();
        engine.ntt(&mut a);
        engine.ntt(&mut b);
        let sum_ntt: Vec<u64> = a
            .iter()
            .zip(b.iter())
            .map(|(u, v)| add_mod(*u, *v, NTT_MODULUS))
            .collect();

        let mut z: Vec<u64> = x
            .iter()
            .zip(y.iter())
            .map(|(u, v)| add_mod(*u, *v, NTT_MODULUS))
            .collect();
        engine.ntt(&mut z);
        assert_eq!(sum_ntt, z);
    }
}
