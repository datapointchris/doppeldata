//! A keyed bijection of `0..size`: a balanced Feistel network over the smallest even power of two
//! holding `size`, walked again from any value it maps past `size`.

use crate::rng::splitmix;

const ROUNDS: usize = 6;
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone)]
pub(crate) struct KeyedPermutation {
    size: u64,
    half_bits: u32,
    keys: [u64; ROUNDS],
    identity: bool,
}

impl KeyedPermutation {
    /// The permutation of `0..size` the seed and the stream `key` name.
    pub(crate) fn new(size: u64, seed: u64, key: u64) -> Self {
        let mut half_bits = 1;
        while half_bits < 32 && (1u64 << (2 * half_bits)) < size {
            half_bits += 1;
        }
        let base = splitmix(seed ^ splitmix(key));
        let keys = std::array::from_fn(|round| splitmix(base.wrapping_add(GOLDEN.wrapping_mul(round as u64 + 1))));
        Self {
            size,
            half_bits,
            keys,
            identity: size <= 1,
        }
    }

    /// The permutation that leaves every value where it is.
    pub(crate) fn identity(size: u64) -> Self {
        Self {
            size,
            half_bits: 1,
            keys: [0; ROUNDS],
            identity: true,
        }
    }

    pub(crate) fn forward(&self, value: u64) -> u64 {
        if self.identity {
            return value;
        }
        let mut walked = self.feistel(value);
        while walked >= self.size {
            walked = self.feistel(walked);
        }
        walked
    }

    pub(crate) fn inverse(&self, value: u64) -> u64 {
        if self.identity {
            return value;
        }
        let mut walked = self.feistel_inverse(value);
        while walked >= self.size {
            walked = self.feistel_inverse(walked);
        }
        walked
    }

    fn mask(&self) -> u64 {
        (1u64 << self.half_bits) - 1
    }

    fn feistel(&self, value: u64) -> u64 {
        let mask = self.mask();
        let (mut left, mut right) = (value >> self.half_bits, value & mask);
        for key in self.keys {
            (left, right) = (right, left ^ (splitmix(right ^ key) & mask));
        }
        (left << self.half_bits) | right
    }

    fn feistel_inverse(&self, value: u64) -> u64 {
        let mask = self.mask();
        let (mut left, mut right) = (value >> self.half_bits, value & mask);
        for key in self.keys.iter().rev() {
            (left, right) = (right ^ (splitmix(left ^ key) & mask), left);
        }
        (left << self.half_bits) | right
    }
}
