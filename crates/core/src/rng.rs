//! The random streams every value is drawn from. Each stream is a ChaCha8 generator keyed by the
//! seed and a name, and positioned by a fixed block of rows, so a value depends on neither thread
//! count nor batch size nor column order.

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

/// The version of every sampler's draw order. It is part of a dataset's identity, and any change
/// that alters a generated value for the same inputs raises it.
pub const STREAM_VERSION: u32 = 10;

/// The rows one block holds. A block's streams start fresh, so any thread can produce any block.
pub const BLOCK_ROWS: u64 = 65_536;

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a over `table\0path\0draw`, so a stream is named, never numbered.
pub(crate) fn stream_key(table: &str, path: &str, draw: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xCBF2_9CE4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut hash = OFFSET_BASIS;
    for part in [table.as_bytes(), b"\0", path.as_bytes(), b"\0", draw.as_bytes()] {
        for &byte in part {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

/// The stream named by `key`, positioned at the start of `block`.
pub(crate) fn rng_for(seed: u64, key: u64, block: u64) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::seed_from_u64(splitmix(seed ^ splitmix(key)));
    rng.set_stream(block);
    rng
}

/// A uniform draw from [0, 1) with 53 bits of precision, from one 64-bit word.
pub(crate) fn unit(rng: &mut ChaCha8Rng) -> f64 {
    const SCALE: f64 = 1.0 / (1u64 << 53) as f64;
    (rng.next_u64() >> 11) as f64 * SCALE
}

/// A uniform draw from (0, 1), never either end, from one 64-bit word.
pub(crate) fn open_unit(rng: &mut ChaCha8Rng) -> f64 {
    const SCALE: f64 = 1.0 / (1u64 << 53) as f64;
    ((rng.next_u64() >> 11) as f64 + 0.5) * SCALE
}

/// A draw from `0..n` as one 32-bit word, by multiply and shift. Its bias is below n / 2^32.
pub(crate) fn below(rng: &mut ChaCha8Rng, n: u32) -> u32 {
    ((u64::from(rng.next_u32()) * u64::from(n)) >> 32) as u32
}

/// A draw from `0..n` as one 64-bit word, by multiply and shift. Its bias is below n / 2^64.
pub(crate) fn below_u64(rng: &mut ChaCha8Rng, n: u64) -> u64 {
    ((u128::from(rng.next_u64()) * u128::from(n)) >> 64) as u64
}

/// A Bernoulli trial as one 64-bit word compared against a fixed threshold.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bernoulli {
    threshold: u64,
    always: bool,
}

impl Bernoulli {
    pub(crate) fn new(p: f64) -> Self {
        const TWO_TO_THE_64: f64 = 18_446_744_073_709_551_616.0;
        let p = p.clamp(0.0, 1.0);
        Self {
            threshold: (p * TWO_TO_THE_64) as u64,
            always: p >= 1.0,
        }
    }

    pub(crate) fn never(&self) -> bool {
        self.threshold == 0 && !self.always
    }

    pub(crate) fn draw(&self, rng: &mut ChaCha8Rng) -> bool {
        let word = rng.next_u64();
        self.always || word < self.threshold
    }
}
