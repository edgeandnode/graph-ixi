use std::env;

use once_cell::sync::Lazy;
use rand::{rngs::OsRng, rngs::SmallRng, RngCore, SeedableRng};

mod gen;
mod indexing_statuses;
mod mocks;
mod proofs_of_indexing;

pub use gen::*;
pub use mocks::*;

pub static TEST_SEED: Lazy<u64> = Lazy::new(|| {
    let seed = env::var("TEST_SEED")
        .map(|seed| seed.parse().expect("Invalid TEST_SEED value"))
        .unwrap_or(OsRng.next_u64());

    println!("------------------------------------------------------------------------");
    println!("TEST_SEED={}", seed);
    println!("  This value can be changed via the environment variable TEST_SEED.");
    println!("------------------------------------------------------------------------");

    seed
});

pub fn fast_rng() -> SmallRng {
    SmallRng::seed_from_u64(*TEST_SEED)
}
