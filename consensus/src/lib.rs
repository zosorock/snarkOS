// Copyright (C) 2019-2021 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

#![deny(unused_import_braces, unused_qualifications, trivial_casts, trivial_numeric_casts)]
#![deny(unused_qualifications, variant_size_differences, stable_features, unreachable_pub)]
#![deny(non_shorthand_field_patterns, unused_attributes, unused_extern_crates)]
#![deny(
    renamed_and_removed_lints,
    stable_features,
    unused_allocation,
    unused_comparisons,
    bare_trait_objects
)]
#![deny(
    const_err,
    unused_must_use,
    unused_mut,
    unused_unsafe,
    private_in_public,
    unsafe_code
)]
#![allow(clippy::needless_lifetimes)]
#![forbid(unsafe_code)]

#[macro_use]
extern crate thiserror;

#[macro_use]
extern crate tracing;

pub mod consensus;
pub use consensus::*;

pub mod difficulty;
pub use difficulty::*;

pub mod error;

pub mod miner;
pub use miner::MineContext;

pub mod memory_pool;
pub use memory_pool::MemoryPool;

pub mod parameters;
pub use parameters::*;

pub mod ledger;
pub use ledger::*;
use snarkvm_dpc::AleoAmount;

pub const OLDEST_FORK_THRESHOLD: usize = u32::MAX as usize;

/// Calculate a block reward that halves every 4 years * 365 days * 24 hours * 100 blocks/hr = 3,504,000 blocks.
pub fn get_block_reward(block_num: u32) -> AleoAmount {
    let expected_blocks_per_hour: u32 = 100;
    let num_years = 4;
    let block_segments = num_years * 365 * 24 * expected_blocks_per_hour;

    let aleo_denonimation = AleoAmount::COIN;
    let initial_reward = 150i64 * aleo_denonimation;

    // The block reward halves at most 2 times - minimum is 37.5 ALEO after 8 years.
    let num_halves = u32::min(block_num / block_segments, 2);
    let reward = initial_reward / (2_u64.pow(num_halves)) as i64;

    AleoAmount::from_bytes(reward)
}
