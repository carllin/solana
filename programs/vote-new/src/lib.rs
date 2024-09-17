#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(min_specialization))]

pub mod vote_processor_new;
pub mod vote_state_new;
pub mod vote_transaction_new;

#[macro_use]
extern crate solana_metrics;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;

pub use solana_sdk::vote_new::{
    authorized_voters, error as vote_error, instruction as vote_instruction,
    program::{check_id, id},
};
