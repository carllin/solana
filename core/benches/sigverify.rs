#![feature(test)]

extern crate test;

use solana_core::blocktree::make_many_slot_entries_with_keypair;
use solana_core::packet::to_packets;
use solana_core::recycler::Recycler;
use solana_core::sigverify;
use solana_core::test_tx::test_tx;
use solana_sdk::signature::Keypair;
use solana_sdk::signature::KeypairUtil;
use std::sync::Arc;
use test::Bencher;

#[bench]
fn bench_sigverify(bencher: &mut Bencher) {
    let tx = test_tx();

    // generate packet vector
    let batches = to_packets(&vec![tx; 128]);

    let recycler = Recycler::default();
    let recycler_out = Recycler::default();
    // verify packets
    bencher.iter(|| {
        let _ans = sigverify::ed25519_verify(&batches, &recycler, &recycler_out);
    })
}

#[bench]
fn bench_shred_verify(bencher: &mut Bencher) {
    let keypair = Arc::new(Keypair::new());
    let pubkey = keypair.pubkey();
    let (shreds, _) = make_many_slot_entries_with_keypair(0, 10, 10, &keypair);
    println!("num shreds: {}", shreds.len());
    let serialized_shreds: Vec<_> = shreds
        .iter()
        .map(|s| bincode::serialize(s).unwrap())
        .collect();
    // verify packets
    bencher.iter(|| {
        for (s, d) in shreds.iter().zip(serialized_shreds.iter()) {
            s.fast_verify(d, &pubkey);
        }
    })
}
