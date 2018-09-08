extern crate solana;

use solana::ledger::LedgerWriter;
use solana::mint::Mint;
use solana::signature::{Keypair, KeypairUtil};

pub fn tmp_ledger_path(name: &str) -> String {
    let keypair = Keypair::new();

    format!("/tmp/tmp-ledger-{}-{}", name, keypair.pubkey())
}

pub fn genesis(name: &str, num: i64) -> (Mint, String) {
    let mint = Mint::new(num);

    let path = tmp_ledger_path(name);
    let mut writer = LedgerWriter::open(&path, true).unwrap();

    writer.write_entries(mint.create_entries()).unwrap();

    (mint, path)
}