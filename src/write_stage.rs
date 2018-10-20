//! The `write_stage` module implements the TPU's write stage. It
//! writes entries to the given writer, which is typically a file or
//! stdout, and then sends the Entry to its output channel.

use counter::Counter;
use entry::Entry;
use ledger::LedgerWriter;
use log::Level;
use result::{Error, Result};
use service::Service;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::thread::{self, Builder, JoinHandle};
use std::time::{Duration, Instant};
use timing::{duration_as_ms, duration_as_s};

pub struct WriteStage {
    write_thread: JoinHandle<()>,
}

impl WriteStage {
    /// Process any Entry items that have been published by the RecordStage.
    /// continuosly send entries out
    pub fn write_and_send_entries(
        ledger_writer: &mut LedgerWriter,
        entry_sender: &Sender<Vec<Entry>>,
        entry_receiver: &Receiver<Vec<Entry>>,
    ) -> Result<()> {
        let mut ventries = Vec::new();
        let mut received_entries = entry_receiver.recv_timeout(Duration::new(1, 0))?;
        let now = Instant::now();
        let mut num_new_entries = 0;
        let mut num_txs = 0;

        loop {
            num_new_entries += received_entries.len();
            ventries.push(received_entries);

            if let Ok(n) = entry_receiver.try_recv() {
                received_entries = n;
            } else {
                break;
            }
        }
        inc_new_counter_info!("write_stage-entries_received", num_new_entries);

        debug!("write_stage entries: {}", num_new_entries);

        let mut entries_send_total = 0;

        let start = Instant::now();
        for entries in ventries {
            for e in &entries {
                num_txs += e.transactions.len();
                ledger_writer.write_entry_noflush(&e)?;
            }

            inc_new_counter_info!("write_stage-write_entries", entries.len());

            //TODO(anatoly): real stake based voting needs to change this
            //leader simply votes if the current set of validators have voted
            //on a valid last id

            trace!("New entries? {}", entries.len());
            let entries_send_start = Instant::now();
            if !entries.is_empty() {
                inc_new_counter_info!("write_stage-entries_sent", entries.len());
                trace!("broadcasting {}", entries.len());
                entry_sender.send(entries)?;
            }

            entries_send_total += duration_as_ms(&entries_send_start.elapsed());
        }
        ledger_writer.flush()?;
        inc_new_counter_info!(
            "write_stage-time_ms",
            duration_as_ms(&now.elapsed()) as usize
        );
        debug!(
            "done write_stage txs: {} time {} ms txs/s: {} entries_send_total: {}",
            num_txs,
            duration_as_ms(&start.elapsed()),
            num_txs as f32 / duration_as_s(&start.elapsed()),
            entries_send_total,
        );

        Ok(())
    }

    /// Create a new WriteStage for writing and broadcasting entries.
    pub fn new(
        ledger_path: &str,
        entry_receiver: Receiver<Vec<Entry>>,
    ) -> (Self, Receiver<Vec<Entry>>) {
        let (entry_sender, entry_receiver_forward) = channel();
        let mut ledger_writer = LedgerWriter::recover(ledger_path).unwrap();

        let write_thread = Builder::new()
            .name("solana-writer".to_string())
            .spawn(move || loop {
                if let Err(e) =
                    Self::write_and_send_entries(&mut ledger_writer, &entry_sender, &entry_receiver)
                {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => {
                            break;
                        }
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        _ => {
                            inc_new_counter_info!("write_stage-write_and_send_entries-error", 1);
                            error!("{:?}", e);
                        }
                    }
                };
            }).unwrap();

        (WriteStage { write_thread }, entry_receiver_forward)
    }
}

impl Service for WriteStage {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.write_thread.join()
    }
}
