//! The `sigverify_stage` implements the signature verification stage of the TPU. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next stage whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if perf-libs are available

use crate::packet::Packets;
use crate::result::{Error, Result};
use crate::sigverify;
use crate::streamer::{self, PacketReceiver};
use crossbeam_channel::Sender as CrossbeamSender;
use solana_measure::measure::Measure;
use solana_metrics::datapoint_debug;
use solana_perf::perf_libs;
use solana_sdk::timing;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder, JoinHandle};

const RECV_BATCH_MAX_CPU: usize = 1_000;
const RECV_BATCH_MAX_GPU: usize = 5_000;

pub struct SigVerifyStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

pub trait SigVerifier {
    fn verify_batch(&self, batch: Vec<Packets>) -> Vec<Packets>;
}

#[derive(Default, Clone)]
pub struct DisabledSigVerifier {}

impl SigVerifier for DisabledSigVerifier {
    fn verify_batch(&self, mut batch: Vec<Packets>) -> Vec<Packets> {
        let r = sigverify::ed25519_verify_disabled(&batch);
        sigverify::mark_disabled(&mut batch, &r);
        batch
    }
}

impl SigVerifyStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: Receiver<Packets>,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        verifier: T,
        name: &str,
    ) -> Self {
        let thread_hdls = Self::verifier_services(packet_receiver, verified_sender, verifier, name);
        Self { thread_hdls }
    }

    fn verifier<T: SigVerifier>(
        recvr: &Arc<Mutex<PacketReceiver>>,
        sendr: &CrossbeamSender<Vec<Packets>>,
        id: usize,
        verifier: &T,
    ) -> Result<()> {
        let (batch, len, recv_time) = streamer::recv_batch(
            &recvr.lock().expect("'recvr' lock in fn verifier"),
            if perf_libs::api().is_some() {
                RECV_BATCH_MAX_GPU
            } else {
                RECV_BATCH_MAX_CPU
            },
        )?;

        let mut verify_batch_time = Measure::start("sigverify_batch_time");
        let batch_len = batch.len();
        info!(
            "@{:?} verifier: verifying: {} name: {} id: {}",
            timing::timestamp(),
            len,
            thread::current().name().unwrap(),
            id
        );

        let verified_batch = verifier.verify_batch(batch);

        for v in verified_batch {
            if let Err(e) = sendr.send(vec![v]) {
                error!("Verifier sender error: {:?}", e);
                return Err(Error::SendError);
            }
        }

        verify_batch_time.stop();

        debug!(
            "@{:?} verifier: done. batches: {} total verify time: {:?} id: {} verified: {} v/s {}",
            timing::timestamp(),
            batch_len,
            verify_batch_time.as_ms(),
            id,
            len,
            (len as f32 / verify_batch_time.as_s())
        );

        datapoint_debug!(
            "sigverify_stage-total_verify_time",
            ("num_batches", batch_len, i64),
            ("num_packets", len, i64),
            ("verify_time_ms", verify_batch_time.as_ms(), i64),
            ("recv_time", recv_time, i64),
        );

        Ok(())
    }

    fn verifier_service<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: Arc<Mutex<PacketReceiver>>,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        id: usize,
        verifier: &T,
        name: &str,
    ) -> JoinHandle<()> {
        let verifier = verifier.clone();
        Builder::new()
            .name(format!("solana-verifier-{}-{}", name, id))
            .spawn(move || loop {
                if let Err(e) = Self::verifier(&packet_receiver, &verified_sender, id, &verifier) {
                    match e {
                        Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => {
                            error!(
                                "Verifier service recv disconnected {:?}",
                                thread::current().name()
                            );
                            break;
                        }
                        Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                        Error::SendError => {
                            error!(
                                "Verifier service send disconnected {:?}",
                                thread::current().name()
                            );
                            break;
                        }
                        _ => error!(
                            "Verifier service {:?} errored: {:?}",
                            thread::current().name(),
                            e
                        ),
                    }
                }
            })
            .unwrap()
    }

    fn verifier_services<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: PacketReceiver,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        verifier: T,
        name: &str,
    ) -> Vec<JoinHandle<()>> {
        let receiver = Arc::new(Mutex::new(packet_receiver));
        (0..4)
            .map(|id| {
                Self::verifier_service(
                    receiver.clone(),
                    verified_sender.clone(),
                    id,
                    &verifier,
                    name,
                )
            })
            .collect()
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
