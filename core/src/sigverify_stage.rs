//! The `sigverify_stage` implements the signature verification stage of the TPU. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next stage whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if perf-libs are available

use crate::sigverify;
use crossbeam_channel::{SendError, Sender as CrossbeamSender};
use solana_measure::measure::Measure;
use solana_metrics::datapoint_debug;
use solana_perf::packet::Packets;
use solana_perf::perf_libs;
use solana_sdk::timing;
use solana_streamer::streamer::{self, PacketReceiver, StreamerError};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread::{self, Builder, JoinHandle};
use thiserror::Error;

const RECV_BATCH_MAX_CPU: usize = 1_000;
const RECV_BATCH_MAX_GPU: usize = 5_000;

#[derive(Error, Debug)]
pub enum SigVerifyServiceError {
    #[error("send packets batch error")]
    SendError(#[from] SendError<Vec<Packets>>),

    #[error("streamer error")]
    StreamerError(#[from] StreamerError),
}

type Result<T> = std::result::Result<T, SigVerifyServiceError>;

#[derive(Debug, Default)]
pub struct SigVerifyStageChannelLimitStats {
    last_report: AtomicU64,
    dropped_packets: AtomicUsize,
}
impl SigVerifyStageChannelLimitStats {
    fn report(&self, report_interval_ms: u64, channel_size_tracker: &AtomicUsize) {
        let should_report = {
            let last = self.last_report.load(Ordering::Relaxed);
            let now = solana_sdk::timing::timestamp();
            now.saturating_sub(last) > report_interval_ms
                && self.last_report.compare_exchange(
                    last,
                    now,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) == Ok(last)
        };

        if should_report {
            datapoint_info!(
                "sigverify_stage-channel-limit-stats",
                (
                    "dropped_packets",
                    self.dropped_packets.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "current_channel_size",
                    channel_size_tracker.load(Ordering::Relaxed) as i64,
                    i64
                )
            );
        }
    }
}

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
        channel_size_limit: Option<(usize, &Arc<AtomicUsize>)>,
    ) -> Self {
        let thread_hdls = Self::verifier_services(
            packet_receiver,
            verified_sender,
            verifier,
            channel_size_limit,
        );
        Self { thread_hdls }
    }

    fn verifier<T: SigVerifier>(
        recvr: &Arc<Mutex<PacketReceiver>>,
        sendr: &CrossbeamSender<Vec<Packets>>,
        id: usize,
        verifier: &T,
        channel_size_limit: &Option<(usize, Arc<AtomicUsize>)>,
        sigverify_stage_stats: &SigVerifyStageChannelLimitStats,
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
        debug!(
            "@{:?} verifier: verifying: {} id: {}",
            timing::timestamp(),
            len,
            id
        );

        let verified_batch = verifier.verify_batch(batch);

        for v in verified_batch {
            let v_len = v.packets.len();
            let should_send = channel_size_limit
                .as_ref()
                .map(|(limit, channel_size_tracker)| {
                    let size = channel_size_tracker.load(Ordering::SeqCst);
                    if size + v_len <= *limit {
                        // Send first to avoid consumer from removing from the channel,
                        // subtracting from the counter, and causing overflow
                        info!(
                            "Sending packet because size: {}, requested: {}, limit: {}",
                            size, v_len, *limit
                        );
                        channel_size_tracker.fetch_add(v_len, Ordering::SeqCst);
                        true
                    } else {
                        sigverify_stage_stats
                            .dropped_packets
                            .fetch_add(v_len, Ordering::Relaxed);
                        info!(
                            "Dumped packet because size: {}, requested: {}, limit: {}",
                            size, v_len, *limit
                        );
                        false
                    }
                })
                .unwrap_or(true);

            if should_send {
                sendr.send(vec![v])?;
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
        channel_size_limit: Option<(usize, Arc<AtomicUsize>)>,
        sigverify_stage_stats: Arc<SigVerifyStageChannelLimitStats>,
    ) -> JoinHandle<()> {
        let verifier = verifier.clone();
        Builder::new()
            .name(format!("solana-verifier-{}", id))
            .spawn(move || loop {
                if let Err(e) = Self::verifier(
                    &packet_receiver,
                    &verified_sender,
                    id,
                    &verifier,
                    &channel_size_limit,
                    &sigverify_stage_stats,
                ) {
                    match e {
                        SigVerifyServiceError::StreamerError(StreamerError::RecvTimeoutError(
                            RecvTimeoutError::Disconnected,
                        )) => break,
                        SigVerifyServiceError::StreamerError(StreamerError::RecvTimeoutError(
                            RecvTimeoutError::Timeout,
                        )) => (),
                        SigVerifyServiceError::SendError(_) => {
                            break;
                        }
                        _ => error!("{:?}", e),
                    }
                }

                if id == 0 {
                    if let Some((_, channel_size_tracker)) = &channel_size_limit {
                        sigverify_stage_stats.report(1000, channel_size_tracker);
                    }
                }
            })
            .unwrap()
    }

    fn verifier_services<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: PacketReceiver,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        verifier: T,
        channel_size_limit: Option<(usize, &Arc<AtomicUsize>)>,
    ) -> Vec<JoinHandle<()>> {
        let receiver = Arc::new(Mutex::new(packet_receiver));
        let sigverify_stage_stats = Arc::new(SigVerifyStageChannelLimitStats::default());
        (0..4)
            .map(|id| {
                Self::verifier_service(
                    receiver.clone(),
                    verified_sender.clone(),
                    id,
                    &verifier,
                    channel_size_limit.map(|(limit, limit_tracker)| (limit, limit_tracker.clone())),
                    sigverify_stage_stats.clone(),
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
