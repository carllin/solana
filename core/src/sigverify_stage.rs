//! The `sigverify_stage` implements the signature verification stage of the TPU. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next stage whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if perf-libs are available

use {
    crate::{find_packet_sender_stake_stage, sigverify},
    core::time::Duration,
    crossbeam_channel::{RecvTimeoutError, SendError},
    itertools::Itertools,
    solana_measure::measure::Measure,
    solana_perf::{
        packet::{Packet, PacketBatch},
        sigverify::{count_valid_packets, shrink_batches, Deduper},
    },
    solana_sdk::timing,
    solana_streamer::streamer::{self, StreamerError},
    std::{
        thread::{self, Builder, JoinHandle},
        time::Instant,
    },
    thiserror::Error,
};

const MAX_SIGVERIFY_BATCH: usize = 10_000;

#[derive(Error, Debug)]
pub enum SigVerifyServiceError<SendType> {
    #[error("send packets batch error")]
    Send(#[from] SendError<SendType>),

    #[error("streamer error")]
    Streamer(#[from] StreamerError),
}

type Result<T, SendType> = std::result::Result<T, SigVerifyServiceError<SendType>>;

pub struct SigVerifyStage {
    thread_hdl: JoinHandle<()>,
}

pub trait SigVerifier {
    type SendType: std::fmt::Debug;
    fn verify_batches(&self, batches: Vec<PacketBatch>, valid_packets: usize) -> Vec<PacketBatch>;
    fn process_received_packet(&mut self, _packet: &mut Packet, _is_dup: bool) {}
    fn process_excess_packet(&mut self, _packet: &Packet) {}
    fn process_passed_sigverify_packet(&mut self, _packet: &Packet) {}
    fn send_packets(&mut self, packet_batches: Vec<PacketBatch>) -> Result<(), Self::SendType>;
}

#[derive(Default, Clone)]
pub struct DisabledSigVerifier {}

#[derive(Default)]
struct SigVerifierStats {
    recv_batches_us_hist: histogram::Histogram, // time to call recv_batch
    verify_batches_pp_us_hist: histogram::Histogram, // per-packet time to call verify_batch
    discard_packets_pp_us_hist: histogram::Histogram, // per-packet time to call verify_batch
    dedup_packets_pp_us_hist: histogram::Histogram, // per-packet time to call verify_batch
    batches_hist: histogram::Histogram,         // number of packet batches per verify call
    packets_hist: histogram::Histogram,         // number of packets per verify call
    total_batches: usize,
    total_packets: usize,
    total_dedup: usize,
    total_excess_fail: usize,
    total_valid_packets: usize,
    total_shrinks: usize,
    total_dedup_time_us: usize,
    total_discard_time_us: usize,
    total_verify_time_us: usize,
    total_shrink_time_us: usize,
}

impl SigVerifierStats {
    fn report(&self, name: &'static str) {
        datapoint_info!(
            name,
            (
                "recv_batches_us_90pct",
                self.recv_batches_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_min",
                self.recv_batches_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_max",
                self.recv_batches_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_mean",
                self.recv_batches_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_90pct",
                self.verify_batches_pp_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_min",
                self.verify_batches_pp_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_max",
                self.verify_batches_pp_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_mean",
                self.verify_batches_pp_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_90pct",
                self.discard_packets_pp_us_hist
                    .percentile(90.0)
                    .unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_min",
                self.discard_packets_pp_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_max",
                self.discard_packets_pp_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_mean",
                self.discard_packets_pp_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_90pct",
                self.dedup_packets_pp_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_min",
                self.dedup_packets_pp_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_max",
                self.dedup_packets_pp_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_mean",
                self.dedup_packets_pp_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "batches_90pct",
                self.batches_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("batches_min", self.batches_hist.minimum().unwrap_or(0), i64),
            ("batches_max", self.batches_hist.maximum().unwrap_or(0), i64),
            ("batches_mean", self.batches_hist.mean().unwrap_or(0), i64),
            (
                "packets_90pct",
                self.packets_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("packets_min", self.packets_hist.minimum().unwrap_or(0), i64),
            ("packets_max", self.packets_hist.maximum().unwrap_or(0), i64),
            ("packets_mean", self.packets_hist.mean().unwrap_or(0), i64),
            ("total_batches", self.total_batches, i64),
            ("total_packets", self.total_packets, i64),
            ("total_dedup", self.total_dedup, i64),
            ("total_excess_fail", self.total_excess_fail, i64),
            ("total_valid_packets", self.total_valid_packets, i64),
            ("total_shrinks", self.total_shrinks, i64),
            ("total_dedup_time_us", self.total_dedup_time_us, i64),
            ("total_discard_time_us", self.total_discard_time_us, i64),
            ("total_verify_time_us", self.total_verify_time_us, i64),
            ("total_shrink_time_us", self.total_shrink_time_us, i64),
        );
    }
}

impl SigVerifier for DisabledSigVerifier {
    type SendType = ();
    fn verify_batches(
        &self,
        mut batches: Vec<PacketBatch>,
        _valid_packets: usize,
    ) -> Vec<PacketBatch> {
        sigverify::ed25519_verify_disabled(&mut batches);
        batches
    }

    fn send_packets(&mut self, _packet_batches: Vec<PacketBatch>) -> Result<(), Self::SendType> {
        Ok(())
    }
}

impl SigVerifyStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: T,
        name: &'static str,
    ) -> Self {
        let thread_hdl = Self::verifier_services(packet_receiver, verifier, name);
        Self { thread_hdl }
    }

    pub fn discard_excess_packets(
        batches: &mut [PacketBatch],
        mut max_packets: usize,
        mut process_excess_packet: impl FnMut(&Packet),
    ) {
        // Group packets by their incoming IP address.
        let mut addrs = batches
            .iter_mut()
            .rev()
            .flat_map(|batch| batch.packets.iter_mut().rev())
            .filter(|packet| !packet.meta.discard())
            .map(|packet| (packet.meta.addr, packet))
            .into_group_map();
        // Allocate max_packets evenly across addresses.
        while max_packets > 0 && !addrs.is_empty() {
            let num_addrs = addrs.len();
            addrs.retain(|_, packets| {
                let cap = (max_packets + num_addrs - 1) / num_addrs;
                max_packets -= packets.len().min(cap);
                packets.truncate(packets.len().saturating_sub(cap));
                !packets.is_empty()
            });
        }
        // Discard excess packets from each address.
        for packet in addrs.into_values().flatten() {
            process_excess_packet(packet);
            packet.meta.set_discard(true);
        }
    }

    fn verifier<T: SigVerifier>(
        deduper: &Deduper,
        recvr: &find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: &mut T,
        stats: &mut SigVerifierStats,
    ) -> Result<(), T::SendType> {
        let (mut batches, num_packets, recv_duration) = streamer::recv_vec_packet_batches(recvr)?;

        let batches_len = batches.len();
        debug!(
            "@{:?} verifier: verifying: {}",
            timing::timestamp(),
            num_packets,
        );

        let mut dedup_time = Measure::start("sigverify_dedup_time");
        let discard_or_dedup_fail =
            deduper.dedup_packets_and_count_discards(&mut batches, |received_packet, is_dup| {
                verifier.process_received_packet(received_packet, is_dup);
            }) as usize;
        dedup_time.stop();
        let num_unique = num_packets.saturating_sub(discard_or_dedup_fail as usize);

        let mut discard_time = Measure::start("sigverify_discard_time");
        let mut num_valid_packets = num_unique;
        if num_unique > MAX_SIGVERIFY_BATCH {
            Self::discard_excess_packets(&mut batches, MAX_SIGVERIFY_BATCH, |excess_packet| {
                verifier.process_excess_packet(excess_packet)
            });
            num_valid_packets = MAX_SIGVERIFY_BATCH;
        }
        let excess_fail = num_unique.saturating_sub(MAX_SIGVERIFY_BATCH);
        discard_time.stop();

        let mut verify_time = Measure::start("sigverify_batch_time");
        let mut batches = verifier.verify_batches(batches, num_valid_packets);
        verify_time.stop();

        let mut shrink_time = Measure::start("sigverify_shrink_time");
        let num_valid_packets = count_valid_packets(&batches, |valid_packet| {
            verifier.process_passed_sigverify_packet(valid_packet)
        });
        let start_len = batches.len();
        const MAX_EMPTY_BATCH_RATIO: usize = 4;
        if num_packets > num_valid_packets.saturating_mul(MAX_EMPTY_BATCH_RATIO) {
            let valid = shrink_batches(&mut batches);
            batches.truncate(valid);
        }
        let total_shrinks = start_len.saturating_sub(batches.len());
        shrink_time.stop();

        verifier.send_packets(batches)?;

        debug!(
            "@{:?} verifier: done. batches: {} total verify time: {:?} verified: {} v/s {}",
            timing::timestamp(),
            batches_len,
            verify_time.as_ms(),
            num_packets,
            (num_packets as f32 / verify_time.as_s())
        );

        stats
            .recv_batches_us_hist
            .increment(recv_duration.as_micros() as u64)
            .unwrap();
        stats
            .verify_batches_pp_us_hist
            .increment(verify_time.as_us() / (num_packets as u64))
            .unwrap();
        stats
            .discard_packets_pp_us_hist
            .increment(discard_time.as_us() / (num_packets as u64))
            .unwrap();
        stats
            .dedup_packets_pp_us_hist
            .increment(dedup_time.as_us() / (num_packets as u64))
            .unwrap();
        stats.batches_hist.increment(batches_len as u64).unwrap();
        stats.packets_hist.increment(num_packets as u64).unwrap();
        stats.total_batches += batches_len;
        stats.total_packets += num_packets;
        stats.total_dedup += discard_or_dedup_fail;
        stats.total_valid_packets += num_valid_packets;
        stats.total_excess_fail += excess_fail;
        stats.total_shrinks += total_shrinks;
        stats.total_dedup_time_us += dedup_time.as_us() as usize;
        stats.total_discard_time_us += discard_time.as_us() as usize;
        stats.total_verify_time_us += verify_time.as_us() as usize;
        stats.total_shrink_time_us += shrink_time.as_us() as usize;

        Ok(())
    }

    fn verifier_service<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        mut verifier: T,
        name: &'static str,
    ) -> JoinHandle<()> {
        let mut stats = SigVerifierStats::default();
        let mut last_print = Instant::now();
        const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
        const MAX_DEDUPER_ITEMS: u32 = 1_000_000;
        Builder::new()
            .name("solana-verifier".to_string())
            .spawn(move || {
                let mut deduper = Deduper::new(MAX_DEDUPER_ITEMS, MAX_DEDUPER_AGE);
                loop {
                    deduper.reset();
                    if let Err(e) =
                        Self::verifier(&deduper, &packet_receiver, &mut verifier, &mut stats)
                    {
                        match e {
                            SigVerifyServiceError::Streamer(StreamerError::RecvTimeout(
                                RecvTimeoutError::Disconnected,
                            )) => break,
                            SigVerifyServiceError::Streamer(StreamerError::RecvTimeout(
                                RecvTimeoutError::Timeout,
                            )) => (),
                            SigVerifyServiceError::Send(_) => {
                                break;
                            }
                            _ => error!("{:?}", e),
                        }
                    }
                    if last_print.elapsed().as_secs() > 2 {
                        stats.report(name);
                        stats = SigVerifierStats::default();
                        last_print = Instant::now();
                    }
                }
            })
            .unwrap()
    }

    fn verifier_services<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: T,
        name: &'static str,
    ) -> JoinHandle<()> {
        Self::verifier_service(packet_receiver, verifier, name)
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{sigverify::TransactionSigVerifier, sigverify_stage::timing::duration_as_ms},
        crossbeam_channel::unbounded,
        solana_perf::{
            packet::{to_packet_batches, Packet},
            test_tx::test_tx,
        },
        solana_sdk::packet::PacketFlags,
    };

    fn count_non_discard(packet_batches: &[PacketBatch]) -> usize {
        packet_batches
            .iter()
            .map(|batch| {
                batch
                    .packets
                    .iter()
                    .map(|p| if p.meta.discard() { 0 } else { 1 })
                    .sum::<usize>()
            })
            .sum::<usize>()
    }

    #[test]
    fn test_packet_discard() {
        solana_logger::setup();
        let mut batch = PacketBatch::default();
        let mut tracer_packet = Packet::default();
        tracer_packet.meta.flags |= PacketFlags::TRACER_PACKET;
        batch.packets.resize(10, tracer_packet);
        batch.packets[3].meta.addr = std::net::IpAddr::from([1u16; 8]);
        batch.packets[3].meta.set_discard(true);
        let num_discarded_before_filter = 1;
        batch.packets[4].meta.addr = std::net::IpAddr::from([2u16; 8]);
        let total_num_packets = batch.packets.len();
        let mut batches = vec![batch];
        let max = 3;
        let mut total_tracer_packets_discarded = 0;
        SigVerifyStage::discard_excess_packets(&mut batches, max, |packet| {
            if packet.meta.is_tracer_packet() {
                total_tracer_packets_discarded += 1;
            }
        });
        let total_non_discard = count_non_discard(&batches);
        let total_discarded = total_num_packets - total_non_discard;
        // Every packet except the packets already marked `discard` before the call
        // to `discard_excess_packets()` should count towards the
        // `total_tracer_packets_discarded`
        assert_eq!(
            total_tracer_packets_discarded,
            total_discarded - num_discarded_before_filter
        );
        assert_eq!(total_non_discard, max);
        assert!(!batches[0].packets[0].meta.discard());
        assert!(batches[0].packets[3].meta.discard());
        assert!(!batches[0].packets[4].meta.discard());
    }

    fn gen_batches(
        use_same_tx: bool,
        packets_per_batch: usize,
        total_packets: usize,
    ) -> Vec<PacketBatch> {
        if use_same_tx {
            let tx = test_tx();
            to_packet_batches(&vec![tx; total_packets], packets_per_batch)
        } else {
            let txs: Vec<_> = (0..total_packets).map(|_| test_tx()).collect();
            to_packet_batches(&txs, packets_per_batch)
        }
    }

    #[test]
    fn test_sigverify_stage() {
        solana_logger::setup();
        trace!("start");
        let (packet_s, packet_r) = unbounded();
        let (verified_s, verified_r) = unbounded();
        let verifier = TransactionSigVerifier::new(verified_s);
        let stage = SigVerifyStage::new(packet_r, verifier, "test");

        let use_same_tx = true;
        let now = Instant::now();
        let packets_per_batch = 1024;
        let total_packets = 4096;
        // This is important so that we don't discard any packets and fail asserts below about
        // `num_excess_tracer_packets`
        assert!(total_packets < MAX_SIGVERIFY_BATCH);
        let mut batches = gen_batches(use_same_tx, packets_per_batch, total_packets);
        trace!(
            "starting... generation took: {} ms batches: {}",
            duration_as_ms(&now.elapsed()),
            batches.len()
        );

        let mut sent_len = 0;
        for _ in 0..batches.len() {
            if let Some(mut batch) = batches.pop() {
                sent_len += batch.packets.len();
                batch
                    .packets
                    .iter_mut()
                    .for_each(|packet| packet.meta.flags |= PacketFlags::TRACER_PACKET);
                assert_eq!(batch.packets.len(), packets_per_batch);
                packet_s.send(vec![batch]).unwrap();
            }
        }
        let mut received = 0;
        let mut total_tracer_packets_received = 0;
        trace!("sent: {}", sent_len);
        loop {
            if let Ok((mut verifieds, tracer_packet_stats_option)) = verified_r.recv() {
                let tracer_packet_stats = tracer_packet_stats_option.unwrap();
                total_tracer_packets_received += tracer_packet_stats.total_tracer_packets_received;
                assert_eq!(
                    tracer_packet_stats.total_tracer_packets_received % packets_per_batch,
                    0,
                );

                if use_same_tx {
                    // Every transaction other than the very first one in the very first batch
                    // should be deduped.

                    // Also have to account for the fact that deduper could be cleared periodically,
                    // in which case the first transaction in the next batch won't be deduped
                    assert!(
                        (tracer_packet_stats.total_tracer_packets_deduped
                            == tracer_packet_stats.total_tracer_packets_received - 1)
                            || (tracer_packet_stats.total_tracer_packets_deduped
                                == tracer_packet_stats.total_tracer_packets_received)
                    );
                    assert!(
                        (tracer_packet_stats.total_tracker_packets_passed_sigverify == 1)
                            || (tracer_packet_stats.total_tracker_packets_passed_sigverify == 0)
                    );
                } else {
                    assert_eq!(tracer_packet_stats.total_tracer_packets_deduped, 0);
                    assert!(
                        (tracer_packet_stats.total_tracker_packets_passed_sigverify
                            == tracer_packet_stats.total_tracer_packets_received)
                    );
                }
                assert_eq!(tracer_packet_stats.num_excess_tracer_packets, 0);
                while let Some(v) = verifieds.pop() {
                    received += v.packets.len();
                    batches.push(v);
                }
            }

            if total_tracer_packets_received >= sent_len {
                break;
            }
        }
        trace!("received: {}", received);
        assert_eq!(total_tracer_packets_received, total_packets);
        drop(packet_s);
        stage.join().unwrap();
    }
}
