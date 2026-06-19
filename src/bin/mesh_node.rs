use fluidic::api::state::ApiState;
use fluidic::api::start_api_server;
use fluidic::consensus::Oscillator;
use fluidic::crypto::keys::KeyPair;
use fluidic::crypto::{CommutativeShift, Signal, StakeShift, DEFAULT_DEX_DOMAIN};
use fluidic::field::coordinates::Coordinate;
use fluidic::network::TcpGossipNode;
use fluidic::persistence;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, trace, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let id_str = std::env::var("OSCILLATOR_ID").unwrap_or_else(|_| "0".to_string());
    let id = {
        let mut arr = [0u8; 32];
        // Support both plain numbers ("0") and StatefulSet pod names ("mesh-node-0").
        let n: u64 = id_str
            .rsplit_once('-')
            .and_then(|(_, suffix)| suffix.parse().ok())
            .unwrap_or_else(|| {
                id_str
                    .parse()
                    .expect("OSCILLATOR_ID must be a number or end with one (e.g. mesh-node-0)")
            });
        arr[0..8].copy_from_slice(&n.to_le_bytes());
        arr
    };

    let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:7000".to_string())
        .parse()
        .expect("BIND_ADDR must be a valid SocketAddr");

    let peers: Vec<String> = std::env::var("PEERS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let synth_interval_ms: u64 = std::env::var("SYNTHESIS_INTERVAL_MS")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .expect("SYNTHESIS_INTERVAL_MS must be a number");

    info!(
        "starting mesh node id={} bind={} peers={:?}",
        id_str, bind_addr, peers
    );

    // Derive a deterministic local keypair from the oscillator id so the node
    // keeps the same identity across restarts.
    let local_keypair = KeyPair::from_seed(&id);

    let mut oscillator = Oscillator::new(id, 2048);
    oscillator.set_operator_keypair(local_keypair.clone());
    let oscillator = Arc::new(oscillator);

    // Membership is dynamic: any node can announce its stake via gossiped
    // StakeShift messages. No static operator registry is loaded.

    // Load persisted state if available, then seed only fresh accounts.
    let snapshot_path = persistence::snapshot_path();
    if let Err(e) = persistence::load(&oscillator, &snapshot_path) {
        warn!("failed to load snapshot: {}", e);
    } else {
        info!("loaded snapshot from {:?}", snapshot_path);
    }

    // Seed genesis balance for the local operator on first boot and lock it as
    // stake so a fresh node is immediately eligible to synthesize certificates.
    let genesis_balance = 1_000_000_000_000_000u128;
    let local_account = local_keypair.account_id();
    if oscillator
        .wave_field
        .lock()
        .unwrap()
        .account_balance(local_account)
        .units
        == 0
    {
        oscillator.seed_account(local_account, genesis_balance);
    }
    oscillator.stake_table.stake(local_account, genesis_balance);

    let api_state = Arc::new(ApiState::new(oscillator.clone()));
    api_state.set_operator_keypair(local_keypair.clone());
    api_state.register_key(local_keypair.account_id(), local_keypair.public_key());

    let api_port: u16 = std::env::var("API_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("API_PORT must be a number");
    let api_state_for_server = api_state.clone();
    tokio::spawn(async move {
        if let Err(e) = start_api_server(api_state_for_server, api_port).await {
            tracing::error!("API server failed: {}", e);
        }
    });

    let gossip = TcpGossipNode::bind(bind_addr)
        .await
        .expect("failed to bind gossip socket");
    info!("gossip bound to {}", gossip.local_addr);
    api_state.set_gossip(gossip.outbound.clone());

    // Announce the local operator's stake to the mesh so peers learn it.
    let timestamp_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let stake_signal = Signal::Stake(StakeShift::sign(
        &local_keypair,
        genesis_balance,
        0,
        timestamp_ns,
    ));
    let _ = gossip.outbound.try_send(stake_signal);

    // Connect to peers (resolve DNS names like mesh-node-1.mesh-node:7000).
    for peer in peers {
        match tokio::net::lookup_host(&peer).await {
            Ok(mut addrs) => {
                if let Some(addr) = addrs.next() {
                    if let Err(e) = gossip.add_peer(addr).await {
                        warn!("failed to queue peer {}: {}", addr, e);
                    }
                } else {
                    warn!("peer {} resolved to no addresses", peer);
                }
            }
            Err(e) => warn!("failed to resolve peer {}: {}", peer, e),
        }
    }

    // Ingest loop: apply incoming phase-shifts to the oscillator.
    let osc_ingest = oscillator.clone();
    let api_state_ingest = api_state.clone();
    let ping_outbound = gossip.outbound.clone();
    let mut inbound = gossip.inbound;
    tokio::spawn(async move {
        while let Some(shift) = inbound.recv().await {
            match shift {
                Signal::Registration(reg) => {
                    let vk = match ed25519_dalek::VerifyingKey::from_bytes(&reg.public_key) {
                        Ok(vk) => vk,
                        Err(_) => {
                            warn!("invalid public key in registration gossip");
                            continue;
                        }
                    };
                    api_state_ingest.register_key(reg.account, vk);
                    api_state_ingest.register_key(reg.wave_account, vk);
                    api_state_ingest.register_key(reg.usdc_account, vk);
                    api_state_ingest.register_derived(reg.wave_account, reg.account);
                    api_state_ingest.register_derived(reg.usdc_account, reg.account);
                    osc_ingest.apply_registration(&reg);
                }
                Signal::Stake(stake) => {
                    if !osc_ingest.apply_stake(&stake) {
                        warn!("rejected invalid stake gossip from {}", stake.operator);
                    }
                }
                Signal::Ping { timestamp_ns, nonce } => {
                    let _ = ping_outbound.try_send(Signal::Pong { timestamp_ns, nonce });
                }
                Signal::Pong { timestamp_ns, .. } => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    let rtt_ms = (now.saturating_sub(timestamp_ns)) as f64 / 1_000_000.0;
                    api_state_ingest.record_network_latency_ms(rtt_ms);
                }
                Signal::Certificate(cert) => {
                    let registry = api_state_ingest.key_registry();
                    if let Err(e) = osc_ingest.ingest_certificate(cert.clone(), &registry) {
                        warn!("rejected peer certificate: {:?}", e);
                    } else {
                        trace!("accepted certificate for tick {} from {}", cert.tick, cert.operator);
                    }
                }
                other => {
                    if let Err(e) = osc_ingest.ingest(other) {
                        warn!("ingest error: {}", e);
                    }
                }
            }
        }
    });

    // Gossip RTT probe loop.
    let ping_sender = gossip.outbound.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        let mut nonce = 0u64;
        loop {
            ticker.tick().await;
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            if let Err(e) = ping_sender.try_send(Signal::Ping { timestamp_ns, nonce }) {
                warn!("ping send error: {}", e);
            }
            nonce += 1;
        }
    });

    // Generator loop: emit periodic commutative phase-shifts.
    let sender = gossip.outbound.clone();
    let generator_key = local_keypair.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(100));
        let mut nonce = 0u64;
        let pool = [0xAB; 32];
        loop {
            ticker.tick().await;
            let shift = CommutativeShift::new(
                &generator_key,
                DEFAULT_DEX_DOMAIN,
                Coordinate::from_scalar(nonce),
                1_000_000,
                pool,
                nonce,
                0,
            );
            if let Err(e) = sender.send(Signal::Commutative(shift)).await {
                warn!("broadcast error: {}", e);
                return;
            }
            nonce += 1;
        }
    });

    // Periodic snapshot save.
    let osc_save = oscillator.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if let Err(e) = persistence::save(&osc_save, persistence::snapshot_path()) {
                warn!("snapshot save failed: {}", e);
            }
        }
    });

    // Synthesis loop with graceful shutdown.
    let mut synth_ticker = interval(Duration::from_millis(synth_interval_ms));
    let mut shutdown = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    loop {
        tokio::select! {
            _ = synth_ticker.tick() => {
                let registry = api_state.key_registry();
                let result = oscillator.synthesize(&registry);
                api_state.record_synthesis(&result);

                // Gossip our own certificate so peers can form a quorum.
                let tick = oscillator.synthesis_tick.load(std::sync::atomic::Ordering::SeqCst).saturating_sub(1);
                if let Some(cert) = oscillator.certificates.read().unwrap().get(&tick).cloned() {
                    if let Err(e) = gossip.outbound.try_send(Signal::Certificate(cert)) {
                        warn!("failed to gossip certificate: {}", e);
                    }
                }

                // Check for a stake-weighted quorum on the previous tick.
                if let Some((view, stake)) = oscillator.check_quorum(tick) {
                    info!(
                        "quorum reached for tick {} with stake {}/{} on roots comm={} state={} evm={}",
                        tick,
                        stake,
                        oscillator.stake_table.total_stake(),
                        hex::encode(view.commutative_root),
                        hex::encode(view.stateful_root),
                        hex::encode(view.evm_root),
                    );
                }

                info!(
                    "synthesis: commutative={} stateful={} evm={} rejected={} latency_ms={:.2} throughput={:.1}",
                    result.commutative_applied,
                    result.stateful_applied,
                    result.evm_applied,
                    result.stateful_rejected.len(),
                    result.avg_latency_ms,
                    result.throughput_per_sec,
                );
                for err in &result.stateful_rejected {
                    tracing::warn!("stateful shift rejected: {:?}", err);
                }
            }
            _ = shutdown.recv() => {
                info!("SIGTERM received, saving snapshot and shutting down");
                if let Err(e) = persistence::save(&oscillator, persistence::snapshot_path()) {
                    warn!("final snapshot save failed: {}", e);
                }
                std::process::exit(0);
            }
        }
    }
}
