use crate::crypto::Signal;
use crate::network::node::encode_packet;
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{error, info, trace, warn};

/// Authentication challenge used for the gossip pre-shared-key handshake.
const AUTH_CHALLENGE: &[u8] = b"fluidic:gossip:auth:v1";

/// Maximum serialized phase-shift size.
const MAX_PACKET_SIZE: usize = 64 * 1024;

/// A TCP-based gossip peer. Each connection is a bidirectional length-prefixed
/// stream of `Signal` messages.
pub struct TcpGossipNode {
    pub local_addr: SocketAddr,
    /// Send channel for outbound phase-shifts to all connected peers.
    pub outbound: mpsc::Sender<Signal>,
    /// Send channel to register new outbound peer connections.
    peer_tx: mpsc::Sender<SocketAddr>,
    /// Receive channel for inbound phase-shifts from all connected peers.
    pub inbound: mpsc::Receiver<Signal>,
}

impl TcpGossipNode {
    pub async fn bind(addr: SocketAddr, psk: Option<[u8; 32]>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (outbound_tx, outbound_rx) = mpsc::channel(4096);
        let (peer_tx, peer_rx) = mpsc::channel(64);
        let (inbound_tx, inbound_rx) = mpsc::channel(4096);
        let local_addr = listener.local_addr()?;

        tokio::spawn(run_gossip(listener, inbound_tx, outbound_rx, peer_rx, psk));

        Ok(Self {
            local_addr,
            outbound: outbound_tx,
            peer_tx,
            inbound: inbound_rx,
        })
    }

    /// Request a connection to a peer. The connection is established
    /// asynchronously by the gossip loop and automatically retried after any
    /// disconnect.
    pub async fn add_peer(&self, addr: SocketAddr) -> Result<(), String> {
        self.peer_tx
            .send(addr)
            .await
            .map_err(|e| format!("peer channel closed: {}", e))
    }

    /// Broadcast a phase-shift to all connected peers.
    pub async fn broadcast(&self, shift: Signal) -> Result<(), String> {
        self.outbound
            .send(shift)
            .await
            .map_err(|e| format!("outbound channel closed: {}", e))
    }
}

async fn run_gossip(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<Signal>,
    mut outbound_rx: mpsc::Receiver<Signal>,
    mut peer_rx: mpsc::Receiver<SocketAddr>,
    psk: Option<[u8; 32]>,
) {
    let (writer_tx, mut writer_rx) = mpsc::channel::<WriteHalf<TcpStream>>(64);
    let dial_peers: Arc<Mutex<HashSet<SocketAddr>>> = Arc::new(Mutex::new(HashSet::new()));
    let active_peers: Arc<Mutex<HashSet<SocketAddr>>> = Arc::new(Mutex::new(HashSet::new()));

    let auth_proof = psk.map(|key| {
        let mut hasher = blake3::Hasher::new_keyed(&key);
        hasher.update(AUTH_CHALLENGE);
        *hasher.finalize().as_bytes()
    });

    // Accept inbound connections forever.
    let tx = writer_tx.clone();
    let inbound = inbound_tx.clone();
    let inbound_psk = auth_proof;
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("accepted connection from {}", addr);
                    let (read, write) = tokio::io::split(stream);
                    if let Some(proof) = inbound_psk {
                        // Inbound peers must authenticate before we accept signals.
                        tokio::spawn(handshake_inbound(
                            read,
                            write,
                            addr,
                            proof,
                            tx.clone(),
                            inbound.clone(),
                        ));
                    } else {
                        let _ = tx.send(write).await;
                        tokio::spawn(read_loop(read, addr, inbound.clone()));
                    }
                }
                Err(e) => {
                    warn!("accept error: {}", e);
                }
            }
        }
    });

    // Dial outbound peers requested via add_peer and reconnect automatically.
    let tx = writer_tx.clone();
    let inbound = inbound_tx.clone();
    let dial_peers_task = dial_peers.clone();
    let active_peers_task = active_peers.clone();
    let outbound_psk = auth_proof;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                Some(peer) = peer_rx.recv() => {
                    dial_peers_task.lock().unwrap().insert(peer);
                    try_connect(peer, tx.clone(), inbound.clone(), active_peers_task.clone(), outbound_psk).await;
                }
                _ = ticker.tick() => {
                    let to_retry: Vec<SocketAddr> = {
                        let dial = dial_peers_task.lock().unwrap();
                        let active = active_peers_task.lock().unwrap();
                        dial.difference(&active).copied().collect()
                    };
                    for peer in to_retry {
                        try_connect(peer, tx.clone(), inbound.clone(), active_peers_task.clone(), outbound_psk).await;
                    }
                }
                else => break,
            }
        }
    });

    // Fan-out loop: distribute outbound shifts to all connected peers.
    let mut writers: Vec<WriteHalf<TcpStream>> = Vec::new();
    loop {
        tokio::select! {
            Some(writer) = writer_rx.recv() => {
                writers.push(writer);
            }
            Some(shift) = outbound_rx.recv() => {
                let packet = match encode_packet(&shift) {
                    Ok(p) => Bytes::from(p),
                    Err(e) => {
                        error!("encode error: {}", e);
                        continue;
                    }
                };
                let mut disconnected = Vec::new();
                for (i, writer) in writers.iter_mut().enumerate() {
                    if let Err(e) = writer.write_all(&packet).await {
                        warn!("peer write error: {}", e);
                        disconnected.push(i);
                    } else if let Err(e) = writer.flush().await {
                        warn!("peer flush error: {}", e);
                        disconnected.push(i);
                    }
                }
                for i in disconnected.into_iter().rev() {
                    writers.remove(i);
                }
            }
            else => break,
        }
    }
}

async fn try_connect(
    peer: SocketAddr,
    writer_tx: mpsc::Sender<WriteHalf<TcpStream>>,
    inbound_tx: mpsc::Sender<Signal>,
    active_peers: Arc<Mutex<HashSet<SocketAddr>>>,
    auth_proof: Option<[u8; 32]>,
) {
    {
        let active = active_peers.lock().unwrap();
        if active.contains(&peer) {
            return;
        }
    }
    match TcpStream::connect(peer).await {
        Ok(stream) => {
            info!("connected to peer {}", peer);
            let (read, mut write) = tokio::io::split(stream);

            // If a PSK is configured, send the authentication proof first.
            if let Some(proof) = auth_proof {
                if let Err(e) = send_auth(&mut write, proof).await {
                    warn!("failed to send auth to {}: {}", peer, e);
                    return;
                }
            }

            let _ = writer_tx.send(write).await;
            active_peers.lock().unwrap().insert(peer);
            let active = active_peers.clone();
            tokio::spawn(async move {
                read_loop(read, peer, inbound_tx).await;
                active.lock().unwrap().remove(&peer);
                info!("peer {} disconnected, will retry", peer);
            });
        }
        Err(e) => {
            trace!("failed to connect to peer {}: {}", peer, e);
        }
    }
}

async fn send_auth(writer: &mut WriteHalf<TcpStream>, proof: [u8; 32]) -> std::io::Result<()> {
    let signal = Signal::Auth { proof };
    let packet = encode_packet(&signal).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("encode: {}", e))
    })?;
    writer.write_all(&packet).await?;
    writer.flush().await
}

async fn handshake_inbound(
    mut read: ReadHalf<TcpStream>,
    write: WriteHalf<TcpStream>,
    addr: SocketAddr,
    expected_proof: [u8; 32],
    writer_tx: mpsc::Sender<WriteHalf<TcpStream>>,
    inbound_tx: mpsc::Sender<Signal>,
) {
    match read_one_signal(&mut read).await {
        Some(Signal::Auth { proof }) => {
            if proof != expected_proof {
                warn!("inbound peer {} failed auth", addr);
                return;
            }
            info!("inbound peer {} authenticated", addr);
            let _ = writer_tx.send(write).await;
            read_loop(read, addr, inbound_tx).await;
        }
        Some(other) => {
            warn!(
                "inbound peer {} sent {} before auth; disconnecting",
                addr,
                signal_name(&other)
            );
        }
        None => {
            trace!("inbound peer {} disconnected during auth", addr);
        }
    }
}

fn signal_name(signal: &Signal) -> &'static str {
    match signal {
        Signal::Commutative(_) => "Commutative",
        Signal::Stateful(_) => "Stateful",
        Signal::Registration(_) => "Registration",
        Signal::Stake(_) => "Stake",
        Signal::Ping { .. } => "Ping",
        Signal::Pong { .. } => "Pong",
        Signal::Certificate(_) => "Certificate",
        Signal::Auth { .. } => "Auth",
    }
}

/// Read a single length-prefixed signal from a stream.
async fn read_one_signal(read: &mut ReadHalf<TcpStream>) -> Option<Signal> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    while buf.len() < 4 {
        if read.read_buf(&mut buf).await.unwrap_or(0) == 0 {
            return None;
        }
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.advance(4);
    if len > MAX_PACKET_SIZE {
        return None;
    }
    while buf.len() < len {
        if read.read_buf(&mut buf).await.unwrap_or(0) == 0 {
            return None;
        }
    }
    let payload = buf.split_to(len).freeze();
    bincode::deserialize::<Signal>(&payload).ok()
}

/// Read length-prefixed phase-shifts from a stream and forward them.
async fn read_loop(
    mut read: ReadHalf<TcpStream>,
    addr: SocketAddr,
    inbound_tx: mpsc::Sender<Signal>,
) {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        while buf.len() < 4 {
            if read.read_buf(&mut buf).await.unwrap_or(0) == 0 {
                info!("peer {} disconnected", addr);
                return;
            }
        }
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        buf.advance(4);
        if len > MAX_PACKET_SIZE {
            warn!("oversized packet {} from {}", len, addr);
            return;
        }
        while buf.len() < len {
            if read.read_buf(&mut buf).await.unwrap_or(0) == 0 {
                return;
            }
        }
        let payload = buf.split_to(len).freeze();
        match bincode::deserialize::<Signal>(&payload) {
            Ok(shift) => {
                if inbound_tx.send(shift).await.is_err() {
                    return;
                }
            }
            Err(e) => warn!("deserialize error from {}: {}", addr, e),
        }
    }
}
