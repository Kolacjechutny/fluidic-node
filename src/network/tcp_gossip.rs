use crate::crypto::Signal;
use crate::network::node::encode_packet;
use bytes::{Buf, Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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
    pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (outbound_tx, outbound_rx) = mpsc::channel(4096);
        let (peer_tx, peer_rx) = mpsc::channel(64);
        let (inbound_tx, inbound_rx) = mpsc::channel(4096);
        let local_addr = listener.local_addr()?;

        tokio::spawn(run_gossip(listener, inbound_tx, outbound_rx, peer_rx));

        Ok(Self {
            local_addr,
            outbound: outbound_tx,
            peer_tx,
            inbound: inbound_rx,
        })
    }

    /// Request a connection to a peer. The connection is established
    /// asynchronously by the gossip loop.
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
) {
    let (writer_tx, mut writer_rx) = mpsc::channel::<WriteHalf<TcpStream>>(64);

    // Accept inbound connections forever.
    let tx = writer_tx.clone();
    let inbound = inbound_tx.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("accepted connection from {}", addr);
                    let (read, write) = tokio::io::split(stream);
                    let _ = tx.send(write).await;
                    tokio::spawn(read_loop(read, addr, inbound.clone()));
                }
                Err(e) => {
                    warn!("accept error: {}", e);
                }
            }
        }
    });

    // Dial outbound peers requested via add_peer.
    let tx = writer_tx.clone();
    let inbound = inbound_tx.clone();
    tokio::spawn(async move {
        while let Some(peer) = peer_rx.recv().await {
            let tx = tx.clone();
            let inbound = inbound.clone();
            tokio::spawn(async move {
                match TcpStream::connect(peer).await {
                    Ok(stream) => {
                        info!("connected to peer {}", peer);
                        let (read, write) = tokio::io::split(stream);
                        let _ = tx.send(write).await;
                        tokio::spawn(read_loop(read, peer, inbound));
                    }
                    Err(e) => {
                        warn!("failed to connect to peer {}: {}", peer, e);
                    }
                }
            });
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
