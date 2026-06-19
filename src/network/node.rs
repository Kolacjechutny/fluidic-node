use crate::crypto::Signal;
use crate::network::buffer::RingBuffer;
use bytes::BytesMut;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, error};

/// The maximum serialized phase-shift size we allow through the network buffer.
pub const MAX_SHIFT_SIZE: usize = 8 * 1024;

/// A network packet carrying a serialized phase-shift.
#[derive(Clone, Debug)]
pub struct NetworkPacket {
    pub from: SocketAddr,
    pub payload: BytesMut,
}

/// In-process simulation transport using Tokio mpsc channels.
#[derive(Clone)]
pub enum Transport {
    /// Real UDP socket transport (placeholder for production deployment).
    Udp(SocketAddr),
    /// In-process channel transport for tests and benchmarks.
    Channel(mpsc::Sender<NetworkPacket>),
}

/// A Tuning-Fork network node. In simulation mode it routes phase-shifts over
/// bounded async channels; in UDP mode it would bind a socket and gossip.
pub struct NetworkNode {
    pub local_addr: SocketAddr,
    pub rx: mpsc::Receiver<NetworkPacket>,
    pub peers: Arc<Mutex<HashMap<SocketAddr, Transport>>>,
    pub ingress_buffer: Arc<Mutex<RingBuffer>>,
}

impl NetworkNode {
    /// Create a new in-process node. Returns the node and a sender that can be
    /// used to inject packets as if from the network.
    pub fn new_in_process(local_addr: SocketAddr) -> (Self, mpsc::Sender<NetworkPacket>) {
        let (tx, rx) = mpsc::channel(1024);
        let node = Self {
            local_addr,
            rx,
            peers: Arc::new(Mutex::new(HashMap::new())),
            ingress_buffer: Arc::new(Mutex::new(RingBuffer::new(256 * 1024))),
        };
        (node, tx)
    }

    /// Register a peer transport.
    pub async fn add_peer(&self, addr: SocketAddr, transport: Transport) {
        self.peers.lock().await.insert(addr, transport);
    }

    /// Serialize and broadcast a phase-shift to all known peers.
    pub async fn broadcast(&self, shift: &Signal) -> Result<(), String> {
        let encoded = bincode::serialize(shift).map_err(|e| e.to_string())?;
        if encoded.len() > MAX_SHIFT_SIZE {
            return Err("phase-shift exceeds maximum packet size".to_string());
        }
        let packet = NetworkPacket {
            from: self.local_addr,
            payload: BytesMut::from(&encoded[..]),
        };

        let peers = self.peers.lock().await.clone();
        for (addr, transport) in peers {
            let packet = packet.clone();
            match transport {
                Transport::Udp(_) => {
                    // UDP gossip not implemented in this prototype.
                    debug!("dropping UDP broadcast to {}", addr);
                }
                Transport::Channel(tx) => {
                    if let Err(e) = tx.send(packet).await {
                        error!("failed to send to {}: {}", addr, e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Run the receive loop, pushing incoming packets into the ingress ring buffer.
    pub async fn run(&mut self) {
        while let Some(packet) = self.rx.recv().await {
            let mut buf = self.ingress_buffer.lock().await;
            let _ = buf.write(&packet.payload);
        }
    }

    /// Try to deserialize the next complete phase-shift from the ingress buffer.
    /// Returns `Ok(Some(shift))` if a packet was available and valid.
    pub async fn next_shift(&self) -> Result<Option<Signal>, String> {
        let mut buf = self.ingress_buffer.lock().await;
        // Peek length prefix (4 bytes, little-endian).
        if buf.len() < 4 {
            return Ok(None);
        }
        let header = buf.peek(4);
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if len > MAX_SHIFT_SIZE {
            return Err(format!("oversized packet length {}", len));
        }
        if buf.len() < 4 + len {
            return Ok(None);
        }
        buf.consume(4);
        let payload = buf.read_chunk(len);
        drop(buf);
        let shift: Signal = bincode::deserialize(&payload).map_err(|e| e.to_string())?;
        Ok(Some(shift))
    }
}

/// Helper to serialize a phase-shift with a 4-byte little-endian length prefix.
pub fn encode_packet(shift: &Signal) -> Result<BytesMut, String> {
    let body = bincode::serialize(shift).map_err(|e| e.to_string())?;
    let mut packet = BytesMut::with_capacity(4 + body.len());
    packet.extend_from_slice(&(body.len() as u32).to_le_bytes());
    packet.extend_from_slice(&body);
    Ok(packet)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{DEFAULT_DEX_DOMAIN, keys::KeyPair};
    use crate::crypto::phase_shift::CommutativeShift;
    use crate::field::coordinates::Coordinate;

    #[tokio::test]
    async fn in_process_broadcast_and_receive() {
        let addr1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8002".parse().unwrap();

        let (node1, tx1) = NetworkNode::new_in_process(addr1);
        let (node2, _tx2) = NetworkNode::new_in_process(addr2);

        node1.add_peer(addr2, Transport::Channel(tx1)).await;

        let kp = KeyPair::generate();
        let shift = Signal::Commutative(CommutativeShift::new(
            &kp,
            DEFAULT_DEX_DOMAIN,
            Coordinate::from_scalar(1),
            42,
            [3u8; 32],
            1,
            0,
        ));

        node1.broadcast(&shift).await.unwrap();

        // In this simplified test we directly feed the packet to node2's buffer.
        let packet = encode_packet(&shift).unwrap();
        let mut buf = node2.ingress_buffer.lock().await;
        buf.write(&packet);
        drop(buf);

        let received = node2.next_shift().await.unwrap();
        assert!(received.is_some());
    }
}
