use bytes::BytesMut;

/// A fixed-capacity ring buffer backed by `BytesMut`. It is intended for
/// zero-copy ingestion of serialized phase-shifts into an oscillator node.
pub struct RingBuffer {
    buf: BytesMut,
    capacity: usize,
    head: usize,
    tail: usize,
    len: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            capacity,
            head: 0,
            tail: 0,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    /// Write `data` into the ring buffer. Returns the number of bytes written.
    pub fn write(&mut self, data: &[u8]) -> usize {
        let to_write = data.len().min(self.capacity - self.len);
        self.buf.resize(self.capacity, 0);
        for (i, &byte) in data.iter().take(to_write).enumerate() {
            let idx = (self.tail + i) % self.capacity;
            self.buf[idx] = byte;
        }
        self.tail = (self.tail + to_write) % self.capacity;
        self.len += to_write;
        to_write
    }

    /// Read up to `max` bytes without consuming them.
    pub fn peek(&self, max: usize) -> Vec<u8> {
        let to_read = max.min(self.len);
        let mut out = Vec::with_capacity(to_read);
        for i in 0..to_read {
            let idx = (self.head + i) % self.capacity;
            out.push(self.buf[idx]);
        }
        out
    }

    /// Consume `n` bytes from the front of the buffer.
    pub fn consume(&mut self, n: usize) {
        let consumed = n.min(self.len);
        self.head = (self.head + consumed) % self.capacity;
        self.len -= consumed;
    }

    /// Take a contiguous view of up to `max` bytes from the head, advancing it.
    pub fn read_chunk(&mut self, max: usize) -> Vec<u8> {
        let data = self.peek(max);
        self.consume(data.len());
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_wraps() {
        let mut rb = RingBuffer::new(8);
        assert_eq!(rb.write(b"hello"), 5);
        assert_eq!(rb.write(b"world"), 3); // only "wor" fits
        assert_eq!(rb.read_chunk(5), b"hello".to_vec());
        assert_eq!(rb.read_chunk(3), b"wor".to_vec());
    }

    #[test]
    fn ring_buffer_peek_does_not_consume() {
        let mut rb = RingBuffer::new(16);
        rb.write(b"fluidic");
        assert_eq!(rb.peek(4), b"flui".to_vec());
        assert_eq!(rb.len(), 7);
    }
}
