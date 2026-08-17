/// Accumulates items for batching. Implementations define how items
/// are stored and how their byte size is measured.
///
/// The batch stage calls `push()` for each item, uses the returned
/// byte count to track size-based thresholds, and calls `flush()`
/// when a threshold is reached.
pub trait Buffer<T>: Send + Sync {
    /// Add an item to the buffer. Returns the item's size in bytes.
    fn push(&mut self, item: T) -> u64;

    /// Number of items in the buffer.
    fn len(&self) -> u64;

    /// Whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drain the buffer and return all accumulated items.
    fn flush(&mut self) -> Vec<T>;
}

/// Simple buffer that stores items in a Vec. Every item counts as 1 byte
/// (effectively making byte-based triggering equivalent to count-based).
pub struct VecBuffer<T> {
    items: Vec<T>,
}

impl<T> VecBuffer<T> {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }
}

impl<T: Send + Sync> Buffer<T> for VecBuffer<T> {
    fn push(&mut self, item: T) -> u64 {
        self.items.push(item);
        1 // each item counts as 1 "byte" — use a custom Buffer for real byte sizing
    }

    fn len(&self) -> u64 {
        self.items.len() as u64
    }

    fn flush(&mut self) -> Vec<T> {
        std::mem::take(&mut self.items)
    }
}
