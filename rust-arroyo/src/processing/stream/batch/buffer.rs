/// Accumulates items for batching. Implementations define how items
/// are stored, how their byte size is measured, and what the flushed
/// output type is.
///
/// The batch stage calls `push()` for each item, uses the returned
/// byte count to track size-based thresholds, and calls `flush()`
/// when a threshold is reached.
pub trait Buffer<T>: Send + Sync {
    /// The type returned by `flush()`. For collecting buffers this is `Vec<T>`.
    /// For merging buffers this can be a single aggregated value.
    type Output: Send + Sync;

    /// Add an item to the buffer. Returns the item's size in bytes.
    fn push(&mut self, item: T) -> u64;

    /// Number of items in the buffer.
    fn len(&self) -> u64;

    /// Whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drain the buffer and return the accumulated output.
    fn flush(&mut self) -> Self::Output;
}
