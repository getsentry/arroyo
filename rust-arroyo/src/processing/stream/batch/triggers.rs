/// A monotonically increasing counter that triggers when a threshold is reached.
///
/// Used by batch stages to determine when to flush — e.g., when accumulated
/// row count or byte size exceeds a configured limit.
///
/// Intentionally has no reset — create a new instance for a fresh counter.
///
/// ```
/// use sentry_arroyo::processing::stream::triggers::SizeTrigger;
///
/// let mut trigger = SizeTrigger::new(100);
/// trigger.increment(30);
/// trigger.increment(50);
/// assert!(!trigger.is_complete());
/// assert_eq!(trigger.watermark(), 80);
///
/// trigger.increment(25);
/// assert!(trigger.is_complete());
/// assert_eq!(trigger.watermark(), 105);
/// ```
pub struct SizeTrigger {
    threshold: u64,
    accumulated: u64,
}

impl SizeTrigger {
    pub fn new(threshold: u64) -> Self {
        Self {
            threshold,
            accumulated: 0,
        }
    }

    pub fn increment(&mut self, quantity: u64) {
        self.accumulated += quantity;
    }

    pub fn watermark(&self) -> u64 {
        self.accumulated
    }

    pub fn is_complete(&self) -> bool {
        self.accumulated >= self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_trigger_basic() {
        let mut trigger = SizeTrigger::new(100);
        assert_eq!(trigger.watermark(), 0);
        assert!(!trigger.is_complete());

        trigger.increment(50);
        assert_eq!(trigger.watermark(), 50);
        assert!(!trigger.is_complete());

        trigger.increment(50);
        assert_eq!(trigger.watermark(), 100);
        assert!(trigger.is_complete());
    }

    #[test]
    fn test_size_trigger_overflow() {
        let mut trigger = SizeTrigger::new(100);
        trigger.increment(150);
        assert_eq!(trigger.watermark(), 150);
        assert!(trigger.is_complete());
    }

    #[test]
    fn test_size_trigger_zero_threshold() {
        let trigger = SizeTrigger::new(0);
        assert!(trigger.is_complete());
    }
}
