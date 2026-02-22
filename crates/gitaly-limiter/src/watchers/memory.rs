use super::{is_threshold_exceeded, PressureWatcher};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemoryWatcher {
    pressure_threshold_percent: f64,
    usage_percent: f64,
}

impl MemoryWatcher {
    #[must_use]
    pub fn new(pressure_threshold_percent: f64) -> Self {
        Self {
            pressure_threshold_percent,
            usage_percent: 0.0,
        }
    }

    pub fn set_usage_percent(&mut self, usage_percent: f64) {
        self.usage_percent = usage_percent;
    }

    #[must_use]
    pub fn pressure_threshold_percent(&self) -> f64 {
        self.pressure_threshold_percent
    }

    #[must_use]
    pub fn usage_percent(&self) -> f64 {
        self.usage_percent
    }

    #[must_use]
    pub fn is_pressure(usage_percent: f64, threshold_percent: f64) -> bool {
        is_threshold_exceeded(usage_percent, threshold_percent)
    }
}

impl PressureWatcher for MemoryWatcher {
    fn is_under_pressure(&self) -> bool {
        Self::is_pressure(self.usage_percent, self.pressure_threshold_percent)
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryWatcher;
    use crate::watchers::PressureWatcher;

    #[test]
    fn threshold_logic_uses_greater_or_equal() {
        assert!(!MemoryWatcher::is_pressure(74.9, 75.0));
        assert!(MemoryWatcher::is_pressure(75.0, 75.0));
        assert!(MemoryWatcher::is_pressure(90.0, 75.0));
    }

    #[test]
    fn reports_pressure_from_current_usage() {
        let mut watcher = MemoryWatcher::new(90.0);
        watcher.set_usage_percent(89.0);
        assert!(!watcher.is_under_pressure());

        watcher.set_usage_percent(90.0);
        assert!(watcher.is_under_pressure());
    }
}
