use super::{is_threshold_exceeded, PressureWatcher};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuWatcher {
    pressure_threshold_percent: f64,
    usage_percent: f64,
}

impl CpuWatcher {
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

impl PressureWatcher for CpuWatcher {
    fn is_under_pressure(&self) -> bool {
        Self::is_pressure(self.usage_percent, self.pressure_threshold_percent)
    }
}

#[cfg(test)]
mod tests {
    use super::CpuWatcher;
    use crate::watchers::PressureWatcher;

    #[test]
    fn threshold_logic_uses_greater_or_equal() {
        assert!(!CpuWatcher::is_pressure(69.9, 70.0));
        assert!(CpuWatcher::is_pressure(70.0, 70.0));
        assert!(CpuWatcher::is_pressure(80.0, 70.0));
    }

    #[test]
    fn reports_pressure_from_current_usage() {
        let mut watcher = CpuWatcher::new(85.0);
        watcher.set_usage_percent(84.0);
        assert!(!watcher.is_under_pressure());

        watcher.set_usage_percent(85.0);
        assert!(watcher.is_under_pressure());
    }
}
