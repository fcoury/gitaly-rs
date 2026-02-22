use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum AdaptiveConfigError {
    #[error("minimum limit must be greater than zero")]
    MinLimitZero,
    #[error("minimum limit {min_limit} cannot exceed maximum limit {max_limit}")]
    MinExceedsMax { min_limit: usize, max_limit: usize },
    #[error("initial limit {initial_limit} must be between {min_limit} and {max_limit}")]
    InitialOutOfRange {
        initial_limit: usize,
        min_limit: usize,
        max_limit: usize,
    },
    #[error("additive step must be greater than zero")]
    AdditiveStepZero,
    #[error("multiplicative decrease factor must be in range (0, 1), got {factor}")]
    InvalidDecreaseFactor { factor: f64 },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveCalculator {
    min_limit: usize,
    max_limit: usize,
    additive_step: usize,
    multiplicative_decrease_factor: f64,
    current_limit: usize,
}

impl AdaptiveCalculator {
    pub fn new(
        min_limit: usize,
        max_limit: usize,
        initial_limit: usize,
        additive_step: usize,
        multiplicative_decrease_factor: f64,
    ) -> Result<Self, AdaptiveConfigError> {
        if min_limit == 0 {
            return Err(AdaptiveConfigError::MinLimitZero);
        }
        if min_limit > max_limit {
            return Err(AdaptiveConfigError::MinExceedsMax {
                min_limit,
                max_limit,
            });
        }
        if !(min_limit..=max_limit).contains(&initial_limit) {
            return Err(AdaptiveConfigError::InitialOutOfRange {
                initial_limit,
                min_limit,
                max_limit,
            });
        }
        if additive_step == 0 {
            return Err(AdaptiveConfigError::AdditiveStepZero);
        }
        if !(0.0..1.0).contains(&multiplicative_decrease_factor) {
            return Err(AdaptiveConfigError::InvalidDecreaseFactor {
                factor: multiplicative_decrease_factor,
            });
        }

        Ok(Self {
            min_limit,
            max_limit,
            additive_step,
            multiplicative_decrease_factor,
            current_limit: initial_limit,
        })
    }

    #[must_use]
    pub fn current_limit(&self) -> usize {
        self.current_limit
    }

    #[must_use]
    pub fn update(&mut self, under_pressure: bool) -> usize {
        if under_pressure {
            let decreased = ((self.current_limit as f64) * self.multiplicative_decrease_factor)
                .floor() as usize;
            self.current_limit = decreased.max(self.min_limit);
        } else {
            let increased = self.current_limit.saturating_add(self.additive_step);
            self.current_limit = increased.min(self.max_limit);
        }

        self.current_limit
    }
}

#[cfg(test)]
mod tests {
    use super::{AdaptiveCalculator, AdaptiveConfigError};

    #[test]
    fn additive_increase_respects_max_bound() {
        let mut calculator = AdaptiveCalculator::new(1, 10, 8, 2, 0.5).expect("valid config");

        assert_eq!(calculator.update(false), 10);
        assert_eq!(calculator.update(false), 10);
    }

    #[test]
    fn multiplicative_decrease_respects_min_bound() {
        let mut calculator = AdaptiveCalculator::new(2, 20, 5, 1, 0.5).expect("valid config");

        assert_eq!(calculator.update(true), 2);
        assert_eq!(calculator.update(true), 2);
    }

    #[test]
    fn increase_then_decrease_behaves_as_aimd() {
        let mut calculator = AdaptiveCalculator::new(1, 32, 8, 3, 0.5).expect("valid config");

        assert_eq!(calculator.update(false), 11);
        assert_eq!(calculator.update(false), 14);
        assert_eq!(calculator.update(true), 7);
        assert_eq!(calculator.update(false), 10);
    }

    #[test]
    fn rejects_invalid_configuration() {
        assert!(matches!(
            AdaptiveCalculator::new(0, 10, 1, 1, 0.5).expect_err("min=0 must fail"),
            AdaptiveConfigError::MinLimitZero
        ));
        assert!(matches!(
            AdaptiveCalculator::new(10, 5, 10, 1, 0.5).expect_err("min>max must fail"),
            AdaptiveConfigError::MinExceedsMax { .. }
        ));
        assert!(matches!(
            AdaptiveCalculator::new(1, 10, 11, 1, 0.5).expect_err("initial out of range"),
            AdaptiveConfigError::InitialOutOfRange { .. }
        ));
        assert!(matches!(
            AdaptiveCalculator::new(1, 10, 5, 0, 0.5).expect_err("step=0 must fail"),
            AdaptiveConfigError::AdditiveStepZero
        ));
        assert!(matches!(
            AdaptiveCalculator::new(1, 10, 5, 1, 1.0).expect_err("factor=1 must fail"),
            AdaptiveConfigError::InvalidDecreaseFactor { .. }
        ));
    }
}
