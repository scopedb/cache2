//! Explicit upstream-fill protection for cache miss storms.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const NANOS_PER_TOKEN: u128 = 1_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OriginFillConfig {
    /// Maximum fills admitted per second. The burst is one second of tokens.
    pub fills_per_second: u64,
    /// Hard cap on fills that have been admitted but whose permit is not dropped.
    pub max_in_flight: usize,
}

impl OriginFillConfig {
    pub const fn new(fills_per_second: u64, max_in_flight: usize) -> Self {
        Self {
            fills_per_second,
            max_in_flight,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OriginFillRejectReason {
    Disabled,
    RateLimited,
    ConcurrencyLimited,
}

impl fmt::Display for OriginFillRejectReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Disabled => "origin fill protection is not configured",
            Self::RateLimited => "origin fill rate limit reached",
            Self::ConcurrencyLimited => "origin fill concurrency limit reached",
        })
    }
}

impl std::error::Error for OriginFillRejectReason {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OriginFillStats {
    pub attempts: u64,
    pub admitted: u64,
    pub rate_limited: u64,
    pub concurrency_limited: u64,
    pub in_flight: u64,
    pub in_flight_peak: u64,
}

/// RAII permit for one cache-miss fill against the authoritative data source.
///
/// Acquire this only after a cache miss, hold it until the fill completes, and
/// shed or defer work when acquisition is rejected. This keeps miss storms
/// from becoming an unbounded upstream fan-out.
pub struct OriginFillPermit {
    limiter: Arc<OriginFillLimiter>,
}

impl fmt::Debug for OriginFillPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginFillPermit")
            .finish_non_exhaustive()
    }
}

impl Drop for OriginFillPermit {
    fn drop(&mut self) {
        self.limiter.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct OriginFillLimiter {
    config: OriginFillConfig,
    bucket: Mutex<TokenBucket>,
    attempts: AtomicU64,
    admitted: AtomicU64,
    rate_limited: AtomicU64,
    concurrency_limited: AtomicU64,
    in_flight: AtomicUsize,
    in_flight_peak: AtomicU64,
}

impl OriginFillLimiter {
    pub(crate) fn try_new(config: OriginFillConfig) -> Result<Arc<Self>, &'static str> {
        if config.fills_per_second == 0 {
            return Err("origin fills_per_second must be greater than zero");
        }
        if config.max_in_flight == 0 {
            return Err("origin max_in_flight must be greater than zero");
        }
        let capacity = u128::from(config.fills_per_second)
            .checked_mul(NANOS_PER_TOKEN)
            .ok_or("origin fill rate is too large")?;
        Ok(Arc::new(Self {
            config,
            bucket: Mutex::new(TokenBucket {
                credit: capacity,
                capacity,
                last_refill: Instant::now(),
            }),
            attempts: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            concurrency_limited: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            in_flight_peak: AtomicU64::new(0),
        }))
    }

    pub(crate) fn try_acquire(
        self: &Arc<Self>,
    ) -> Result<OriginFillPermit, OriginFillRejectReason> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.config.max_in_flight {
                self.concurrency_limited.fetch_add(1, Ordering::Relaxed);
                return Err(OriginFillRejectReason::ConcurrencyLimited);
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        self.in_flight_peak
            .fetch_max((current + 1) as u64, Ordering::Relaxed);

        let now = Instant::now();
        let mut bucket = self
            .bucket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let elapsed = now.duration_since(bucket.last_refill).as_nanos();
        let refill = elapsed.saturating_mul(u128::from(self.config.fills_per_second));
        bucket.credit = bucket.credit.saturating_add(refill).min(bucket.capacity);
        bucket.last_refill = now;
        if bucket.credit < NANOS_PER_TOKEN {
            drop(bucket);
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.rate_limited.fetch_add(1, Ordering::Relaxed);
            return Err(OriginFillRejectReason::RateLimited);
        }
        bucket.credit -= NANOS_PER_TOKEN;
        drop(bucket);
        self.admitted.fetch_add(1, Ordering::Relaxed);
        Ok(OriginFillPermit {
            limiter: Arc::clone(self),
        })
    }

    pub(crate) fn snapshot(&self) -> OriginFillStats {
        OriginFillStats {
            attempts: self.attempts.load(Ordering::Relaxed),
            admitted: self.admitted.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            concurrency_limited: self.concurrency_limited.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Acquire) as u64,
            in_flight_peak: self.in_flight_peak.load(Ordering::Relaxed),
        }
    }
}

struct TokenBucket {
    /// Token credit in token-nanoseconds, preserving fractional refill.
    credit: u128,
    capacity: u128,
    last_refill: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_enforces_rate_and_concurrency_without_leaking_permits() {
        let limiter = OriginFillLimiter::try_new(OriginFillConfig::new(2, 1)).unwrap();
        let first = limiter.try_acquire().unwrap();
        assert!(matches!(
            limiter.try_acquire(),
            Err(OriginFillRejectReason::ConcurrencyLimited)
        ));
        drop(first);
        let second = limiter.try_acquire().unwrap();
        drop(second);
        assert!(matches!(
            limiter.try_acquire(),
            Err(OriginFillRejectReason::RateLimited)
        ));
        let stats = limiter.snapshot();
        assert_eq!(stats.admitted, 2);
        assert_eq!(stats.concurrency_limited, 1);
        assert_eq!(stats.rate_limited, 1);
        assert_eq!(stats.in_flight, 0);
        assert_eq!(stats.in_flight_peak, 1);
    }
}
