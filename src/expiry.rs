use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
pub(crate) enum ExpiryClock {
    System,
    Fixed(u64),
}

impl ExpiryClock {
    pub(crate) fn is_expired(self, expires_at_unix_ms: u64) -> bool {
        expires_at_unix_ms != 0 && expires_at_unix_ms <= self.now_unix_ms()
    }

    fn now_unix_ms(self) -> u64 {
        match self {
            Self::System => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            Self::Fixed(now_unix_ms) => now_unix_ms,
        }
    }
}
