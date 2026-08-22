//! Bounded append-batch planning.
//!
//! The planner is deliberately independent from queueing and I/O.  It keeps
//! every record individually decodable under Format V1 and assigns any batch
//! tail padding to the last record.  Direct-I/O mode rounds the resulting
//! append cursor to 4 KiB, so at most the first batch after reopening an older
//! 32-byte-aligned active region needs the buffered fallback.

use crate::format::RECORD_ALIGNMENT;
use crate::index::MAX_RECORD_LEN;
use crate::io_backend::DIRECT_IO_ALIGNMENT;

pub(crate) const MAX_BATCH_BYTES: usize = 128 * 1024;
pub(crate) const MAX_BATCH_RECORDS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BatchPlan {
    /// Number of leading input records selected for this batch.
    pub(crate) records: usize,
    /// Encoded length for each selected record. Only the final value may be
    /// larger than its input minimum.
    pub(crate) record_lengths: Vec<u32>,
    /// One contiguous positioned write covers this many bytes.
    pub(crate) write_len: usize,
}

/// Select the largest bounded prefix that fits the active region.
///
/// `minimum_lengths` must contain valid Format V1 record lengths.  Invalid
/// input returns `None` instead of relying on debug assertions in production.
pub(crate) fn plan_batch(
    minimum_lengths: &[u32],
    start_offset: u64,
    remaining: u64,
    align_end_for_direct: bool,
) -> Option<BatchPlan> {
    if minimum_lengths.is_empty() || remaining == 0 {
        return None;
    }

    let batch_limit = remaining.min(MAX_BATCH_BYTES as u64);
    let mut selected = Vec::with_capacity(minimum_lengths.len().min(MAX_BATCH_RECORDS));
    let mut total = 0_u64;
    for &record_len in minimum_lengths.iter().take(MAX_BATCH_RECORDS) {
        if record_len == 0
            || record_len as usize % RECORD_ALIGNMENT != 0
            || record_len > MAX_RECORD_LEN
        {
            return None;
        }
        let candidate = total.checked_add(u64::from(record_len))?;
        // The byte cap controls coalescing, not maximum object size. A single
        // large record remains a valid one-record batch.
        if (!selected.is_empty() && candidate > batch_limit) || candidate > remaining {
            break;
        }
        selected.push(record_len);
        total = candidate;
    }
    if selected.is_empty() {
        return None;
    }

    if align_end_for_direct {
        loop {
            let end = start_offset.checked_add(total)?;
            let padding = alignment_padding(end, DIRECT_IO_ALIGNMENT as u64);
            let padded_total = total.checked_add(padding)?;
            let last = u64::from(*selected.last()?);
            let padded_last = last.checked_add(padding)?;
            let within_batch_limit = padded_total <= MAX_BATCH_BYTES as u64 || selected.len() == 1;
            if padded_total <= remaining
                && within_batch_limit
                && padded_last <= u64::from(MAX_RECORD_LEN)
            {
                *selected.last_mut()? = u32::try_from(padded_last).ok()?;
                total = padded_total;
                break;
            }
            // Format V1 can represent a record up to MAX_RECORD_LEN, while
            // adding a 4 KiB tail may exceed that packed limit. Keep a valid
            // unpadded single record: the runtime routes it through the
            // buffered compatibility descriptor instead of rotating forever.
            if selected.len() == 1 && padded_last > u64::from(MAX_RECORD_LEN) {
                break;
            }
            total = total.checked_sub(u64::from(selected.pop()?))?;
            if selected.is_empty() {
                return None;
            }
        }
    }

    Some(BatchPlan {
        records: selected.len(),
        record_lengths: selected,
        write_len: usize::try_from(total).ok()?,
    })
}

fn alignment_padding(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    value.wrapping_neg() & (alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_batch_is_a_bounded_prefix_without_extra_padding() {
        let plan = plan_batch(&[96, 128, 160], 4096, 4096, false).unwrap();
        assert_eq!(plan.records, 3);
        assert_eq!(plan.record_lengths, vec![96, 128, 160]);
        assert_eq!(plan.write_len, 384);
    }

    #[test]
    fn direct_batch_pads_only_the_final_record_and_aligns_next_cursor() {
        let plan = plan_batch(&[96, 128, 160], 4096, 8192, true).unwrap();
        assert_eq!(plan.records, 3);
        assert_eq!(&plan.record_lengths[..2], &[96, 128]);
        assert_eq!(plan.write_len, 4096);
        assert_eq!(
            plan.record_lengths
                .iter()
                .map(|v| *v as usize)
                .sum::<usize>(),
            4096
        );
        assert_eq!((4096 + plan.write_len) % DIRECT_IO_ALIGNMENT, 0);
    }

    #[test]
    fn direct_batch_can_realign_an_older_unaligned_active_cursor() {
        let plan = plan_batch(&[96], 4096 + 96, 8192, true).unwrap();
        assert_eq!(plan.write_len, 4000);
        assert_eq!((4096 + 96 + plan.write_len) % DIRECT_IO_ALIGNMENT, 0);
        assert_eq!(plan.record_lengths, vec![4000]);
    }

    #[test]
    fn planner_reduces_the_prefix_when_tail_padding_does_not_fit() {
        let plan = plan_batch(&[2048, 2048], 4096, 4096, true).unwrap();
        assert_eq!(plan.records, 2);
        assert_eq!(plan.write_len, 4096);

        assert!(plan_batch(&[4064], 4096, 4064, true).is_none());
    }

    #[test]
    fn invalid_record_lengths_are_rejected() {
        assert!(plan_batch(&[0], 4096, 4096, false).is_none());
        assert!(plan_batch(&[65], 4096, 4096, false).is_none());
        assert!(plan_batch(&[MAX_RECORD_LEN + 1], 4096, u64::MAX, false).is_none());
    }

    #[test]
    fn one_large_record_is_not_rejected_by_the_coalescing_cap() {
        let record_len = (MAX_BATCH_BYTES + RECORD_ALIGNMENT) as u32;
        let plan = plan_batch(&[record_len, 96], 4096, 1024 * 1024, false).unwrap();
        assert_eq!(plan.records, 1);
        assert_eq!(plan.record_lengths, vec![record_len]);
        assert_eq!(plan.write_len, record_len as usize);
    }

    #[test]
    fn maximum_format_record_falls_back_without_tail_padding() {
        let plan = plan_batch(
            &[MAX_RECORD_LEN],
            DIRECT_IO_ALIGNMENT as u64,
            u64::from(MAX_RECORD_LEN) + DIRECT_IO_ALIGNMENT as u64,
            true,
        )
        .unwrap();
        assert_eq!(plan.record_lengths, vec![MAX_RECORD_LEN]);
        assert_eq!(plan.write_len, MAX_RECORD_LEN as usize);
    }
}
