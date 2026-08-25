use std::hint::black_box;
use std::time::{Duration, Instant};

use hashbrown::HashTable;

#[path = "../src/memory_directory.rs"]
#[allow(dead_code, unused_imports)]
mod memory_directory;

use memory_directory::{DirectoryUpsert, MemoryDirectory};

const DEFAULT_ENTRIES: usize = 7 * 16 * 1024;
const DEFAULT_OPERATIONS: usize = 2_000_000;
const INDEX_STRIDE: usize = 8_191;
const MAX_HIT_RATIO: f64 = 2.0;
const MAX_MISS_RATIO: f64 = 1.25;
const MAX_CHURN_RATIO: f64 = 4.0;

fn main() {
    let entries = setting("CACHE_DIRECTORY_BENCH_ENTRIES", DEFAULT_ENTRIES);
    let operations = setting("CACHE_DIRECTORY_BENCH_OPERATIONS", DEFAULT_OPERATIONS);
    assert!(entries > INDEX_STRIDE);
    assert!(entries <= u32::MAX as usize);
    assert!(operations > 0);

    let hashes: Vec<_> = (0..entries).map(|ordinal| mix(ordinal as u64)).collect();
    let mut owned = build_owned(&hashes);
    let mut hashbrown = build_hashbrown(&hashes);

    println!("cache-rs memory directory benchmark: entries={entries} operations={operations}");
    let owned_hit = measure("owned_hit", operations, || {
        lookup_owned(&owned, &hashes, operations, false)
    });
    let hashbrown_hit = measure("hashbrown_hit", operations, || {
        lookup_hashbrown(&hashbrown, &hashes, operations, false)
    });
    let owned_miss = measure("owned_miss", operations, || {
        lookup_owned(&owned, &hashes, operations, true)
    });
    let hashbrown_miss = measure("hashbrown_miss", operations, || {
        lookup_hashbrown(&hashbrown, &hashes, operations, true)
    });

    let mut owned_hashes = hashes.clone();
    let owned_churn = measure("owned_churn", operations, || {
        churn_owned(&mut owned, &mut owned_hashes, operations)
    });
    let mut hashbrown_hashes = hashes;
    let hashbrown_churn = measure("hashbrown_churn", operations, || {
        churn_hashbrown(&mut hashbrown, &mut hashbrown_hashes, operations)
    });

    relative_gate("hit", owned_hit, hashbrown_hit, MAX_HIT_RATIO);
    relative_gate("miss", owned_miss, hashbrown_miss, MAX_MISS_RATIO);
    relative_gate("churn", owned_churn, hashbrown_churn, MAX_CHURN_RATIO);
}

fn build_owned(hashes: &[u64]) -> MemoryDirectory {
    let started = Instant::now();
    let mut directory = MemoryDirectory::new(hashes.len()).expect("owned directory allocation");
    for (ordinal, hash) in hashes.iter().copied().enumerate() {
        let result = directory.upsert(
            hash,
            ordinal as u32,
            |head| hashes[head as usize] == hash,
            |head| hashes[head as usize],
        );
        assert_eq!(result, DirectoryUpsert::Inserted);
    }
    print_build("owned_build", hashes.len(), started.elapsed());
    directory
}

fn build_hashbrown(hashes: &[u64]) -> HashTable<u32> {
    let started = Instant::now();
    let mut directory = HashTable::with_capacity(hashes.len());
    for (ordinal, hash) in hashes.iter().copied().enumerate() {
        directory.insert_unique(hash, ordinal as u32, |head| hashes[*head as usize]);
    }
    assert_eq!(directory.len(), hashes.len());
    print_build("hashbrown_build", hashes.len(), started.elapsed());
    directory
}

fn lookup_owned(
    directory: &MemoryDirectory,
    hashes: &[u64],
    operations: usize,
    misses: bool,
) -> u64 {
    let mut index = 0;
    let mut checksum = 0_u64;
    for operation in 0..operations {
        index = next_index(index, hashes.len());
        let hash = if misses {
            mix((hashes.len() + operation) as u64)
        } else {
            hashes[index]
        };
        let head = directory.get(hash, |head| hashes[head as usize] == hash);
        checksum ^= black_box(head.unwrap_or(u32::MAX)) as u64;
    }
    checksum
}

fn lookup_hashbrown(
    directory: &HashTable<u32>,
    hashes: &[u64],
    operations: usize,
    misses: bool,
) -> u64 {
    let mut index = 0;
    let mut checksum = 0_u64;
    for operation in 0..operations {
        index = next_index(index, hashes.len());
        let hash = if misses {
            mix((hashes.len() + operation) as u64)
        } else {
            hashes[index]
        };
        let head = directory.find(hash, |head| hashes[*head as usize] == hash);
        checksum ^= black_box(head.copied().unwrap_or(u32::MAX)) as u64;
    }
    checksum
}

fn churn_owned(directory: &mut MemoryDirectory, slot_hashes: &mut [u64], operations: usize) -> u64 {
    let mut index = 0;
    let mut checksum = 0_u64;
    for operation in 0..operations {
        index = next_index(index, slot_hashes.len());
        let old_hash = slot_hashes[index];
        let removed = directory
            .remove(old_hash, |head| slot_hashes[head as usize] == old_hash)
            .expect("owned churn entry disappeared");
        let new_hash = mix((slot_hashes.len() + operation) as u64);
        slot_hashes[index] = new_hash;
        let inserted = directory.upsert(
            new_hash,
            removed,
            |head| slot_hashes[head as usize] == new_hash,
            |head| slot_hashes[head as usize],
        );
        assert_eq!(inserted, DirectoryUpsert::Inserted);
        checksum ^= black_box(removed) as u64;
    }
    checksum
}

fn churn_hashbrown(
    directory: &mut HashTable<u32>,
    slot_hashes: &mut [u64],
    operations: usize,
) -> u64 {
    let mut index = 0;
    let mut checksum = 0_u64;
    for operation in 0..operations {
        index = next_index(index, slot_hashes.len());
        let old_hash = slot_hashes[index];
        let (removed, _) = directory
            .find_entry(old_hash, |head| slot_hashes[*head as usize] == old_hash)
            .expect("hashbrown churn entry disappeared")
            .remove();
        let new_hash = mix((slot_hashes.len() + operation) as u64);
        slot_hashes[index] = new_hash;
        directory.insert_unique(new_hash, removed, |head| slot_hashes[*head as usize]);
        checksum ^= black_box(removed) as u64;
    }
    checksum
}

fn next_index(index: usize, entries: usize) -> usize {
    let next = index + INDEX_STRIDE;
    if next >= entries {
        next - entries
    } else {
        next
    }
}

fn measure(phase: &str, operations: usize, mut run: impl FnMut() -> u64) -> f64 {
    let _ = black_box(run());
    let started = Instant::now();
    let checksum = black_box(run());
    let elapsed = started.elapsed();
    let nanoseconds_per_operation = elapsed.as_secs_f64() * 1_000_000_000.0 / operations as f64;
    println!(
        "result phase={phase} operations={operations} elapsed_ns={} ns_per_op={:.3} checksum={checksum}",
        elapsed.as_nanos(),
        nanoseconds_per_operation
    );
    nanoseconds_per_operation
}

fn relative_gate(phase: &str, owned: f64, hashbrown: f64, maximum_ratio: f64) {
    let ratio = owned / hashbrown;
    println!("result comparison={phase} owned_to_hashbrown={ratio:.3} maximum={maximum_ratio:.3}");
    assert!(
        ratio <= maximum_ratio,
        "owned {phase} ratio {ratio:.3} exceeded {maximum_ratio:.3}x hashbrown"
    );
}

fn print_build(phase: &str, entries: usize, elapsed: Duration) {
    println!(
        "result phase={phase} entries={entries} elapsed_ns={} ns_per_entry={:.3}",
        elapsed.as_nanos(),
        elapsed.as_secs_f64() * 1_000_000_000.0 / entries as f64
    );
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("benchmark setting must be an integer"))
        .unwrap_or(default)
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
