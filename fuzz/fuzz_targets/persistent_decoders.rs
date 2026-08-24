#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    cache_rs::fuzzing::persistent_decoders_and_index_probe(input);
});
