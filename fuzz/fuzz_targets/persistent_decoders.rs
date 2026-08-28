// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    cache2::fuzzing::persistent_decoders_and_index_probe(input);
});
