// Copyright 2026 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared byte assertions for module-local persistent-format fixtures.
//!
//! Golden fixtures pin versioned on-disk bytes. Changes require an explicit format-version
//! decision; tests never regenerate them. Each fixture lives beside the module that owns its
//! format.
//!
//! The sparse representation starts with the complete byte length. Each following line contains a
//! hexadecimal offset and hexadecimal bytes; unspecified bytes are zero.

/// Checks every byte, including zero padding, and returns the committed bytes
/// for decoder compatibility checks.
#[track_caller]
pub fn assert_golden(actual: &[u8], fixture: &str) -> Vec<u8> {
    let golden = sparse_golden(fixture);
    assert_eq!(actual.len(), golden.len(), "golden byte length differs");
    if let Some(offset) = actual.iter().zip(&golden).position(|(a, b)| a != b) {
        panic!(
            "golden byte mismatch at offset {offset:#06x}: expected {:#04x}, got {:#04x}",
            golden[offset], actual[offset]
        );
    }
    golden
}

#[track_caller]
fn sparse_golden(input: &str) -> Vec<u8> {
    let mut output: Option<Vec<u8>> = None;
    for raw_line in input.lines() {
        let line = raw_line.split('#').next().unwrap().trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let first = fields.next().unwrap();
        if first == "length" {
            let length = fields.next().unwrap().parse::<usize>().unwrap();
            assert!(output.replace(vec![0_u8; length]).is_none());
            continue;
        }
        let offset = usize::from_str_radix(first, 16).unwrap();
        let encoded = fields.next().unwrap();
        assert_eq!(encoded.len() % 2, 0);
        let bytes = encoded
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        let output = output.as_mut().expect("golden length must come first");
        output[offset..offset + bytes.len()].copy_from_slice(&bytes);
        assert!(fields.next().is_none());
    }
    output.expect("golden fixture must declare its length")
}
