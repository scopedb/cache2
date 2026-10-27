#!/usr/bin/env python3
# Copyright 2026 ScopeDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "compare_benchmarks", Path(__file__).with_name("compare-benchmarks.py")
)
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class ComparisonTests(unittest.TestCase):
    def test_parse_counts_hits_independently_of_library_statistics(self):
        output = """report version=2 type=job benchmark=cache_soak job=get iops=100 runtime_ns=20000000000
complete hits=600 writes=1000 cache_puts=0 l1_hits=0 l2_hits=0
report version=2 type=run status=pass
"""
        result = compare.parse_measurement(output, ["get", "hit"], 5)
        self.assertEqual(result["rates"]["hit"], 30)
        self.assertEqual(result["rates"]["get"], 100)

    def test_rejects_failed_short_and_missing_measurements(self):
        output = """result phase=put_steady ops_per_sec=1000 elapsed_ns=2000000000
report version=2 type=run status=pass
"""
        for text, metrics, minimum in [
            (output.replace("status=pass", "status=error"), ["put_steady"], 1),
            (output, ["put_steady"], 5),
            (output, ["get"], 1),
        ]:
            with self.assertRaises(ValueError):
                compare.parse_measurement(text, metrics, minimum)

    def test_abba_cancels_linear_log_drift_without_hiding_absolute_drift(self):
        samples = []
        for block in range(4):
            for slot, version in enumerate("ABBA" if block % 2 == 0 else "BAAB"):
                samples.append({
                    "block": block, "version": version,
                    "rates": {"put": 1000 * 1.01 ** (4 * block + slot) * (1.1 if version == "B" else 1)},
                })
        result = compare.summarize(samples, ["put"], 4)["put"]
        self.assertAlmostEqual(result["paired_change_percent"], 10)
        for bound in result["paired_bootstrap_95_percent"]:
            self.assertAlmostEqual(bound, 10)
        self.assertGreater(result["block_center_spread_percent"], 12)

    def test_noisy_pairs_keep_their_uncertainty(self):
        samples = []
        for block, ratio in enumerate([0.8, 1.2, 0.8, 1.2]):
            for version in "ABBA":
                samples.append({
                    "block": block, "version": version,
                    "rates": {"put": 1000 * (ratio if version == "B" else 1)},
                })
        interval = compare.summarize(samples, ["put"], 4)["put"]["paired_bootstrap_95_percent"]
        self.assertLess(interval[0], -3)
        self.assertGreater(interval[1], 3)

    def test_rejects_incomplete_blocks(self):
        with self.assertRaises(ValueError):
            compare.summarize([], ["put"], 4)

    def test_ac_power_alone_does_not_establish_stable_power(self):
        for state in ["3%; discharging;", "85%; discharging;", "3%; charging;"]:
            self.assertFalse(compare.stable_power("Now drawing from 'AC Power'\n" + state, 20))
        self.assertFalse(compare.stable_power("Now drawing from 'Battery Power'\n80%; discharging;", 20))
        for state in ["80%; charging;", "100%; charged;", "80%; AC attached; not charging", ""]:
            self.assertTrue(compare.stable_power("Now drawing from 'AC Power'\n" + state, 20))


if __name__ == "__main__":
    unittest.main()
