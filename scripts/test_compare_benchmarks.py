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

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "compare_benchmarks", Path(__file__).with_name("compare-benchmarks.py")
)
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class ComparisonTests(unittest.TestCase):
    def run_comparison_fixture(self, rates, *, aa=False, expected_exit=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            binary = root / "benchmark"
            binary.write_bytes(b"fixture benchmark")
            settings = root / "environment.json"
            settings.write_text(json.dumps({"CACHE_SOAK_KEYS": "123"}))
            output = root / "results"
            arguments = [
                "compare-benchmarks.py", "--baseline", str(binary),
                "--candidate", str(binary), "--env", str(settings),
                "--output", str(output), "--blocks", "4", "--metric", "put_steady",
            ]
            if aa:
                arguments.append("--aa")
            remaining = iter(rates)

            def run_fixture(command, *, env, stdout, **kwargs):
                self.assertEqual(command, [str(binary)])
                self.assertEqual(env["CACHE_SOAK_KEYS"], "123")
                self.assertNotIn("CACHE_BENCH_TEST_INHERITED", env)
                stdout.write(
                    f"result phase=put_steady ops_per_sec={next(remaining)} elapsed_ns=5000000000\n"
                    "report version=2 type=run status=pass\n"
                )
                return compare.subprocess.CompletedProcess(command, 0)

            with (
                mock.patch("sys.argv", arguments),
                mock.patch.dict(compare.os.environ, {"CACHE_BENCH_TEST_INHERITED": "1"}),
                mock.patch.object(compare, "record_power"),
                mock.patch.object(compare.platform, "platform", return_value="test host"),
                mock.patch.object(compare.subprocess, "run", side_effect=run_fixture),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                if expected_exit:
                    with self.assertRaises(SystemExit) as error:
                        compare.main()
                    self.assertEqual(error.exception.code, expected_exit)
                else:
                    compare.main()
            self.assertEqual(len(list(output.glob("*.log"))), 16)
            samples = json.loads((output / "samples.json").read_text())
            self.assertEqual("".join(sample["version"] for sample in samples), "ABBABAAB" * 2)
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["sha256"]["A"], manifest["sha256"]["B"])
            return json.loads((output / "summary.json").read_text())

    def test_stable_aa_retains_all_samples_and_passes(self):
        summary = self.run_comparison_fixture([1000] * 16, aa=True)
        self.assertTrue(summary["stable"])
        self.assertTrue(summary["aa_pass"])

    def test_pairing_does_not_hide_alternating_run_noise(self):
        for aa in (False, True):
            with self.subTest(aa=aa):
                summary = self.run_comparison_fixture([1000, 500] * 8, aa=aa, expected_exit=2)
                metric = summary["metrics"]["put_steady"]
                self.assertEqual(metric["paired_bootstrap_95_percent"], [0, 0])
                self.assertEqual(metric["block_center_spread_percent"], 0)
                self.assertAlmostEqual(metric["max_within_block_spread_percent"], 100)
                self.assertFalse(summary["stable"])
                self.assertEqual(summary["aa_pass"], False if aa else None)

    def test_block_drift_fails_comparison_even_when_repeats_are_stable(self):
        summary = self.run_comparison_fixture(
            [1000 * 1.1 ** block for block in range(4) for _ in range(4)], expected_exit=2,
        )
        metric = summary["metrics"]["put_steady"]
        self.assertEqual(metric["max_within_block_spread_percent"], 0)
        self.assertGreater(metric["block_center_spread_percent"], 33)
        self.assertFalse(summary["stable"])

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
