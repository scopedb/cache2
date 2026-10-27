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

"""Serial ABBA comparisons of saved cache/cache_soak benchmark binaries."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import statistics
import subprocess


def parse_measurement(output, metrics, minimum_seconds):
    rows = [
        dict(part.split("=", 1) for part in line.split() if "=" in part)
        for line in output.splitlines()
        if line.startswith(("result ", "report version=2 ", "complete "))
    ]
    run = next(row for row in rows if row.get("type") == "run")
    if run["status"] != "pass":
        raise ValueError("benchmark status was not pass")
    rates, durations, details = {}, {}, {"run": run}
    for row in rows:
        if "ops_per_sec" in row:
            rates[row["phase"]] = float(row["ops_per_sec"])
            durations[row["phase"]] = int(row["elapsed_ns"]) / 1e9
        if row.get("type") == "job" and row.get("benchmark") == "cache_soak":
            rates[row["job"]] = float(row["iops"])
            durations[row["job"]] = int(row["runtime_ns"]) / 1e9
            details[row["job"]] = row
        if row.get("phase") in ("put_admission", "put_steady_admission"):
            details[row["phase"]] = row
        if "hits" in row and "writes" in row:
            rates["hit"] = int(row["hits"]) / durations["get"]
            durations["hit"] = durations["get"]
            details["complete"] = row
    for metric in metrics:
        if not math.isfinite(rates.get(metric, 0)) or rates.get(metric, 0) <= 0:
            raise ValueError(f"missing or nonpositive metric: {metric}")
        if durations[metric] < minimum_seconds:
            raise ValueError(f"{metric} too short: {durations[metric]:.3f}s")
    return {"rates": rates, "seconds": durations, "details": details}


def summarize(samples, metrics, block_count):
    result = {}
    for metric in metrics:
        changes, centers = [], []
        for block in range(block_count):
            groups = {
                version: [
                    math.log(sample["rates"][metric])
                    for sample in samples
                    if sample["block"] == block and sample["version"] == version
                ]
                for version in "AB"
            }
            if any(len(group) != 2 for group in groups.values()):
                raise ValueError("each block requires two samples per binary")
            means = {version: statistics.mean(group) for version, group in groups.items()}
            changes.append(means["B"] - means["A"])
            centers.append(statistics.mean(means.values()))
        # The four serial runs form one paired unit. Resampling individual runs
        # would pretend the within-block order and common drift were independent.
        rng = random.Random(20261027)
        bootstrap = sorted(
            statistics.mean(rng.choices(changes, k=block_count)) for _ in range(10_000)
        )
        result[metric] = {
            "median_A": statistics.median(
                sample["rates"][metric] for sample in samples if sample["version"] == "A"
            ),
            "median_B": statistics.median(
                sample["rates"][metric] for sample in samples if sample["version"] == "B"
            ),
            "paired_change_percent": 100 * math.expm1(statistics.mean(changes)),
            "paired_bootstrap_95_percent": [
                100 * math.expm1(bootstrap[249]),
                100 * math.expm1(bootstrap[9749]),
            ],
            "block_change_percent": [100 * math.expm1(change) for change in changes],
            "block_center_spread_percent": 100 * math.expm1(max(centers) - min(centers)),
        }
    return result


def stable_power(output, minimum_battery_percent):
    # Desktop Macs have no battery line. A connected but underpowered charger
    # can still discharge a laptop; AC Power by itself is insufficient.
    battery = re.search(r"(\d+)%;\s*([^;]+);", output)
    return "'AC Power'" in output and (
        battery is None
        or (int(battery[1]) >= minimum_battery_percent and battery[2] in ("charging", "charged", "AC attached", "finishing charge"))
    )


def record_power(output_directory, label, minimum_battery_percent):
    if platform.system() != "Darwin":
        return
    output = subprocess.check_output(["/usr/bin/pmset", "-g", "batt"], text=True)
    (output_directory / f"{label}-power.txt").write_text(output)
    if not stable_power(output, minimum_battery_percent):
        raise RuntimeError("unstable power or low battery; retained logs, stopped comparison")


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--env", type=Path, required=True, help="JSON object of workload environment variables")
    parser.add_argument("--output", type=Path, required=True, help="new directory for all raw logs and results")
    parser.add_argument("--blocks", type=int, default=8)
    parser.add_argument("--metric", action="append", required=True, help="put_steady, or soak get/hit/put; repeatable")
    parser.add_argument("--prefix", action="append", default=[], help="launcher argument before the binary; repeat for taskset/taskpolicy arguments")
    parser.add_argument("--aa", action="store_true", help="require identical binaries and a narrow interval around zero")
    parser.add_argument("--tolerance", type=float, default=3.0, help="A/A confidence interval bound, percent")
    parser.add_argument("--max-drift", type=float, default=5.0, help="maximum spread of ABBA block centers, percent")
    parser.add_argument("--min-seconds", type=float, default=5.0, help="minimum measured duration for each selected metric")
    parser.add_argument("--min-battery-percent", type=int, default=20, help="macOS battery floor; AC without discharging is always required")
    args = parser.parse_args()
    if args.blocks < 4:
        parser.error("at least four blocks are required")
    if any(not math.isfinite(value) or value <= 0 for value in (args.tolerance, args.max_drift, args.min_seconds)):
        parser.error("tolerances and duration must be finite and positive")
    if not 0 <= args.min_battery_percent <= 100:
        parser.error("battery floor must be in 0..100")
    paths = {"A": args.baseline.resolve(), "B": args.candidate.resolve()}
    hashes = {version: hashlib.sha256(path.read_bytes()).hexdigest() for version, path in paths.items()}
    if args.aa and hashes["A"] != hashes["B"]:
        parser.error("A/A requires byte-identical binaries")
    settings = json.loads(args.env.read_text())
    if not isinstance(settings, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in settings.items()
    ):
        parser.error("environment must be an object of string keys and string values")
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("CACHE_BENCH_", "CACHE_WORKLOAD_", "CACHE_SOAK_"))
    }
    environment.update(settings)
    args.output.mkdir(parents=True, exist_ok=False)
    write_json(args.output / "manifest.json", {
        "binaries": {version: str(path) for version, path in paths.items()},
        "sha256": hashes,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "environment": settings,
        "platform": platform.platform(),
        "command_prefix": args.prefix,
        "blocks": args.blocks,
        "metrics": args.metric,
        "aa": args.aa,
        "tolerance_percent": args.tolerance,
        "max_drift_percent": args.max_drift,
        "min_seconds": args.min_seconds,
        "min_battery_percent": args.min_battery_percent,
    })
    samples = []
    try:
        for block in range(args.blocks):
            order = "ABBA" if block % 2 == 0 else "BAAB"
            for slot, version in enumerate(order):
                label = f"{block + 1:02}-{slot + 1}-{version}"
                record_power(args.output, label + "-before", args.min_battery_percent)
                path = args.output / f"{label}.log"
                with path.open("w") as log:
                    process = subprocess.run(
                        [*args.prefix, str(paths[version])], env=environment,
                        stdout=log, stderr=subprocess.STDOUT, timeout=300,
                    )
                if process.returncode:
                    raise RuntimeError(f"{path}: exit {process.returncode}")
                sample = {
                    "block": block, "slot": slot, "version": version,
                    **parse_measurement(path.read_text(), args.metric, args.min_seconds),
                }
                samples.append(sample)
                write_json(args.output / "samples.json", samples)
                record_power(args.output, label + "-after", args.min_battery_percent)
                print(json.dumps({
                    "block": block + 1, "slot": slot + 1, "version": version,
                    "rates": {metric: sample["rates"][metric] for metric in args.metric},
                    "seconds": {metric: sample["seconds"][metric] for metric in args.metric},
                }), flush=True)
    except (OSError, RuntimeError, ValueError, StopIteration, subprocess.SubprocessError) as error:
        write_json(args.output / "failure.json", {"error": str(error), "completed_samples": len(samples)})
        raise
    metrics = summarize(samples, args.metric, args.blocks)
    stable = all(value["block_center_spread_percent"] <= args.max_drift for value in metrics.values())
    aa_pass = stable and all(
        -args.tolerance <= value["paired_bootstrap_95_percent"][0]
        and value["paired_bootstrap_95_percent"][1] <= args.tolerance
        for value in metrics.values()
    )
    result = {"metrics": metrics, "stable": stable, "aa_pass": aa_pass if args.aa else None}
    write_json(args.output / "summary.json", result)
    print(json.dumps(result), flush=True)
    if not stable or (args.aa and not aa_pass):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
