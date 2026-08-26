#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <cache-directory> <new-report-directory>" >&2
  echo "runs the complete Linux NVMe benchmark matrix and turnover soak" >&2
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

if [[ $(uname -s) != "Linux" ]]; then
  echo "qualification requires Linux" >&2
  exit 2
fi

cache_directory=$(cd -- "$1" 2>/dev/null && pwd -P) || {
  echo "cache directory does not exist: $1" >&2
  exit 2
}
if [[ ! -w "$cache_directory" ]]; then
  echo "cache directory is not writable: $cache_directory" >&2
  exit 2
fi

case "$2" in
  /*) report_directory=$2 ;;
  *) report_directory=$PWD/$2 ;;
esac
report_parent=$(dirname -- "$report_directory")
report_name=$(basename -- "$report_directory")
report_parent=$(cd -- "$report_parent" 2>/dev/null && pwd -P) || {
  echo "report parent directory does not exist: $report_parent" >&2
  exit 2
}
report_directory=$report_parent/$report_name
if [[ -e "$report_directory" ]]; then
  echo "report path already exists: $report_directory" >&2
  exit 2
fi

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
project_directory=$(cd -- "$script_directory/.." && pwd -P)
case "$report_directory/" in
  "$project_directory"/*)
    echo "report directory must be outside the source worktree" >&2
    exit 2
    ;;
esac
cd -- "$project_directory"

if ! rustc_version=$(rustc +1.98.0 -V 2>/dev/null) \
  || [[ $rustc_version != "rustc 1.98.0 "* ]]; then
  echo "qualification requires the exact Rust 1.98.0 toolchain" >&2
  exit 2
fi
if ! cargo +1.98.0 -V >/dev/null 2>&1; then
  echo "qualification cannot invoke Cargo from Rust 1.98.0" >&2
  exit 2
fi

benchmark_runs=${CACHE_QUAL_BENCH_RUNS:-5}
soak_seconds=${CACHE_QUAL_SOAK_SECONDS:-14400}
sample_seconds=${CACHE_QUAL_SAMPLE_SECONDS:-10}
for setting in "$benchmark_runs" "$soak_seconds" "$sample_seconds"; do
  if [[ ! $setting =~ ^[1-9][0-9]*$ ]]; then
    echo "qualification run and duration settings must be positive integers" >&2
    exit 2
  fi
done

qualification_status=m2_pass
if ((benchmark_runs < 5 || soak_seconds < 14400)); then
  qualification_status=preflight_pass
fi

benchmark_entries=${CACHE_BENCH_ENTRIES:-8192}
benchmark_value_bytes=${CACHE_BENCH_VALUE_BYTES:-16384}
benchmark_capacity_mib=${CACHE_BENCH_CAPACITY_MIB:-512}
for setting in "$benchmark_entries" "$benchmark_value_bytes" "$benchmark_capacity_mib"; do
  if [[ ! $setting =~ ^[1-9][0-9]*$ ]]; then
    echo "benchmark entries, value bytes, and capacity must be positive integers" >&2
    exit 2
  fi
done
if ((benchmark_value_bytes < 8)); then
  echo "benchmark values must be at least 8 bytes" >&2
  exit 2
fi

physical_memory_bytes=$(awk '/^MemTotal:/ { printf "%.0f\n", $2 * 1024 }' /proc/meminfo)
if [[ ! $physical_memory_bytes =~ ^[1-9][0-9]*$ ]]; then
  echo "cannot determine physical memory from /proc/meminfo" >&2
  exit 2
fi
dataset_bytes=$(awk -v entries="$benchmark_entries" -v bytes="$benchmark_value_bytes" \
  'BEGIN { printf "%.0f\n", entries * bytes }')
if ! awk -v dataset="$dataset_bytes" -v memory="$physical_memory_bytes" \
  'BEGIN { exit !(dataset > memory) }'; then
  if [[ ${CACHE_QUAL_ALLOW_MEMORY_SIZED_DATASET:-0} != "1" ]]; then
    echo "qualification dataset must exceed physical RAM; set CACHE_BENCH_ENTRIES and CACHE_BENCH_CAPACITY_MIB for the target host" >&2
    echo "use CACHE_QUAL_ALLOW_MEMORY_SIZED_DATASET=1 only for preflight" >&2
    exit 2
  fi
  qualification_status=preflight_pass
fi
if ! awk -v dataset="$dataset_bytes" -v capacity_mib="$benchmark_capacity_mib" \
  'BEGIN { exit !(dataset <= capacity_mib * 1048576 / 2) }'; then
  echo "benchmark dataset must not exceed half of CACHE_BENCH_CAPACITY_MIB" >&2
  exit 2
fi
export CACHE_BENCH_ENTRIES="$benchmark_entries"
export CACHE_BENCH_VALUE_BYTES="$benchmark_value_bytes"
export CACHE_BENCH_CAPACITY_MIB="$benchmark_capacity_mib"

backing_source=
if command -v findmnt >/dev/null 2>&1; then
  backing_source=$(findmnt --noheadings --output SOURCE --target "$cache_directory" \
    2>/dev/null | head -n 1 || true)
fi
backing_source=${backing_source%%\[*}
device_qualification=unverified
if [[ -b $backing_source ]] && command -v lsblk >/dev/null 2>&1; then
  rotational_values=$(lsblk --inverse --noheadings --output ROTA "$backing_source" \
    | awk 'NF { print $1 }' | sort -u | tr '\n' ' ' || true)
  transport_values=$(lsblk --inverse --noheadings --output TRAN "$backing_source" \
    | awk 'NF { print $1 }' | sort -u | tr '\n' ' ' || true)
  if [[ $rotational_values == *0* && $rotational_values != *1* \
    && $transport_values == *nvme* ]]; then
    device_qualification=nvme_non_rotational
  fi
fi
if [[ $device_qualification != nvme_non_rotational ]]; then
  if [[ ${CACHE_QUAL_ALLOW_UNVERIFIED_DEVICE:-0} != "1" ]]; then
    echo "cannot prove that $cache_directory is backed by non-rotational NVMe" >&2
    echo "use CACHE_QUAL_ALLOW_UNVERIFIED_DEVICE=1 only for preflight" >&2
    exit 2
  fi
  qualification_status=preflight_pass
fi

performance_gate_count=0
for gate in \
  CACHE_BENCH_MIN_PUT_OPS \
  CACHE_BENCH_MIN_RESIDENT_L1_OPS \
  CACHE_BENCH_MIN_L2_OPS \
  CACHE_BENCH_MIN_PROMOTED_L1_OPS \
  CACHE_BENCH_MAX_WARM_CLOSE_MS; do
  if [[ -n ${!gate:-} ]]; then
    ((performance_gate_count += 1))
  fi
done
if ((performance_gate_count < 5)); then
  qualification_status=preflight_pass
fi

source_revision=
source_status=
if command -v git >/dev/null 2>&1; then
  source_revision=$(git rev-parse HEAD)
  source_status=$(git status --short)
elif [[ ${CACHE_QUAL_ALLOW_DIRTY:-0} == "1" \
  && ${CACHE_QUAL_SOURCE_REVISION:-} =~ ^[0-9a-fA-F]{7,64}$ ]]; then
  source_revision=$CACHE_QUAL_SOURCE_REVISION
  source_status="git unavailable; caller supplied revision for preflight"
  qualification_status=preflight_pass
else
  echo "qualification requires git and a source revision" >&2
  exit 2
fi

if [[ ${CACHE_QUAL_ALLOW_DIRTY:-0} != "1" ]]; then
  if ! git diff --quiet || ! git diff --cached --quiet \
    || [[ -n $(git ls-files --others --exclude-standard) ]]; then
    echo "qualification requires a clean worktree; set CACHE_QUAL_ALLOW_DIRTY=1 only for preflight" >&2
    exit 2
  fi
else
  qualification_status=preflight_pass
fi

mkdir -- "$report_directory"

record_command() {
  local name=$1
  shift
  if command -v -- "$name" >/dev/null 2>&1; then
    echo "[$name]"
    "$@"
  fi
}

{
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$source_revision"
  echo "cache_directory=$cache_directory"
  echo "benchmark_runs=$benchmark_runs"
  echo "soak_seconds=$soak_seconds"
  echo "sample_seconds=$sample_seconds"
  echo "qualification_status=$qualification_status"
  echo "dataset_bytes=$dataset_bytes"
  echo "physical_memory_bytes=$physical_memory_bytes"
  echo "backing_source=$backing_source"
  echo "device_qualification=$device_qualification"
  echo "performance_gates_configured=$performance_gate_count/5"
  echo "[git-status]"
  printf "%s\n" "$source_status"
  echo "[uname]"
  uname -a
  echo "[rustc]"
  rustc +1.98.0 -Vv
  echo "[cargo]"
  cargo +1.98.0 -V
  record_command lscpu lscpu
  record_command lsblk lsblk --bytes --fs --output NAME,KNAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL,ROTA
  record_command findmnt findmnt --target "$cache_directory"
  record_command df df -hT "$cache_directory"
} >"$report_directory/environment.txt" 2>&1

echo -e "profile\tphase\tmedian_elapsed_ms\tmedian_ops_per_sec" \
  >"$report_directory/summary.tsv"

extract_metric() {
  local phase=$1
  local metric=$2
  local file=$3
  awk -v wanted_phase="$phase" -v wanted_metric="$metric" '
    $1 == "result" {
      phase = ""
      metric = ""
      for (field = 2; field <= NF; field++) {
        split($field, pair, "=")
        if (pair[1] == "phase") {
          phase = pair[2]
        }
        if (pair[1] == wanted_metric) {
          metric = pair[2]
        }
      }
      if (phase == wanted_phase && metric != "") {
        print metric
      }
    }
  ' "$file"
}

median() {
  sort -n | awk '
    { values[NR] = $1 }
    END {
      if (NR == 0) {
        exit 2
      }
      if (NR % 2 == 1) {
        printf "%.3f\n", values[(NR + 1) / 2]
      } else {
        printf "%.3f\n", (values[NR / 2] + values[NR / 2 + 1]) / 2
      }
    }
  '
}

summarize_profile() {
  local profile=$1
  local expected_runs=$2
  local log=$3
  local phase
  for phase in put_drain resident_l1 warm_close l2_promote promoted_l1; do
    local observed
    observed=$(extract_metric "$phase" elapsed_ns "$log" | wc -l)
    if [[ $observed -ne $expected_runs ]]; then
      echo "$profile produced $observed/$expected_runs results for $phase" >&2
      exit 1
    fi
    local elapsed_ms
    local operations_per_second
    elapsed_ms=$(extract_metric "$phase" elapsed_ns "$log" \
      | awk '{ print $1 / 1000000 }' | median)
    operations_per_second=$(extract_metric "$phase" ops_per_sec "$log" | median)
    printf "%s\t%s\t%s\t%s\n" \
      "$profile" "$phase" "$elapsed_ms" "$operations_per_second" \
      >>"$report_directory/summary.tsv"
  done
}

run_benchmark_profile() {
  local profile=$1
  local engine=$2
  local mode=$3
  local workers=$4
  local runs=$5
  local log=$report_directory/benchmark-$profile.log
  local run
  for ((run = 1; run <= runs; run++)); do
    echo "profile=$profile run=$run/$runs"
    CACHE_BENCH_DIR="$cache_directory" \
    CACHE_BENCH_IO_ENGINE="$engine" \
    CACHE_BENCH_IO_MODE="$mode" \
    CACHE_BENCH_IO_WORKERS="$workers" \
      cargo +1.98.0 bench --locked --bench hybrid_cache --quiet 2>&1 | tee -a "$log"
  done
  summarize_profile "$profile" "$runs" "$log"
}

echo "building release benchmark targets"
cargo +1.98.0 build --locked --release --benches

run_benchmark_profile sync-buffered sync buffered 4 "$benchmark_runs"
run_benchmark_profile sync-direct sync direct 4 "$benchmark_runs"
run_benchmark_profile io-uring-direct io-uring direct 4 "$benchmark_runs"

for workers in 1 2 4 8 16; do
  run_benchmark_profile "io-uring-direct-workers-$workers" \
    io-uring direct "$workers" 1
done

echo "starting ${soak_seconds}s io_uring/direct turnover soak"
CACHE_SOAK_SECONDS="$soak_seconds" \
CACHE_SOAK_SAMPLE_SECONDS="$sample_seconds" \
CACHE_SOAK_DIR="$cache_directory" \
CACHE_SOAK_IO_ENGINE=io-uring \
CACHE_SOAK_IO_MODE=direct \
  cargo +1.98.0 bench --locked --bench hybrid_cache_soak --quiet 2>&1 \
  | tee "$report_directory/soak-io-uring-direct.log"

if ! grep -q '^complete .* errors=0 ' "$report_directory/soak-io-uring-direct.log"; then
  echo "soak did not produce a successful completion record" >&2
  exit 1
fi

(
  cd -- "$report_directory"
  sha256sum environment.txt summary.tsv benchmark-*.log soak-io-uring-direct.log \
    >SHA256SUMS
)
{
  echo "status=$qualification_status"
  echo "performance_gates_configured=$performance_gate_count/5"
  echo "completed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$report_directory/qualification.status"

echo "qualification complete: $report_directory"
