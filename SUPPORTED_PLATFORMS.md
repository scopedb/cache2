# Supported platforms

cache-rs is still before its M4/1.0 support freeze. This list distinguishes
locally validated behavior from intended device support.

| Platform | I/O path | Current status |
|---|---|---|
| macOS arm64 | sync buffered | Developer baseline; tests, benchmark, soak preflight, fuzz, and ASan validated |
| Linux arm64 | sync buffered | OrbStack RAM-backed tests, functional benchmark, and 100M-key recovery/shutdown scale passed; NVMe qualification pending |
| Linux arm64 | sync direct | OrbStack loop/ext4 O_DIRECT benchmark and turnover preflight passed; NVMe qualification pending |
| Linux arm64 | io_uring direct | OrbStack live io_uring/O_DIRECT descriptor proof and turnover preflight passed; NVMe qualification pending |
| Linux x86_64 | all configured paths | CI configuration present; remote CI and NVMe results not yet recorded |

Other targets have no current support commitment. `IoEngine::Auto` may fall
back to the synchronous backend, while `IoEngine::IoUring` and
`IoMode::Direct` fail explicitly when the requested path is unavailable.

Production support requires the M2 Linux NVMe matrix, a multi-hour turnover
soak, and the M3 workload canary. Kernel, filesystem, mount options, device,
CPU, and queue topology must accompany any qualified result. Run
`scripts/qualify-linux-nvme.sh` on the target mount to capture the device matrix
and soak as one checksummed evidence set.
