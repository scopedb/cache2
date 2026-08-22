//! Offline management CLI for cache-rs files.

use std::env;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use cache_rs::{
    BucketCacheConfig, CacheConfig, CacheError, CacheFileKind, CacheStatus,
    CheckpointDirectoryState, CheckpointSlotState, ConfigDiagnostics, HybridCacheConfig,
    HybridConfigDiagnostics, HybridInspectReport, HybridVerifyReport, HybridWriteMode,
    InspectReport, IoEngineKind, IoMode, ManagementError, RecoveryMode, StartupDiagnostics,
    VerifyReport, inspect_cache_file, inspect_hybrid_cache_files, verify_cache_file,
    verify_hybrid_cache_files,
};

const EXIT_INVALID: u8 = 2;
const EXIT_LOCKED: u8 = 3;
const EXIT_IO: u8 = 4;

fn main() -> ExitCode {
    match run(env::args().skip(1)) {
        Ok(code) => ExitCode::from(code),
        Err(CliError::Usage(message)) => {
            eprintln!("cachectl: {message}");
            eprintln!("try 'cachectl --help'");
            ExitCode::from(EXIT_INVALID)
        }
        Err(CliError::Management(ManagementError::Locked)) => {
            eprintln!("cachectl: cache file is open by another instance");
            ExitCode::from(EXIT_LOCKED)
        }
        Err(CliError::Cache(CacheError::Locked)) => {
            eprintln!("cachectl: cache file is open by another instance");
            ExitCode::from(EXIT_LOCKED)
        }
        Err(CliError::Cache(error @ CacheError::InvalidConfig(_)))
        | Err(CliError::Cache(error @ CacheError::CorruptMetadata(_))) => {
            eprintln!("cachectl: {error}");
            ExitCode::from(EXIT_INVALID)
        }
        Err(CliError::Cache(error)) => {
            eprintln!("cachectl: {error}");
            ExitCode::from(EXIT_IO)
        }
        Err(CliError::Management(error)) => {
            eprintln!("cachectl: {error}");
            ExitCode::from(EXIT_IO)
        }
    }
}

fn run(arguments: impl IntoIterator<Item = String>) -> Result<u8, CliError> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|command| command.starts_with("hybrid-"))
    {
        return run_hybrid(arguments);
    }
    let options = match Options::parse(arguments)? {
        ParseOutcome::Help => {
            print_help();
            return Ok(0);
        }
        ParseOutcome::Run(options) => *options,
    };
    match options.command {
        Command::Inspect => {
            let report = inspect_cache_file(&options.path)?;
            match options.output {
                Output::Human => print_inspect_human(&options.path, &report),
                Output::Json => println!("{}", inspect_json(&options.path, &report)),
            }
            Ok(if inspect_passed(&report) {
                0
            } else {
                EXIT_INVALID
            })
        }
        Command::Verify => {
            let report = verify_cache_file(&options.path)?;
            match options.output {
                Output::Human => print_verify_human(&options.path, &report),
                Output::Json => println!("{}", verify_json(&options.path, &report)),
            }
            Ok(if report.valid { 0 } else { EXIT_INVALID })
        }
        Command::Diagnose => {
            let diagnostics = build_config(&options)?.diagnostics()?;
            match options.output {
                Output::Human => print_diagnostics_human(&diagnostics),
                Output::Json => println!("{}", diagnostics_json(&diagnostics)),
            }
            Ok(0)
        }
        Command::Format | Command::Reset => {
            let command = options.command;
            let config = build_config(&options)?;
            let diagnostics = config.diagnostics()?;
            let cache = if command == Command::Reset {
                config.reset_existing()?
            } else {
                config.format_empty()?
            };
            let startup = cache.startup_diagnostics();
            cache.close()?;
            match options.output {
                Output::Human => print_format_human(command, &diagnostics, &startup),
                Output::Json => println!("{}", format_json(command, &diagnostics, &startup)),
            }
            Ok(0)
        }
    }
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Management(ManagementError),
    Cache(CacheError),
}

impl From<ManagementError> for CliError {
    fn from(error: ManagementError) -> Self {
        Self::Management(error)
    }
}

impl From<CacheError> for CliError {
    fn from(error: CacheError) -> Self {
        Self::Cache(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Inspect,
    Verify,
    Diagnose,
    Format,
    Reset,
}

impl Command {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Verify => "verify",
            Self::Diagnose => "diagnose",
            Self::Format => "format",
            Self::Reset => "reset",
        }
    }

    const fn needs_config(self) -> bool {
        matches!(self, Self::Diagnose | Self::Format | Self::Reset)
    }

    const fn destructive(self) -> bool {
        matches!(self, Self::Format | Self::Reset)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Output {
    Human,
    Json,
}

#[derive(Debug, Eq, PartialEq)]
struct Options {
    command: Command,
    path: PathBuf,
    output: Output,
    capacity: Option<u64>,
    region_size: Option<u64>,
    index_slots: Option<usize>,
    max_key_size: Option<usize>,
    max_value_size: Option<usize>,
    hash_seed: Option<u64>,
    append_lanes: Option<usize>,
    memory_budget: Option<usize>,
    read_queue_depth: Option<usize>,
    write_queue_depth: Option<usize>,
    io_queue_depth: Option<usize>,
    confirmed: bool,
}

enum ParseOutcome {
    Help,
    Run(Box<Options>),
}

impl Options {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<ParseOutcome, CliError> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(command) = arguments.next() else {
            return Err(CliError::Usage("a command is required".into()));
        };
        if command == "--help" || command == "-h" || command == "help" {
            return Ok(ParseOutcome::Help);
        }
        let command = match command.as_str() {
            "inspect" => Command::Inspect,
            "verify" => Command::Verify,
            "diagnose" => Command::Diagnose,
            "format" => Command::Format,
            "reset" => Command::Reset,
            _ => {
                return Err(CliError::Usage(format!("unknown command {command:?}")));
            }
        };
        let mut path = None;
        let mut output = Output::Human;
        let mut capacity = None;
        let mut region_size = None;
        let mut index_slots = None;
        let mut max_key_size = None;
        let mut max_value_size = None;
        let mut hash_seed = None;
        let mut append_lanes = None;
        let mut memory_budget = None;
        let mut read_queue_depth = None;
        let mut write_queue_depth = None;
        let mut io_queue_depth = None;
        let mut confirmed = false;
        while let Some(argument) = arguments.next() {
            if argument == "--help" || argument == "-h" {
                return Ok(ParseOutcome::Help);
            }
            let (name, inline) = argument
                .split_once('=')
                .map_or((argument.as_str(), None), |(name, value)| {
                    (name, Some(value.to_owned()))
                });
            if name == "--yes" {
                if inline.is_some() {
                    return Err(CliError::Usage("--yes does not take a value".into()));
                }
                confirmed = true;
                continue;
            }
            let value = match inline {
                Some(value) => value,
                None => match arguments.peek() {
                    Some(value) if !value.starts_with('-') => {
                        arguments.next().ok_or_else(|| {
                            CliError::Usage(format!("missing value for option {name}"))
                        })?
                    }
                    _ => {
                        return Err(CliError::Usage(format!("missing value for option {name}")));
                    }
                },
            };
            match name {
                "--path" => path = Some(PathBuf::from(value)),
                "--capacity" => capacity = Some(parse_bytes(&value, name)?),
                "--region-size" => region_size = Some(parse_bytes(&value, name)?),
                "--index-slots" => index_slots = Some(parse_number(&value, name)?),
                "--max-key-size" => max_key_size = Some(parse_usize_bytes(&value, name)?),
                "--max-value-size" => max_value_size = Some(parse_usize_bytes(&value, name)?),
                "--hash-seed" => hash_seed = Some(parse_u64(&value, name)?),
                "--append-lanes" => append_lanes = Some(parse_number(&value, name)?),
                "--memory-budget" => memory_budget = Some(parse_usize_bytes(&value, name)?),
                "--read-queue-depth" => read_queue_depth = Some(parse_number(&value, name)?),
                "--write-queue-depth" => write_queue_depth = Some(parse_number(&value, name)?),
                "--io-queue-depth" => io_queue_depth = Some(parse_number(&value, name)?),
                "--output" => {
                    output = match value.as_str() {
                        "human" => Output::Human,
                        "json" => Output::Json,
                        _ => {
                            return Err(CliError::Usage("--output must be human or json".into()));
                        }
                    }
                }
                _ => {
                    return Err(CliError::Usage(format!("unknown option {name}")));
                }
            }
        }
        let path = path.ok_or_else(|| CliError::Usage("--path is required".into()))?;
        if path.as_os_str().is_empty() {
            return Err(CliError::Usage("--path must not be empty".into()));
        }
        let has_config = capacity.is_some()
            || region_size.is_some()
            || index_slots.is_some()
            || max_key_size.is_some()
            || max_value_size.is_some()
            || hash_seed.is_some()
            || append_lanes.is_some()
            || memory_budget.is_some()
            || read_queue_depth.is_some()
            || write_queue_depth.is_some()
            || io_queue_depth.is_some();
        if command.needs_config() && capacity.is_none() {
            return Err(CliError::Usage(format!(
                "--capacity is required for {}",
                command.as_str()
            )));
        }
        if !command.needs_config() && has_config {
            return Err(CliError::Usage(format!(
                "{} accepts only --path and --output",
                command.as_str()
            )));
        }
        if command.destructive() && !confirmed {
            return Err(CliError::Usage(format!(
                "{} requires explicit --yes confirmation",
                command.as_str()
            )));
        }
        if !command.destructive() && confirmed {
            return Err(CliError::Usage(
                "--yes is valid only for format or reset".into(),
            ));
        }
        Ok(ParseOutcome::Run(Box::new(Self {
            command,
            path,
            output,
            capacity,
            region_size,
            index_slots,
            max_key_size,
            max_value_size,
            hash_seed,
            append_lanes,
            memory_budget,
            read_queue_depth,
            write_queue_depth,
            io_queue_depth,
            confirmed,
        })))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HybridCommand {
    Inspect,
    Verify,
    Diagnose,
    Format,
}

impl HybridCommand {
    const fn needs_config(self) -> bool {
        matches!(self, Self::Diagnose | Self::Format)
    }
}

#[derive(Debug)]
struct HybridOptions {
    command: HybridCommand,
    bucket_path: PathBuf,
    region_path: PathBuf,
    manifest_path: PathBuf,
    output: Output,
    bucket_capacity: Option<u64>,
    bucket_size: usize,
    bucket_memory_budget: usize,
    bucket_buffer_slots: usize,
    bucket_io_engine: IoEngineKind,
    bucket_io_mode: IoMode,
    bucket_io_queue_depth: usize,
    region_capacity: Option<u64>,
    region_size: Option<u64>,
    index_slots: Option<usize>,
    max_key_size: Option<usize>,
    max_value_size: Option<usize>,
    append_lanes: Option<usize>,
    region_memory_budget: Option<usize>,
    region_io_engine: IoEngineKind,
    region_io_mode: IoMode,
    region_io_queue_depth: Option<usize>,
    memory_capacity: Option<usize>,
    memory_shards: usize,
    small_object_max: usize,
    hybrid_memory_budget: Option<usize>,
    journal_capacity: u64,
    request_slots: usize,
    request_memory: usize,
    async_read_queue_depth: usize,
    async_write_queue_depth: usize,
    async_read_workers: usize,
    async_mutation_workers: usize,
    write_mode: HybridWriteMode,
    write_back_queue_depth: usize,
    write_back_workers: usize,
    write_back_memory: usize,
}

impl HybridOptions {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, CliError> {
        let mut arguments = arguments.into_iter();
        let command = match arguments.next().as_deref() {
            Some("hybrid-inspect") => HybridCommand::Inspect,
            Some("hybrid-verify") => HybridCommand::Verify,
            Some("hybrid-diagnose") => HybridCommand::Diagnose,
            Some("hybrid-format") => HybridCommand::Format,
            Some(command) => {
                return Err(CliError::Usage(format!(
                    "unknown Hybrid command {command:?}"
                )));
            }
            None => return Err(CliError::Usage("a Hybrid command is required".into())),
        };
        let mut bucket_path = None;
        let mut region_path = None;
        let mut manifest_path = None;
        let mut output = Output::Human;
        let mut bucket_capacity = None;
        let mut bucket_size = 4 * 1024;
        let mut bucket_memory_budget = 1024 * 1024 * 1024;
        let mut bucket_buffer_slots = 64;
        let mut bucket_io_engine = IoEngineKind::Sync;
        let mut bucket_io_mode = IoMode::Buffered;
        let mut bucket_io_queue_depth = 64;
        let mut region_capacity = None;
        let mut region_size = None;
        let mut index_slots = None;
        let mut max_key_size = None;
        let mut max_value_size = None;
        let mut append_lanes = None;
        let mut region_memory_budget = None;
        let mut region_io_engine = IoEngineKind::Sync;
        let mut region_io_mode = IoMode::Buffered;
        let mut region_io_queue_depth = None;
        let mut memory_capacity = None;
        let mut memory_shards = 256;
        let mut small_object_max = 1024;
        let mut hybrid_memory_budget = None;
        let mut journal_capacity = 16 * 1024 * 1024;
        let mut request_slots = 256;
        let mut request_memory = 64 * 1024 * 1024;
        let mut async_read_queue_depth = 256;
        let mut async_write_queue_depth = 256;
        let mut async_read_workers = 64;
        let mut async_mutation_workers = 4;
        let mut write_mode = HybridWriteMode::WriteBack;
        let mut write_back_queue_depth = 64;
        let mut write_back_workers = 4;
        let mut write_back_memory = 32 * 1024 * 1024;
        let mut confirmed = false;
        let mut arguments = arguments.peekable();
        while let Some(argument) = arguments.next() {
            if matches!(argument.as_str(), "--help" | "-h") {
                print_help();
                return Err(CliError::Usage(
                    "Hybrid help was printed; no command was run".into(),
                ));
            }
            let (name, inline) = argument
                .split_once('=')
                .map_or((argument.as_str(), None), |(name, value)| {
                    (name, Some(value.to_owned()))
                });
            if name == "--yes" {
                if inline.is_some() {
                    return Err(CliError::Usage("--yes does not take a value".into()));
                }
                confirmed = true;
                continue;
            }
            if !command.needs_config()
                && !matches!(
                    name,
                    "--bucket-path" | "--region-path" | "--manifest-path" | "--output"
                )
            {
                return Err(CliError::Usage(
                    "Hybrid inspect/verify accept only the three paths and --output".into(),
                ));
            }
            let value = inline.map_or_else(
                || {
                    arguments
                        .next()
                        .filter(|value| !value.starts_with('-'))
                        .ok_or_else(|| CliError::Usage(format!("missing value for {name}")))
                },
                Ok,
            )?;
            match name {
                "--bucket-path" => bucket_path = Some(PathBuf::from(value)),
                "--region-path" => region_path = Some(PathBuf::from(value)),
                "--manifest-path" => manifest_path = Some(PathBuf::from(value)),
                "--bucket-capacity" => bucket_capacity = Some(parse_bytes(&value, name)?),
                "--bucket-size" => bucket_size = parse_usize_bytes(&value, name)?,
                "--bucket-memory-budget" => bucket_memory_budget = parse_usize_bytes(&value, name)?,
                "--bucket-buffer-slots" => bucket_buffer_slots = parse_number(&value, name)?,
                "--bucket-engine" => bucket_io_engine = parse_io_engine(&value, name)?,
                "--bucket-mode" => bucket_io_mode = parse_io_mode(&value, name)?,
                "--bucket-io-queue-depth" => bucket_io_queue_depth = parse_number(&value, name)?,
                "--region-capacity" => region_capacity = Some(parse_bytes(&value, name)?),
                "--region-size" => region_size = Some(parse_bytes(&value, name)?),
                "--index-slots" => index_slots = Some(parse_number(&value, name)?),
                "--max-key-size" => max_key_size = Some(parse_usize_bytes(&value, name)?),
                "--max-value-size" => max_value_size = Some(parse_usize_bytes(&value, name)?),
                "--append-lanes" => append_lanes = Some(parse_number(&value, name)?),
                "--region-memory-budget" => {
                    region_memory_budget = Some(parse_usize_bytes(&value, name)?)
                }
                "--region-engine" => region_io_engine = parse_io_engine(&value, name)?,
                "--region-mode" => region_io_mode = parse_io_mode(&value, name)?,
                "--region-io-queue-depth" => {
                    region_io_queue_depth = Some(parse_number(&value, name)?)
                }
                "--memory-capacity" => memory_capacity = Some(parse_usize_bytes(&value, name)?),
                "--memory-shards" => memory_shards = parse_number(&value, name)?,
                "--small-object-max" => small_object_max = parse_usize_bytes(&value, name)?,
                "--hybrid-memory-budget" => {
                    hybrid_memory_budget = Some(parse_usize_bytes(&value, name)?)
                }
                "--journal-capacity" => journal_capacity = parse_bytes(&value, name)?,
                "--request-slots" => request_slots = parse_number(&value, name)?,
                "--request-memory" => request_memory = parse_usize_bytes(&value, name)?,
                "--async-read-queue-depth" => async_read_queue_depth = parse_number(&value, name)?,
                "--async-write-queue-depth" => {
                    async_write_queue_depth = parse_number(&value, name)?
                }
                "--async-read-workers" => async_read_workers = parse_number(&value, name)?,
                "--async-mutation-workers" => async_mutation_workers = parse_number(&value, name)?,
                "--write-mode" => {
                    write_mode = match value.as_str() {
                        "write-through" | "write_through" | "through" => {
                            HybridWriteMode::WriteThrough
                        }
                        "write-back" | "write_back" | "back" => HybridWriteMode::WriteBack,
                        _ => {
                            return Err(CliError::Usage(
                                "--write-mode must be write-through or write-back".into(),
                            ));
                        }
                    }
                }
                "--write-back-queue-depth" => write_back_queue_depth = parse_number(&value, name)?,
                "--write-back-workers" => write_back_workers = parse_number(&value, name)?,
                "--write-back-memory" => write_back_memory = parse_usize_bytes(&value, name)?,
                "--output" => {
                    output = match value.as_str() {
                        "human" => Output::Human,
                        "json" => Output::Json,
                        _ => {
                            return Err(CliError::Usage("--output must be human or json".into()));
                        }
                    }
                }
                _ => return Err(CliError::Usage(format!("unknown Hybrid option {name}"))),
            }
        }
        let bucket_path = required_path(bucket_path, "--bucket-path")?;
        let region_path = required_path(region_path, "--region-path")?;
        let manifest_path = required_path(manifest_path, "--manifest-path")?;
        if bucket_path == region_path
            || bucket_path == manifest_path
            || region_path == manifest_path
        {
            return Err(CliError::Usage(
                "Hybrid Bucket, Region, and manifest paths must be distinct".into(),
            ));
        }
        if command.needs_config()
            && (bucket_capacity.is_none() || region_capacity.is_none() || memory_capacity.is_none())
        {
            return Err(CliError::Usage(
                "Hybrid diagnose/format require --bucket-capacity, --region-capacity, and --memory-capacity"
                    .into(),
            ));
        }
        if !command.needs_config()
            && (bucket_capacity.is_some() || region_capacity.is_some() || memory_capacity.is_some())
        {
            return Err(CliError::Usage(
                "Hybrid inspect/verify accept only the three paths and --output".into(),
            ));
        }
        if command == HybridCommand::Format && !confirmed {
            return Err(CliError::Usage(
                "hybrid-format requires explicit --yes confirmation".into(),
            ));
        }
        if command != HybridCommand::Format && confirmed {
            return Err(CliError::Usage(
                "--yes is valid only for hybrid-format".into(),
            ));
        }
        Ok(Self {
            command,
            bucket_path,
            region_path,
            manifest_path,
            output,
            bucket_capacity,
            bucket_size,
            bucket_memory_budget,
            bucket_buffer_slots,
            bucket_io_engine,
            bucket_io_mode,
            bucket_io_queue_depth,
            region_capacity,
            region_size,
            index_slots,
            max_key_size,
            max_value_size,
            append_lanes,
            region_memory_budget,
            region_io_engine,
            region_io_mode,
            region_io_queue_depth,
            memory_capacity,
            memory_shards,
            small_object_max,
            hybrid_memory_budget,
            journal_capacity,
            request_slots,
            request_memory,
            async_read_queue_depth,
            async_write_queue_depth,
            async_read_workers,
            async_mutation_workers,
            write_mode,
            write_back_queue_depth,
            write_back_workers,
            write_back_memory,
        })
    }
}

fn run_hybrid(arguments: Vec<String>) -> Result<u8, CliError> {
    if arguments
        .iter()
        .skip(1)
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        print_help();
        return Ok(0);
    }
    let options = HybridOptions::parse(arguments)?;
    match options.command {
        HybridCommand::Inspect => {
            let report = inspect_hybrid_cache_files(
                &options.bucket_path,
                &options.region_path,
                &options.manifest_path,
            )?;
            print_hybrid_inspect(&options, &report);
            Ok(if report.valid { 0 } else { EXIT_INVALID })
        }
        HybridCommand::Verify => {
            let report = verify_hybrid_cache_files(
                &options.bucket_path,
                &options.region_path,
                &options.manifest_path,
            )?;
            print_hybrid_verify(&options, &report);
            Ok(if report.valid { 0 } else { EXIT_INVALID })
        }
        HybridCommand::Diagnose => {
            let diagnostics = build_hybrid_config(&options)?.diagnostics()?;
            print_hybrid_diagnostics(&options, &diagnostics);
            Ok(0)
        }
        HybridCommand::Format => {
            ensure_hybrid_format_targets_empty(&options)?;
            let diagnostics = build_hybrid_config(&options)?.diagnostics()?;
            let cache = build_hybrid_config(&options)?.open()?;
            cache.close()?;
            let verified = verify_hybrid_cache_files(
                &options.bucket_path,
                &options.region_path,
                &options.manifest_path,
            )?;
            if !verified.valid {
                return Err(CliError::Usage(
                    "newly formatted Hybrid files failed offline verification".into(),
                ));
            }
            print_hybrid_format(&options, &diagnostics, &verified);
            Ok(0)
        }
    }
}

fn required_path(path: Option<PathBuf>, name: &str) -> Result<PathBuf, CliError> {
    let path = path.ok_or_else(|| CliError::Usage(format!("{name} is required")))?;
    if path.as_os_str().is_empty() {
        Err(CliError::Usage(format!("{name} must not be empty")))
    } else {
        Ok(path)
    }
}

fn parse_io_engine(value: &str, name: &str) -> Result<IoEngineKind, CliError> {
    match value {
        "sync" => Ok(IoEngineKind::Sync),
        "auto" => Ok(IoEngineKind::Auto),
        "uring" | "io_uring" | "io-uring" => Ok(IoEngineKind::IoUring),
        _ => Err(CliError::Usage(format!(
            "{name} must be sync, auto, or uring"
        ))),
    }
}

fn parse_io_mode(value: &str, name: &str) -> Result<IoMode, CliError> {
    match value {
        "buffered" => Ok(IoMode::Buffered),
        "auto" => Ok(IoMode::Auto),
        "direct" => Ok(IoMode::Direct),
        _ => Err(CliError::Usage(format!(
            "{name} must be buffered, auto, or direct"
        ))),
    }
}

fn build_hybrid_config(options: &HybridOptions) -> Result<HybridCacheConfig, CliError> {
    let bucket_capacity = options
        .bucket_capacity
        .ok_or_else(|| CliError::Usage("--bucket-capacity is required".into()))?;
    let region_capacity = options
        .region_capacity
        .ok_or_else(|| CliError::Usage("--region-capacity is required".into()))?;
    let memory_capacity = options
        .memory_capacity
        .ok_or_else(|| CliError::Usage("--memory-capacity is required".into()))?;
    let bucket = BucketCacheConfig::new(&options.bucket_path, bucket_capacity)
        .with_bucket_size(options.bucket_size)
        .with_memory_budget(options.bucket_memory_budget)
        .with_buffer_slots(options.bucket_buffer_slots)
        .with_io_engine(options.bucket_io_engine)
        .with_io_mode(options.bucket_io_mode)
        .with_io_queue_depth(options.bucket_io_queue_depth);
    let mut region = CacheConfig::new(&options.region_path, region_capacity)
        .with_io_engine(options.region_io_engine)
        .with_io_mode(options.region_io_mode);
    if let Some(bytes) = options.region_size {
        region = region.with_region_size(bytes);
    }
    if let Some(slots) = options.index_slots {
        region = region.with_index_slots(slots);
    }
    if let Some(bytes) = options.max_key_size {
        region = region.with_max_key_size(bytes);
    }
    if let Some(bytes) = options.max_value_size {
        region = region.with_max_value_size(bytes);
    }
    if let Some(lanes) = options.append_lanes {
        region = region.with_append_lanes(lanes);
    }
    if let Some(bytes) = options.region_memory_budget {
        region = region.with_memory_budget(bytes);
    }
    if let Some(depth) = options.region_io_queue_depth {
        region = region.with_io_queue_depth(depth);
    }
    let mut hybrid = HybridCacheConfig::new(memory_capacity, bucket, region)
        .with_manifest_path(&options.manifest_path)
        .with_memory_shards(options.memory_shards)
        .with_small_object_max(options.small_object_max)
        .with_journal_capacity(options.journal_capacity)
        .with_request_slots(options.request_slots)
        .with_request_memory(options.request_memory)
        .with_async_queue_depths(
            options.async_read_queue_depth,
            options.async_write_queue_depth,
        )
        .with_async_workers(options.async_read_workers, options.async_mutation_workers)
        .with_write_mode(options.write_mode)
        .with_write_back_resources(
            options.write_back_queue_depth,
            options.write_back_workers,
            options.write_back_memory,
        );
    if let Some(bytes) = options.hybrid_memory_budget {
        hybrid = hybrid.with_memory_budget(bytes);
    }
    Ok(hybrid)
}

fn ensure_hybrid_format_targets_empty(options: &HybridOptions) -> Result<(), CliError> {
    for (name, path) in [
        ("Bucket", &options.bucket_path),
        ("Region", &options.region_path),
        ("manifest", &options.manifest_path),
    ] {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(CliError::Usage(format!(
                    "Hybrid {name} path {} must not be a symbolic link",
                    path.display()
                )));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(CliError::Usage(format!(
                    "Hybrid {name} path {} must be a regular file",
                    path.display()
                )));
            }
            Ok(metadata) if metadata.len() != 0 => {
                return Err(CliError::Usage(format!(
                    "hybrid-format refuses non-empty {name} path {}; use new empty paths",
                    path.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CliError::Usage(format!(
                    "cannot inspect Hybrid {name} path {}: {error}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn build_config(options: &Options) -> Result<CacheConfig, CliError> {
    let capacity = options.capacity.ok_or_else(|| {
        CliError::Usage(format!(
            "--capacity is required for {}",
            options.command.as_str()
        ))
    })?;
    let mut config = CacheConfig::new(&options.path, capacity);
    if let Some(bytes) = options.region_size {
        config = config.with_region_size(bytes);
    }
    if let Some(slots) = options.index_slots {
        config = config.with_index_slots(slots);
    }
    if let Some(bytes) = options.max_key_size {
        config = config.with_max_key_size(bytes);
    }
    if let Some(bytes) = options.max_value_size {
        config = config.with_max_value_size(bytes);
    }
    if let Some(seed) = options.hash_seed {
        config = config.with_hash_seed(seed);
    }
    if let Some(lanes) = options.append_lanes {
        config = config.with_append_lanes(lanes);
    }
    if let Some(bytes) = options.memory_budget {
        config = config.with_memory_budget(bytes);
    }
    if options.read_queue_depth.is_some() || options.write_queue_depth.is_some() {
        config = config.with_submission_queue_depths(
            options.read_queue_depth.unwrap_or(2),
            options.write_queue_depth.unwrap_or(2),
        );
    }
    if let Some(depth) = options.io_queue_depth {
        config = config.with_io_queue_depth(depth);
    }
    Ok(config)
}

fn parse_bytes(value: &str, name: &str) -> Result<u64, CliError> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '_')
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    if number.is_empty() {
        return Err(CliError::Usage(format!(
            "invalid byte value {value:?} for {name}"
        )));
    }
    let number = number
        .replace('_', "")
        .parse::<u64>()
        .map_err(|_| CliError::Usage(format!("invalid byte value {value:?} for {name}")))?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024_u64 * 1024 * 1024 * 1024,
        _ => {
            return Err(CliError::Usage(format!(
                "invalid byte suffix in {value:?} for {name}"
            )));
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| CliError::Usage(format!("byte value for {name} is too large")))
}

fn parse_usize_bytes(value: &str, name: &str) -> Result<usize, CliError> {
    usize::try_from(parse_bytes(value, name)?)
        .map_err(|_| CliError::Usage(format!("byte value for {name} does not fit this platform")))
}

fn parse_number<T>(value: &str, name: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
{
    value
        .replace('_', "")
        .parse::<T>()
        .map_err(|_| CliError::Usage(format!("invalid value {value:?} for {name}")))
}

fn parse_u64(value: &str, name: &str) -> Result<u64, CliError> {
    let compact = value.replace('_', "");
    if let Some(hex) = compact
        .strip_prefix("0x")
        .or_else(|| compact.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16)
            .map_err(|_| CliError::Usage(format!("invalid value {value:?} for {name}")))
    } else {
        parse_number(&compact, name)
    }
}

fn print_hybrid_diagnostics(options: &HybridOptions, report: &HybridConfigDiagnostics) {
    match options.output {
        Output::Human => {
            println!("cache-rs Hybrid configuration diagnostics");
            println!("  result:                    pass");
            println!(
                "  Bucket path:               {}",
                report.bucket.path.display()
            );
            println!(
                "  Region path:               {}",
                report.region.path.display()
            );
            println!(
                "  manifest path:             {}",
                report.manifest_path.display()
            );
            println!(
                "  L1 capacity/shards:         {} B / {}",
                report.memory_capacity_bytes, report.memory_shards
            );
            println!(
                "  Bucket capacity/pages:      {} B / {} x {} B",
                report.bucket.capacity_bytes,
                report.bucket.bucket_count,
                report.bucket.bucket_size_bytes
            );
            println!(
                "  Region data/regions:        {} B / {} x {} B",
                report.region.data_file_len_bytes,
                report.region.region_count,
                report.region.region_size_bytes
            );
            println!(
                "  small-object threshold:     {} B",
                report.small_object_max_bytes
            );
            println!(
                "  memory planned/budget:      {}/{} B",
                report.planned_memory_bytes, report.memory_budget_bytes
            );
            println!(
                "  Region checkpoint accounting: {} B",
                report.region.checkpoint_accounting_bytes
            );
            println!(
                "  request slots/bytes:        {} / {} B",
                report.request_slots, report.request_memory_bytes
            );
            println!(
                "  maximum read reservation:   {} B",
                report.maximum_read_temporary_bytes
            );
            println!(
                "  async read/write/workers:   {}/{}/{}+{}",
                report.async_read_queue_depth,
                report.async_write_queue_depth,
                report.async_io_concurrency,
                report.async_mutation_workers
            );
            println!(
                "  write mode/queue/workers:   {}/{}/{} ({} B)",
                hybrid_write_mode_name(report.write_mode),
                report.write_back_queue_depth,
                report.write_back_workers,
                report.write_back_memory_bytes
            );
            println!(
                "  Bucket engine/mode/QD:      {}/{}/{}",
                io_engine_name(report.bucket.io_engine),
                io_mode_name(report.bucket.io_mode),
                report.bucket.io_queue_depth
            );
            println!(
                "  Region engine/mode/QD:      {}/{}/{}",
                io_engine_name(report.region.io_engine),
                io_mode_name(report.region.io_mode),
                report.region.io_queue_depth
            );
            println!(
                "  journal capacity/recovery:  {} / {} B",
                report.journal_capacity_bytes, report.journal_recovery_memory_bytes
            );
            println!("  namespaces:                 {}", report.namespace_count);
        }
        Output::Json => {
            println!("{}", hybrid_diagnostics_json("hybrid-diagnose", report));
        }
    }
}

fn hybrid_diagnostics_json(command: &str, report: &HybridConfigDiagnostics) -> String {
    let mut output = String::with_capacity(3072);
    write!(
        output,
        concat!(
            "{{\"schema_version\":1,\"command\":\"{}\",\"passed\":true,",
            "\"bucket_path\":\"{}\",\"region_path\":\"{}\",",
            "\"manifest_path\":\"{}\",",
            "\"memory_capacity_bytes\":{},\"memory_shards\":{},",
            "\"small_object_max_bytes\":{},",
            "\"memory_budget_bytes\":{},\"planned_memory_bytes\":{},",
            "\"configured_component_budget_bytes\":{},",
            "\"journal_capacity_bytes\":{},\"journal_recovery_memory_bytes\":{},",
            "\"request_slots\":{},\"request_memory_bytes\":{},",
            "\"maximum_read_temporary_bytes\":{},",
            "\"async_read_queue_depth\":{},\"async_write_queue_depth\":{},",
            "\"async_read_workers\":{},\"async_mutation_workers\":{},",
            "\"write_mode\":\"{}\",\"write_back_queue_depth\":{},",
            "\"write_back_workers\":{},\"write_back_memory_bytes\":{},",
            "\"write_back_overhead_bytes\":{},",
            "\"namespace_count\":{},",
            "\"bucket_capacity_bytes\":{},\"bucket_file_len_bytes\":{},",
            "\"bucket_size_bytes\":{},\"bucket_count\":{},",
            "\"bucket_maximum_item_bytes\":{},",
            "\"bucket_memory_budget_bytes\":{},\"bucket_planned_memory_bytes\":{},",
            "\"bucket_io_engine\":\"{}\",\"bucket_io_mode\":\"{}\",",
            "\"bucket_io_queue_depth\":{},",
            "\"region_requested_capacity_bytes\":{},",
            "\"region_file_len_bytes\":{},\"region_size_bytes\":{},",
            "\"region_count\":{},\"region_index_slots\":{},",
            "\"region_memory_budget_bytes\":{},\"region_planned_memory_bytes\":{},",
            "\"region_checkpoint_accounting_bytes\":{},",
            "\"region_io_engine\":\"{}\",\"region_io_mode\":\"{}\",",
            "\"region_io_queue_depth\":{}}}"
        ),
        command,
        json_escape(&report.bucket.path.to_string_lossy()),
        json_escape(&report.region.path.to_string_lossy()),
        json_escape(&report.manifest_path.to_string_lossy()),
        report.memory_capacity_bytes,
        report.memory_shards,
        report.small_object_max_bytes,
        report.memory_budget_bytes,
        report.planned_memory_bytes,
        report.configured_component_budget_bytes,
        report.journal_capacity_bytes,
        report.journal_recovery_memory_bytes,
        report.request_slots,
        report.request_memory_bytes,
        report.maximum_read_temporary_bytes,
        report.async_read_queue_depth,
        report.async_write_queue_depth,
        report.async_io_concurrency,
        report.async_mutation_workers,
        hybrid_write_mode_name(report.write_mode),
        report.write_back_queue_depth,
        report.write_back_workers,
        report.write_back_memory_bytes,
        report.write_back_overhead_bytes,
        report.namespace_count,
        report.bucket.capacity_bytes,
        report.bucket.file_len_bytes,
        report.bucket.bucket_size_bytes,
        report.bucket.bucket_count,
        report.bucket.maximum_item_bytes,
        report.bucket.memory_budget_bytes,
        report.bucket.planned_memory_bytes,
        io_engine_name(report.bucket.io_engine),
        io_mode_name(report.bucket.io_mode),
        report.bucket.io_queue_depth,
        report.region.requested_capacity_bytes,
        report.region.data_file_len_bytes,
        report.region.region_size_bytes,
        report.region.region_count,
        report.region.index_slots,
        report.region.memory_budget_bytes,
        report.region.planned_memory_bytes,
        report.region.checkpoint_accounting_bytes,
        io_engine_name(report.region.io_engine),
        io_mode_name(report.region.io_mode),
        report.region.io_queue_depth,
    )
    .expect("writing JSON into a String cannot fail");
    output
}

fn print_hybrid_inspect(options: &HybridOptions, report: &HybridInspectReport) {
    match options.output {
        Output::Human => {
            println!("cache-rs Hybrid offline inspection");
            print_hybrid_paths(options);
            println!("  result:                    {}", pass_name(report.valid));
            println!(
                "  manifest:                  valid={} clean={} generation={} journal={} B",
                report.manifest.valid,
                optional_bool(report.manifest.clean),
                optional_u64(report.manifest.generation),
                report.manifest.journal_valid_bytes
            );
            println!(
                "  Bucket:                    valid={} clean={} pages={} x {} B",
                report.bucket.valid,
                optional_bool(report.bucket.clean),
                optional_u64(report.bucket.bucket_count),
                report
                    .bucket
                    .bucket_size_bytes
                    .map_or_else(|| "n/a".into(), |value| value.to_string())
            );
            println!(
                "  Region:                    kind={} Regions={} invalid_headers={}",
                report.region.kind.as_str(),
                report.region.regions.expected,
                report.region.regions.invalid_headers
            );
        }
        Output::Json => println!("{}", hybrid_inspect_json("hybrid-inspect", options, report)),
    }
}

fn print_hybrid_verify(options: &HybridOptions, report: &HybridVerifyReport) {
    match options.output {
        Output::Human => {
            println!("cache-rs Hybrid offline verification");
            print_hybrid_paths(options);
            println!("  result:                    {}", pass_name(report.valid));
            println!(
                "  structurally safe to attempt open: {}",
                report.safe_to_open
            );
            println!(
                "  manifest journal:          {} records / {} B, torn_tail={}",
                report.manifest.journal_records,
                report.manifest.journal_valid_bytes,
                report.manifest.journal_torn_tail
            );
            println!(
                "  manifest identity:         cache_id={} layout_fingerprint={}",
                json_optional_cache_id(report.manifest.cache_id),
                json_optional_u64(report.manifest.layout_fingerprint),
            );
            println!(
                "  Bucket pages:              {} verified, {} current, {} stale, {} empty, {} invalid",
                report.bucket.buckets_verified,
                report.bucket.current_epoch_buckets,
                report.bucket.stale_epoch_buckets,
                report.bucket.empty_buckets,
                report.bucket.invalid_buckets
            );
            println!(
                "  Bucket entries:            {} verified",
                report.bucket.entries_verified
            );
            println!(
                "  Region records/issues:     {}/{}",
                report.region.records_verified, report.region.issues_total
            );
        }
        Output::Json => println!("{}", hybrid_verify_json(options, report)),
    }
}

fn print_hybrid_format(
    options: &HybridOptions,
    diagnostics: &HybridConfigDiagnostics,
    verified: &HybridVerifyReport,
) {
    match options.output {
        Output::Human => {
            println!("cache-rs Hybrid format");
            print_hybrid_paths(options);
            println!("  result:                    pass");
            println!("  offline verification:      {}", pass_name(verified.valid));
            println!(
                "  planned memory:            {} B",
                diagnostics.planned_memory_bytes
            );
            println!(
                "  Bucket/Region file bytes:  {}/{}",
                diagnostics.bucket.file_len_bytes, diagnostics.region.data_file_len_bytes
            );
        }
        Output::Json => {
            let base = hybrid_diagnostics_json("hybrid-format", diagnostics);
            let mut output = base.strip_suffix('}').unwrap_or(&base).to_owned();
            write!(
                output,
                ",\"offline_verification_passed\":{},\"safe_to_open\":{}}}",
                verified.valid, verified.safe_to_open
            )
            .expect("writing JSON into a String cannot fail");
            println!("{output}");
        }
    }
}

fn hybrid_inspect_json(
    command: &str,
    options: &HybridOptions,
    report: &HybridInspectReport,
) -> String {
    let mut output = String::with_capacity(3072);
    write!(
        output,
        concat!(
            "{{\"schema_version\":1,\"command\":\"{}\",\"passed\":{},",
            "\"bucket_path\":\"{}\",\"region_path\":\"{}\",",
            "\"manifest_path\":\"{}\",",
            "\"manifest\":{{\"valid\":{},\"file_len\":{},",
            "\"selected_slot\":{},\"generation\":{},\"clean\":{},",
            "\"cache_id\":{},\"layout_fingerprint\":{},",
            "\"version_epoch\":{},\"next_seqno\":{},",
            "\"journal_generation\":{},\"journal_capacity_bytes\":{},",
            "\"journal_valid_bytes\":{},\"journal_records\":{},",
            "\"journal_torn_tail\":{},\"recovery_required\":{}}},",
            "\"bucket\":{{\"valid\":{},\"file_len\":{},",
            "\"selected_superblock\":{},\"generation\":{},\"clean\":{},",
            "\"bucket_size_bytes\":{},\"bucket_count\":{},\"epoch\":{},",
            "\"expected_file_len\":{},\"buckets_verified\":{},",
            "\"current_epoch_buckets\":{},\"stale_epoch_buckets\":{},",
            "\"empty_buckets\":{},\"entries_verified\":{},",
            "\"invalid_buckets\":{}}},",
            "\"region\":{{\"file_kind\":\"{}\",\"file_len\":{},",
            "\"selected_superblock\":{},\"region_count\":{},",
            "\"invalid_headers\":{},\"truncated_headers\":{}}}}}"
        ),
        command,
        report.valid,
        json_escape(&options.bucket_path.to_string_lossy()),
        json_escape(&options.region_path.to_string_lossy()),
        json_escape(&options.manifest_path.to_string_lossy()),
        report.manifest.valid,
        report.manifest.file_len,
        json_optional_u8(report.manifest.selected_slot),
        json_optional_u64(report.manifest.generation),
        json_optional_bool(report.manifest.clean),
        json_optional_cache_id(report.manifest.cache_id),
        json_optional_u64(report.manifest.layout_fingerprint),
        json_optional_u64(report.manifest.version_epoch),
        json_optional_u64(report.manifest.next_seqno),
        json_optional_u64(report.manifest.journal_generation),
        json_optional_u64(report.manifest.journal_capacity_bytes),
        report.manifest.journal_valid_bytes,
        report.manifest.journal_records,
        report.manifest.journal_torn_tail,
        report.manifest.recovery_required,
        report.bucket.valid,
        report.bucket.file_len,
        json_optional_u8(report.bucket.selected_superblock),
        json_optional_u64(report.bucket.generation),
        json_optional_bool(report.bucket.clean),
        json_optional_u32(report.bucket.bucket_size_bytes),
        json_optional_u64(report.bucket.bucket_count),
        json_optional_u64(report.bucket.epoch),
        json_optional_u64(report.bucket.expected_file_len),
        report.bucket.buckets_verified,
        report.bucket.current_epoch_buckets,
        report.bucket.stale_epoch_buckets,
        report.bucket.empty_buckets,
        report.bucket.entries_verified,
        report.bucket.invalid_buckets,
        report.region.kind.as_str(),
        report.region.file_len,
        json_optional_u8(report.region.selected_superblock),
        report.region.regions.expected,
        report.region.regions.invalid_headers,
        report.region.regions.truncated_headers,
    )
    .expect("writing JSON into a String cannot fail");
    output
}

fn hybrid_verify_json(options: &HybridOptions, report: &HybridVerifyReport) -> String {
    let inspect = HybridInspectReport {
        valid: report.valid,
        bucket: report.bucket,
        region: report.region.inspect.clone(),
        manifest: report.manifest,
    };
    let base = hybrid_inspect_json("hybrid-verify", options, &inspect);
    let mut output = base.strip_suffix('}').unwrap_or(&base).to_owned();
    write!(
        output,
        concat!(
            ",\"valid\":{},\"safe_to_open\":{},\"structurally_safe_to_attempt_open\":{},",
            "\"region_records_verified\":{},\"region_issues_total\":{},",
            "\"region_reopen_disposition\":\"{}\"}}"
        ),
        report.valid,
        report.safe_to_open,
        report.safe_to_open,
        report.region.records_verified,
        report.region.issues_total,
        report.region.reopen_disposition.as_str(),
    )
    .expect("writing JSON into a String cannot fail");
    output
}

fn print_hybrid_paths(options: &HybridOptions) {
    println!(
        "  Bucket path:               {}",
        options.bucket_path.display()
    );
    println!(
        "  Region path:               {}",
        options.region_path.display()
    );
    println!(
        "  manifest path:             {}",
        options.manifest_path.display()
    );
}

fn pass_name(passed: bool) -> &'static str {
    if passed { "pass" } else { "FAIL" }
}

fn print_diagnostics_human(report: &ConfigDiagnostics) {
    println!("cache-rs configuration diagnostics");
    println!("  path:                  {}", report.path.display());
    println!("  result:                pass");
    println!(
        "  requested/data bytes: {}/{}",
        report.requested_capacity_bytes, report.data_file_len_bytes
    );
    println!(
        "  Regions:               {} x {} B",
        report.region_count, report.region_size_bytes
    );
    println!("  index slots:           {}", report.index_slots);
    println!("  append lanes:          {}", report.append_lanes);
    println!("  maximum record:        {} B", report.maximum_record_bytes);
    println!(
        "  memory fixed/budget:   {}/{} B",
        report.planned_memory_bytes, report.memory_budget_bytes
    );
    println!(
        "  read/write/device QD:  {}/{}/{}",
        report.read_submission_depth, report.write_submission_depth, report.io_queue_depth
    );
    println!(
        "  I/O engine/mode:       {}/{}",
        io_engine_name(report.io_engine),
        io_mode_name(report.io_mode)
    );
    println!(
        "  recovery mode:         {}",
        recovery_mode_name(report.recovery_mode)
    );
    println!(
        "  checkpoint slot:       {} B",
        report.checkpoint_slot_bytes
    );
    println!(
        "  checkpoint accounting: {} B",
        report.checkpoint_accounting_bytes
    );
}

fn diagnostics_json(report: &ConfigDiagnostics) -> String {
    diagnostics_json_with_command("diagnose", report)
}

fn diagnostics_json_with_command(command: &str, report: &ConfigDiagnostics) -> String {
    let mut output = String::with_capacity(2048);
    write!(
        output,
        concat!(
            "{{\"schema_version\":1,\"command\":\"{}\",",
            "\"path\":\"{}\",\"passed\":true,",
            "\"requested_capacity_bytes\":{},\"data_file_len_bytes\":{},",
            "\"region_size_bytes\":{},\"region_count\":{},",
            "\"index_slots\":{},\"append_lanes\":{},",
            "\"maximum_record_bytes\":{},",
            "\"memory_budget_bytes\":{},\"planned_memory_bytes\":{},",
            "\"read_submission_depth\":{},\"write_submission_depth\":{},",
            "\"io_queue_depth\":{},\"io_engine\":\"{}\",",
            "\"io_mode\":\"{}\",\"recovery_mode\":\"{}\",",
            "\"checkpoint_slot_bytes\":{},\"checkpoint_accounting_bytes\":{}}}"
        ),
        command,
        json_escape(&report.path.to_string_lossy()),
        report.requested_capacity_bytes,
        report.data_file_len_bytes,
        report.region_size_bytes,
        report.region_count,
        report.index_slots,
        report.append_lanes,
        report.maximum_record_bytes,
        report.memory_budget_bytes,
        report.planned_memory_bytes,
        report.read_submission_depth,
        report.write_submission_depth,
        report.io_queue_depth,
        io_engine_name(report.io_engine),
        io_mode_name(report.io_mode),
        recovery_mode_name(report.recovery_mode),
        report.checkpoint_slot_bytes,
        report.checkpoint_accounting_bytes,
    )
    .expect("writing JSON into a String cannot fail");
    output
}

fn print_format_human(
    command: Command,
    diagnostics: &ConfigDiagnostics,
    startup: &StartupDiagnostics,
) {
    println!("cache-rs {}", command.as_str());
    println!("  path:                  {}", diagnostics.path.display());
    println!("  result:                pass");
    println!(
        "  data file length:      {} B",
        diagnostics.data_file_len_bytes
    );
    println!("  Regions:               {}", diagnostics.region_count);
    println!("  startup status:        {}", status_name(startup.status));
    println!("  checkpoint loaded:     {}", startup.checkpoint_loaded);
    println!("  recovered entries:     {}", startup.recovered_entries);
}

fn format_json(
    command: Command,
    diagnostics: &ConfigDiagnostics,
    startup: &StartupDiagnostics,
) -> String {
    let base = diagnostics_json_with_command(command.as_str(), diagnostics);
    let mut output = base
        .strip_suffix('}')
        .expect("diagnostics JSON ends with an object delimiter")
        .to_owned();
    write!(
        output,
        concat!(
            ",\"startup_status\":\"{}\",\"checkpoint_loaded\":{},",
            "\"checkpoint_fallbacks\":{},\"recovered_entries\":{},",
            "\"recovery_regions_scanned\":{},\"recovery_records_scanned\":{},",
            "\"recovery_elapsed_us\":{},\"io_uring_active\":{},",
            "\"direct_io_active\":{}}}"
        ),
        status_name(startup.status),
        startup.checkpoint_loaded,
        startup.checkpoint_fallbacks,
        startup.recovered_entries,
        startup.recovery_regions_scanned,
        startup.recovery_records_scanned,
        startup.recovery_elapsed_us,
        startup.io_uring_active,
        startup.direct_io_active,
    )
    .expect("writing JSON into a String cannot fail");
    output
}

fn status_name(status: CacheStatus) -> &'static str {
    match status {
        CacheStatus::Healthy => "healthy",
        CacheStatus::MissOnly => "miss_only",
        CacheStatus::Poisoned => "poisoned",
        CacheStatus::Closed => "closed",
        _ => "unknown",
    }
}

fn io_engine_name(engine: IoEngineKind) -> &'static str {
    match engine {
        IoEngineKind::Sync => "sync",
        IoEngineKind::Auto => "auto",
        IoEngineKind::IoUring => "io_uring",
        _ => "unknown",
    }
}

fn hybrid_write_mode_name(mode: HybridWriteMode) -> &'static str {
    match mode {
        HybridWriteMode::WriteThrough => "write_through",
        HybridWriteMode::WriteBack => "write_back",
    }
}

fn io_mode_name(mode: IoMode) -> &'static str {
    match mode {
        IoMode::Buffered => "buffered",
        IoMode::Auto => "auto",
        IoMode::Direct => "direct",
        _ => "unknown",
    }
}

fn recovery_mode_name(mode: RecoveryMode) -> &'static str {
    match mode {
        RecoveryMode::Blocking => "blocking",
        RecoveryMode::MissOnly => "miss_only",
        _ => "unknown",
    }
}

fn inspect_passed(report: &InspectReport) -> bool {
    report.kind == CacheFileKind::FormatV1
        && report
            .data_file_len
            .is_some_and(|data_file_len| report.file_len >= data_file_len)
        && report.regions.invalid_headers == 0
        && report.regions.truncated_headers == 0
        && matches!(
            report.checkpoint.directory_state,
            CheckpointDirectoryState::Absent | CheckpointDirectoryState::Valid
        )
        && report.checkpoint.slots.iter().all(|slot| {
            matches!(
                slot.state,
                CheckpointSlotState::Absent | CheckpointSlotState::HeaderValid
            )
        })
}

fn print_inspect_human(path: &Path, report: &InspectReport) {
    println!("cache-rs offline inspection");
    println!("  path:                 {}", path.display());
    println!("  file kind:            {}", report.kind.as_str());
    if let CacheFileKind::Unsupported(version) = report.kind {
        println!("  unsupported version:  {version}");
    }
    println!("  file length:          {} B", report.file_len);
    println!(
        "  selected Superblock: {}",
        optional_u8(report.selected_superblock)
    );
    if let Some(superblock) = report.selected() {
        println!(
            "  generation/clean:     {}/{}",
            optional_u64(superblock.generation),
            optional_bool(superblock.clean)
        );
        println!(
            "  Regions:              {} total, {} free, {} active, {} sealed",
            report.regions.expected,
            report.regions.free,
            report.regions.active,
            report.regions.sealed
        );
        println!(
            "  Region problems:      {} invalid, {} truncated",
            report.regions.invalid_headers, report.regions.truncated_headers
        );
        println!(
            "  persisted record span: {} B",
            report.regions.record_extent_bytes
        );
    }
    println!(
        "  checkpoint directory: {}",
        report.checkpoint.directory_state.as_str()
    );
    for slot in report.checkpoint.slots {
        println!(
            "  checkpoint slot {}:    {} (generation {}, version {}, matches={})",
            slot.slot,
            slot.state.as_str(),
            optional_u64(slot.generation),
            optional_u16(slot.version),
            slot.matches_selected_superblock
        );
    }
    println!(
        "  inspection:           {}",
        if inspect_passed(report) {
            "pass"
        } else {
            "FAIL"
        }
    );
}

fn print_verify_human(path: &Path, report: &VerifyReport) {
    print_inspect_human(path, &report.inspect);
    println!("verification");
    println!(
        "  result:               {}",
        if report.valid { "pass" } else { "FAIL" }
    );
    println!("  safe to open:         {}", report.safe_to_open);
    println!(
        "  reopen disposition:   {}",
        report.reopen_disposition.as_str()
    );
    println!("  Regions verified:     {}", report.regions_verified);
    println!(
        "  records verified:     {} ({} values, {} tombstones, {} B)",
        report.records_verified,
        report.values_verified,
        report.tombstones_verified,
        report.record_bytes_verified
    );
    println!(
        "  checkpoint slots:     {} verified, selected {}",
        report.checkpoint_slots_verified,
        optional_u8(report.selected_verified_checkpoint)
    );
    println!(
        "  issues:               {} total, {} shown",
        report.issues_total,
        report.issues.len()
    );
    for issue in &report.issues {
        println!(
            "    {} at {}{}{}: {}",
            issue.component.as_str(),
            issue.offset,
            issue
                .region_id
                .map_or_else(String::new, |id| format!(", region {id}")),
            issue
                .checkpoint_slot
                .map_or_else(String::new, |slot| format!(", slot {slot}")),
            issue.message
        );
    }
}

fn inspect_json(path: &Path, report: &InspectReport) -> String {
    inspect_json_with_command("inspect", path, report, inspect_passed(report))
}

fn inspect_json_with_command(
    command: &str,
    path: &Path,
    report: &InspectReport,
    passed: bool,
) -> String {
    let mut output = String::with_capacity(4096);
    write!(
        output,
        concat!(
            "{{\"schema_version\":1,\"command\":\"{}\",",
            "\"path\":\"{}\",\"passed\":{},\"file_len\":{},",
            "\"file_kind\":\"{}\",\"unsupported_version\":{},",
            "\"selected_superblock\":{},\"data_file_len\":{},"
        ),
        command,
        json_escape(&path.to_string_lossy()),
        passed,
        report.file_len,
        report.kind.as_str(),
        match report.kind {
            CacheFileKind::Unsupported(version) => version.to_string(),
            _ => "null".into(),
        },
        json_optional_u8(report.selected_superblock),
        json_optional_u64(report.data_file_len),
    )
    .expect("writing JSON into a String cannot fail");
    output.push_str("\"superblocks\":[");
    for (index, superblock) in report.superblocks.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        write!(
            output,
            concat!(
                "{{\"slot\":{},\"state\":\"{}\",\"version\":{},",
                "\"generation\":{},\"clean\":{},\"region_size\":{},",
                "\"region_count\":{},\"epoch\":{},",
                "\"epoch_start_seqno\":{},\"next_seqno\":{},\"hash_seed\":{}}}"
            ),
            superblock.slot,
            superblock.state.as_str(),
            json_optional_u16(superblock.version),
            json_optional_u64(superblock.generation),
            json_optional_bool(superblock.clean),
            json_optional_u64(superblock.region_size),
            json_optional_u32(superblock.region_count),
            json_optional_u32(superblock.epoch),
            json_optional_u64(superblock.epoch_start_seqno),
            json_optional_u64(superblock.next_seqno),
            json_optional_u64(superblock.hash_seed),
        )
        .expect("writing JSON into a String cannot fail");
    }
    write!(
        output,
        concat!(
            "],\"regions\":{{\"expected\":{},\"valid_headers\":{},",
            "\"invalid_headers\":{},\"truncated_headers\":{},",
            "\"free\":{},\"active\":{},\"sealed\":{},",
            "\"record_extent_bytes\":{}}},",
            "\"checkpoint\":{{\"directory_state\":\"{}\",",
            "\"slot_size\":{},\"expected_file_len\":{},",
            "\"selected_header_slot\":{},\"slots\":["
        ),
        report.regions.expected,
        report.regions.valid_headers,
        report.regions.invalid_headers,
        report.regions.truncated_headers,
        report.regions.free,
        report.regions.active,
        report.regions.sealed,
        report.regions.record_extent_bytes,
        report.checkpoint.directory_state.as_str(),
        json_optional_u64(report.checkpoint.slot_size),
        json_optional_u64(report.checkpoint.expected_file_len),
        json_optional_u8(report.checkpoint.selected_header_slot),
    )
    .expect("writing JSON into a String cannot fail");
    for (index, slot) in report.checkpoint.slots.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        write!(
            output,
            concat!(
                "{{\"slot\":{},\"state\":\"{}\",\"version\":{},",
                "\"generation\":{},\"superblock_generation\":{},",
                "\"entry_count\":{},\"payload_len\":{},",
                "\"matches_selected_superblock\":{}}}"
            ),
            slot.slot,
            slot.state.as_str(),
            json_optional_u16(slot.version),
            json_optional_u64(slot.generation),
            json_optional_u64(slot.superblock_generation),
            json_optional_u32(slot.entry_count),
            json_optional_u64(slot.payload_len),
            slot.matches_selected_superblock,
        )
        .expect("writing JSON into a String cannot fail");
    }
    output.push_str("]}}");
    output
}

fn verify_json(path: &Path, report: &VerifyReport) -> String {
    let inspect = inspect_json_with_command("verify", path, &report.inspect, report.valid);
    let inspect_object = inspect
        .strip_suffix('}')
        .expect("inspect JSON always ends in an object delimiter");
    let mut output = String::with_capacity(inspect.len() + 2048);
    output.push_str(inspect_object);
    write!(
        output,
        concat!(
            ",\"valid\":{},\"safe_to_open\":{},",
            "\"reopen_disposition\":\"{}\",\"regions_verified\":{},",
            "\"records_verified\":{},\"values_verified\":{},",
            "\"tombstones_verified\":{},\"record_bytes_verified\":{},",
            "\"checkpoint_slots_verified\":{},",
            "\"selected_verified_checkpoint\":{},",
            "\"issues_total\":{},\"issues\":["
        ),
        report.valid,
        report.safe_to_open,
        report.reopen_disposition.as_str(),
        report.regions_verified,
        report.records_verified,
        report.values_verified,
        report.tombstones_verified,
        report.record_bytes_verified,
        report.checkpoint_slots_verified,
        json_optional_u8(report.selected_verified_checkpoint),
        report.issues_total,
    )
    .expect("writing JSON into a String cannot fail");
    for (index, issue) in report.issues.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        write!(
            output,
            concat!(
                "{{\"component\":\"{}\",\"offset\":{},",
                "\"region_id\":{},\"checkpoint_slot\":{},",
                "\"message\":\"{}\"}}"
            ),
            issue.component.as_str(),
            issue.offset,
            json_optional_u32(issue.region_id),
            json_optional_u8(issue.checkpoint_slot),
            json_escape(issue.message),
        )
        .expect("writing JSON into a String cannot fail");
    }
    output.push_str("]}");
    output
}

fn json_optional_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "null",
    }
}

fn json_optional_u8(value: Option<u8>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn json_optional_u16(value: Option<u16>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn json_optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn json_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn json_optional_cache_id(value: Option<[u8; 16]>) -> String {
    let Some(value) = value else {
        return "null".into();
    };
    let mut output = String::with_capacity(34);
    output.push('"');
    for byte in value {
        write!(output, "{byte:02x}").expect("writing cache id into a String cannot fail");
    }
    output.push('"');
    output
}

fn optional_bool(value: Option<bool>) -> &'static str {
    json_optional_bool(value)
}

fn optional_u8(value: Option<u8>) -> String {
    value.map_or_else(|| "n/a".into(), |value| value.to_string())
}

fn optional_u16(value: Option<u16>) -> String {
    value.map_or_else(|| "n/a".into(), |value| value.to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "n/a".into(), |value| value.to_string())
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                write!(escaped, "\\u{:04x}", u32::from(character))
                    .expect("writing JSON escape into a String cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn print_help() {
    println!(
        r#"cachectl — cache-rs diagnostics and offline management

Usage:
  cachectl inspect --path PATH [--output human|json]
  cachectl verify  --path PATH [--output human|json]
  cachectl diagnose --path PATH --capacity BYTES [config options]
  cachectl format --path PATH --capacity BYTES --yes [config options]
  cachectl reset  --path PATH --capacity BYTES --yes [config options]
  cachectl hybrid-inspect --bucket-path PATH --region-path PATH \
    --manifest-path PATH [--output human|json]
  cachectl hybrid-verify --bucket-path PATH --region-path PATH \
    --manifest-path PATH [--output human|json]
  cachectl hybrid-diagnose --bucket-path PATH --bucket-capacity BYTES \
    --region-path PATH --region-capacity BYTES --manifest-path PATH \
    --memory-capacity BYTES [Hybrid config options]
  cachectl hybrid-format --bucket-path PATH --bucket-capacity BYTES \
    --region-path PATH --region-capacity BYTES --manifest-path PATH \
    --memory-capacity BYTES --yes [Hybrid config options]

inspect/verify open an existing regular file read-only, take the same
non-blocking exclusive lock as DiskCache, and refuse a live cache. inspect
reads metadata and Region Headers; verify additionally streams persisted
records and checkpoint payloads.

diagnose validates layout and bounded resource accounting without creating,
opening, locking, or changing PATH. format takes the lock and creates only a
missing or empty path. reset is destructive: under one lock it accepts only a
recognized Format V1 file, durably truncates it, and performs a fresh format.
format/reset require the literal --yes acknowledgement.

Hybrid inspect/verify acquire all three locks in manifest/Bucket/Region order
and never run recovery. hybrid-format requires three distinct missing or empty
regular-file paths; it refuses every non-empty path even when it contains a
recognized cache, so an operator cannot accidentally reset a live warm cache.

Config options:
  --region-size BYTES         Region size (default: 32MiB)
  --index-slots COUNT         Compact-index slots
  --max-key-size BYTES        New-put key limit
  --max-value-size BYTES      New-put value limit
  --hash-seed U64             Decimal or 0x-prefixed persistent hash seed
  --append-lanes 1..2         Persistent append-lane count
  --memory-budget BYTES       Engine logical memory budget
  --read-queue-depth COUNT    Read submission bound
  --write-queue-depth COUNT   Write submission bound
  --io-queue-depth COUNT      Device submission bound
  --output human|json         Output format (default: human)

Hybrid config options:
  --bucket-size BYTES                 4KiB..64KiB power-of-two page
  --bucket-memory-budget BYTES        Bucket logical memory budget
  --bucket-buffer-slots 1..128        Bounded RMW workspaces
  --bucket-engine sync|auto|uring     Bucket I/O engine
  --bucket-mode buffered|auto|direct  Bucket file I/O mode
  --bucket-io-queue-depth COUNT       Bucket device queue bound
  --region-size BYTES                 RegionLog Region size
  --index-slots COUNT                 RegionLog compact-index slots
  --max-key-size BYTES                RegionLog new-put key limit
  --max-value-size BYTES              Includes the Hybrid envelope
  --append-lanes 1..8                 RegionLog append lanes
  --region-memory-budget BYTES        RegionLog logical memory budget
  --region-engine sync|auto|uring     RegionLog I/O engine
  --region-mode buffered|auto|direct  RegionLog file I/O mode
  --region-io-queue-depth COUNT       RegionLog device queue bound
  --memory-shards 1..4096             Power-of-two L1/ordering shards
  --small-object-max BYTES            Bucket routing threshold
  --hybrid-memory-budget BYTES        Aggregate hard logical budget
  --journal-capacity BYTES            4KiB-aligned, 64KiB..4GiB
  --request-slots COUNT               Hybrid synchronous request bound
  --request-memory BYTES              Hybrid request-byte bound
  --async-read-queue-depth COUNT      Hybrid async read bound
  --async-write-queue-depth COUNT     Hybrid async mutation bound
  --async-read-workers COUNT          Hybrid async read concurrency
  --async-mutation-workers COUNT      Hybrid mutation workers
  --write-mode write-through|write-back
                                        L1 write policy (default: write-back)
  --write-back-queue-depth COUNT      Reserved demotion queue bound
  --write-back-workers COUNT          Reserved demotion workers
  --write-back-memory BYTES           Reserved key/value byte bound

Exit codes:
  0  command passed
  2  invalid configuration or corrupt/unsupported cache contents
  3  cache file is locked by a running instance
  4  target or I/O error
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(name: &str) -> Self {
            let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            Self(
                std::env::temp_dir()
                    .join(format!("cachectl-{name}-{}-{nonce}", std::process::id())),
            )
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn parser_requires_an_explicit_read_only_command_and_path() {
        assert!(matches!(
            Options::parse(["inspect".to_owned(), "--path".to_owned(), "x".to_owned()]),
            Ok(ParseOutcome::Run(options))
                if options.command == Command::Inspect
                    && options.path == Path::new("x")
                    && options.output == Output::Human
        ));
        assert!(matches!(
            Options::parse([
                "verify".to_owned(),
                "--path=x".to_owned(),
                "--output=json".to_owned(),
            ]),
            Ok(ParseOutcome::Run(options))
                if options.command == Command::Verify
                    && options.path == Path::new("x")
                    && options.output == Output::Json
        ));
        assert!(Options::parse(["format".to_owned()]).is_err());
        assert!(Options::parse(["verify".to_owned()]).is_err());
    }

    #[test]
    fn parser_requires_confirmation_and_covers_format_configuration() {
        let parsed = Options::parse([
            "format".to_owned(),
            "--path=x".to_owned(),
            "--capacity=64MiB".to_owned(),
            "--region-size=16MiB".to_owned(),
            "--index-slots=1024".to_owned(),
            "--max-key-size=64".to_owned(),
            "--max-value-size=1KiB".to_owned(),
            "--hash-seed=0x1234".to_owned(),
            "--append-lanes=2".to_owned(),
            "--memory-budget=128MiB".to_owned(),
            "--read-queue-depth=4".to_owned(),
            "--write-queue-depth=5".to_owned(),
            "--io-queue-depth=32".to_owned(),
            "--output=json".to_owned(),
            "--yes".to_owned(),
        ])
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable format options");
        };
        assert_eq!(options.command, Command::Format);
        assert_eq!(options.capacity, Some(64 * 1024 * 1024));
        assert_eq!(options.hash_seed, Some(0x1234));
        assert_eq!(options.output, Output::Json);
        assert!(options.confirmed);

        assert!(
            Options::parse([
                "format".to_owned(),
                "--path=x".to_owned(),
                "--capacity=64MiB".to_owned(),
            ])
            .is_err()
        );
        assert!(
            Options::parse([
                "inspect".to_owned(),
                "--path=x".to_owned(),
                "--yes".to_owned(),
            ])
            .is_err()
        );
        assert!(
            Options::parse([
                "verify".to_owned(),
                "--path".to_owned(),
                "--output=json".to_owned(),
            ])
            .is_err()
        );
    }

    #[test]
    fn diagnose_does_not_touch_the_target_path() {
        let path = TestPath::new("diagnose");
        let code = run([
            "diagnose".to_owned(),
            format!("--path={}", path.0.display()),
            "--capacity=57344".to_owned(),
            "--region-size=16KiB".to_owned(),
            "--index-slots=64".to_owned(),
            "--max-key-size=64".to_owned(),
            "--max-value-size=1KiB".to_owned(),
            "--memory-budget=16MiB".to_owned(),
            "--output=json".to_owned(),
        ])
        .unwrap();
        assert_eq!(code, 0);
        assert!(!path.0.exists());
    }

    #[test]
    fn json_output_is_one_validly_delimited_object() {
        let report = InspectReport {
            file_len: 0,
            kind: CacheFileKind::FormatV1,
            selected_superblock: None,
            data_file_len: Some(0),
            superblocks: [
                cache_rs::SuperblockSummary {
                    slot: 0,
                    state: cache_rs::SuperblockState::Missing,
                    version: None,
                    generation: None,
                    clean: None,
                    region_size: None,
                    region_count: None,
                    epoch: None,
                    epoch_start_seqno: None,
                    next_seqno: None,
                    hash_seed: None,
                },
                cache_rs::SuperblockSummary {
                    slot: 1,
                    state: cache_rs::SuperblockState::Missing,
                    version: None,
                    generation: None,
                    clean: None,
                    region_size: None,
                    region_count: None,
                    epoch: None,
                    epoch_start_seqno: None,
                    next_seqno: None,
                    hash_seed: None,
                },
            ],
            regions: cache_rs::RegionSummary::default(),
            checkpoint: cache_rs::CheckpointSummary::default(),
        };
        let encoded = inspect_json(Path::new("a\"b"), &report);
        assert!(encoded.starts_with('{'));
        assert!(encoded.ends_with('}'));
        assert!(encoded.contains("a\\\"b"));
        assert!(encoded.contains("\"passed\":true"));
        assert_eq!(encoded.matches('{').count(), encoded.matches('}').count());

        let verified = VerifyReport {
            inspect: report,
            // Inspect cannot see a payload checksum failure, while verify can.
            valid: false,
            safe_to_open: false,
            reopen_disposition: cache_rs::ReopenDisposition::SafeEmpty,
            regions_verified: 0,
            records_verified: 0,
            values_verified: 0,
            tombstones_verified: 0,
            record_bytes_verified: 0,
            checkpoint_slots_verified: 0,
            selected_verified_checkpoint: None,
            issues_total: 0,
            issues: Vec::new(),
        };
        let encoded = verify_json(Path::new("cache"), &verified);
        assert_eq!(encoded.matches("\"command\"").count(), 1);
        assert!(encoded.contains("\"command\":\"verify\""));
        assert!(!encoded.contains("\"command\":\"inspect\""));
        assert!(encoded.contains("\"passed\":false"));
        assert!(encoded.contains("\"valid\":false"));
    }

    #[test]
    fn hybrid_parser_separates_read_only_and_destructive_surfaces() {
        let inspected = HybridOptions::parse([
            "hybrid-inspect".to_owned(),
            "--bucket-path=bucket".to_owned(),
            "--region-path=region".to_owned(),
            "--manifest-path=manifest".to_owned(),
            "--output=json".to_owned(),
        ])
        .unwrap();
        assert_eq!(inspected.command, HybridCommand::Inspect);
        assert_eq!(inspected.output, Output::Json);

        assert!(
            HybridOptions::parse([
                "hybrid-format".to_owned(),
                "--bucket-path=bucket".to_owned(),
                "--region-path=region".to_owned(),
                "--manifest-path=manifest".to_owned(),
                "--bucket-capacity=16KiB".to_owned(),
                "--region-capacity=1MiB".to_owned(),
                "--memory-capacity=1MiB".to_owned(),
            ])
            .is_err()
        );
        assert!(
            HybridOptions::parse([
                "hybrid-inspect".to_owned(),
                "--bucket-path=same".to_owned(),
                "--region-path=same".to_owned(),
                "--manifest-path=manifest".to_owned(),
            ])
            .is_err()
        );
        assert!(
            HybridOptions::parse([
                "hybrid-verify".to_owned(),
                "--bucket-path=bucket".to_owned(),
                "--region-path=region".to_owned(),
                "--manifest-path=manifest".to_owned(),
                "--write-mode=write-back".to_owned(),
            ])
            .is_err()
        );

        let diagnosed = HybridOptions::parse([
            "hybrid-diagnose".to_owned(),
            "--bucket-path=bucket".to_owned(),
            "--bucket-capacity=16KiB".to_owned(),
            "--region-path=region".to_owned(),
            "--region-capacity=1MiB".to_owned(),
            "--manifest-path=manifest".to_owned(),
            "--memory-capacity=1MiB".to_owned(),
            "--write-mode=write-back".to_owned(),
            "--write-back-queue-depth=2".to_owned(),
            "--write-back-workers=1".to_owned(),
            "--write-back-memory=1MiB".to_owned(),
        ])
        .unwrap();
        assert_eq!(diagnosed.write_mode, HybridWriteMode::WriteBack);
        assert_eq!(diagnosed.write_back_queue_depth, 2);
    }

    #[test]
    fn hybrid_diagnose_does_not_create_any_target_file() {
        let bucket = TestPath::new("hybrid-diagnose-bucket");
        let region = TestPath::new("hybrid-diagnose-region");
        let manifest = TestPath::new("hybrid-diagnose-manifest");
        let code = run([
            "hybrid-diagnose".to_owned(),
            format!("--bucket-path={}", bucket.0.display()),
            "--bucket-capacity=16KiB".to_owned(),
            format!("--region-path={}", region.0.display()),
            "--region-capacity=640KiB".to_owned(),
            format!("--manifest-path={}", manifest.0.display()),
            "--memory-capacity=16KiB".to_owned(),
            "--memory-shards=4".to_owned(),
            "--region-size=128KiB".to_owned(),
            "--index-slots=128".to_owned(),
            "--max-value-size=4KiB".to_owned(),
            "--bucket-buffer-slots=1".to_owned(),
            "--bucket-memory-budget=1MiB".to_owned(),
            "--region-memory-budget=16MiB".to_owned(),
            "--journal-capacity=64KiB".to_owned(),
            "--output=json".to_owned(),
        ])
        .unwrap();
        assert_eq!(code, 0);
        assert!(!bucket.0.exists());
        assert!(!region.0.exists());
        assert!(!manifest.0.exists());
    }
}
