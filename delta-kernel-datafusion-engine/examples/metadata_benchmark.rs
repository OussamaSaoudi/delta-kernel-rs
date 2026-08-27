//! One-shot metadata benchmark comparing Kernel iterator and declarative-plan execution paths.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, fmt, thread};

use datafusion::physical_plan::displayable;
use delta_kernel::engine::default::storage::store_from_url_opts;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata, Scan, ScanMetadata};
use delta_kernel::{DeltaResult, Engine, Error, Expression, Predicate, PredicateRef, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use futures::TryStreamExt;
use url::Url;

const DEFAULT_REGION: &str = "us-west-2";
const CSV_HEADER: &str = concat!(
    "table,method,predicate,cycle,version,snapshot_ms,parallelism,run,elapsed_ms,live_files,",
    "files_per_second,peak_rss_mib"
);

const TABLES: &[TableSpec] = &[
    TableSpec {
        name: "large_log_no_checkpoint",
        path: concat!(
            "s3://benchmarking-scratch-us-west-2/quicksilver/",
            "large-number-files-delta-2mil"
        ),
        expected_files: 2_000_000,
    },
    TableSpec {
        name: "large_log_checkpoint",
        path: concat!(
            "s3://benchmarking-scratch-us-west-2/quicksilver/",
            "large-number-files-delta-5mil-clone-checkpointed"
        ),
        expected_files: 5_000_000,
    },
    TableSpec {
        name: "large_log_dvs",
        path: concat!(
            "s3://benchmarking-scratch-us-west-2/quicksilver/",
            "large-number-files-delta-5mil-clone-checkpointed-with-dvs"
        ),
        expected_files: 5_000_000,
    },
];

#[derive(Clone, Copy)]
struct TableSpec {
    name: &'static str,
    path: &'static str,
    expected_files: usize,
}

#[derive(Clone)]
struct OwnedTableSpec {
    name: String,
    path: String,
    expected_files: Option<usize>,
}

#[derive(Clone, Copy)]
enum Method {
    Kernel,
    KernelParallel,
    DataFusion,
}

impl Method {
    fn as_str(self) -> &'static str {
        match self {
            Self::Kernel => "kernel",
            Self::KernelParallel => "kernel-parallel",
            Self::DataFusion => "datafusion",
        }
    }

    fn parallelism(self, kernel_parallel_workers: usize) -> usize {
        match self {
            Self::Kernel => 1,
            Self::KernelParallel => kernel_parallel_workers,
            Self::DataFusion => thread::available_parallelism().map_or(1, usize::from),
        }
    }
}

impl FromStr for Method {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "kernel" => Ok(Self::Kernel),
            "kernel-parallel" => Ok(Self::KernelParallel),
            "datafusion" => Ok(Self::DataFusion),
            _ => Err(format!(
                "unknown method `{value}`; expected kernel, kernel-parallel, or datafusion"
            )),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PredicateCase {
    None,
    KeepAll,
    KeepSome,
    KeepFew,
    Conjunctive,
    SkipAll,
}

impl PredicateCase {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::KeepAll => "keep-all",
            Self::KeepSome => "keep-some",
            Self::KeepFew => "keep-few",
            Self::Conjunctive => "conjunctive",
            Self::SkipAll => "skip-all",
        }
    }

    fn predicate(self) -> Option<PredicateRef> {
        let header_time = || Expression::column(["upstream_header_time"]);
        let q_time = || Expression::column(["upstream_q_time"]);
        let greater_than =
            |column: Expression, value: f64| Predicate::gt(column, Expression::literal(value));
        let predicate = match self {
            Self::None => return None,
            Self::KeepAll => Predicate::ge(header_time(), Expression::literal(0.0_f64)),
            Self::KeepSome => greater_than(header_time(), 0.99),
            Self::KeepFew => greater_than(header_time(), 0.999),
            Self::Conjunctive => Predicate::and(
                greater_than(header_time(), 0.99),
                greater_than(q_time(), 0.99),
            ),
            Self::SkipAll => greater_than(header_time(), 2.0),
        };
        Some(Arc::new(predicate))
    }
}

impl FromStr for PredicateCase {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "keep-all" => Ok(Self::KeepAll),
            "keep-some" => Ok(Self::KeepSome),
            "keep-few" => Ok(Self::KeepFew),
            "conjunctive" => Ok(Self::Conjunctive),
            "skip-all" => Ok(Self::SkipAll),
            _ => Err(format!(
                "unknown predicate `{value}`; expected none, keep-all, keep-some, keep-few, \
                 conjunctive, or skip-all"
            )),
        }
    }
}

struct Config {
    table: OwnedTableSpec,
    method: Method,
    predicate: PredicateCase,
    cycle: usize,
    runs: usize,
    warmups: usize,
    workers: usize,
    region: String,
    explain_plan: bool,
    print_header: bool,
}

struct Measurement {
    run: usize,
    elapsed: Duration,
    live_files: usize,
    peak_rss_mib: Option<f64>,
}

impl fmt::Display for Measurement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{},{:.3},{},{:.0},{}",
            self.run,
            self.elapsed.as_secs_f64() * 1_000.0,
            self.live_files,
            self.live_files as f64 / self.elapsed.as_secs_f64(),
            self.peak_rss_mib
                .map(|rss| format!("{rss:.1}"))
                .unwrap_or_default()
        )
    }
}

fn usage() -> &'static str {
    "Usage: metadata_benchmark --method METHOD [OPTIONS]\n\
     \n\
     METHOD: kernel | kernel-parallel | datafusion\n\
     \n\
     Options:\n\
       --table NAME       Preset table name (default: large_log_checkpoint)\n\
       --path URL         Override the preset with any Delta table URL\n\
       --name NAME        Name used with --path (default: custom)\n\
       --expected N       Expected live-file count for validation\n\
       --predicate NAME   Data-skipping predicate case (default: none)\n\
       --cycle N          Outer ABC cycle recorded in CSV output (default: 1)\n\
       --runs N           Measured scans in this process (default: 1)\n\
       --warmups N        Unmeasured scans before recording (default: 0)\n\
       --workers N        Kernel parallel-phase workers (default: available CPUs)\n\
       --region REGION    S3 region (default: us-west-2)\n\
       --explain-plan     Print DataFusion logical and physical plans before execution\n\
       --header           Print the CSV header\n\
       --list-tables      Print preset names and paths\n\
       -h, --help         Print this help"
}

fn next_value(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value"))
}

fn parse_usize(value: String, option: &str, allow_zero: bool) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {option} value `{value}`: {error}"))?;
    if !allow_zero && parsed == 0 {
        return Err(format!("{option} must be greater than zero"));
    }
    Ok(parsed)
}

fn preset(name: &str) -> Result<OwnedTableSpec, String> {
    TABLES
        .iter()
        .find(|table| table.name == name)
        .map(|table| OwnedTableSpec {
            name: table.name.to_string(),
            path: table.path.to_string(),
            expected_files: Some(table.expected_files),
        })
        .ok_or_else(|| format!("unknown table `{name}`; use --list-tables"))
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut method = None;
    let mut predicate = PredicateCase::None;
    let mut table_name = "large_log_checkpoint".to_string();
    let mut path = None;
    let mut custom_name = "custom".to_string();
    let mut expected = None;
    let mut cycle = 1;
    let mut runs = 1;
    let mut warmups = 0;
    let mut workers = thread::available_parallelism().map_or(1, usize::from);
    let mut region = DEFAULT_REGION.to_string();
    let mut explain_plan = false;
    let mut print_header = false;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--method" => method = Some(next_value(&mut args, "--method")?.parse::<Method>()?),
            "--table" => table_name = next_value(&mut args, "--table")?,
            "--path" => path = Some(next_value(&mut args, "--path")?),
            "--name" => custom_name = next_value(&mut args, "--name")?,
            "--expected" => {
                expected = Some(parse_usize(
                    next_value(&mut args, "--expected")?,
                    "--expected",
                    true,
                )?)
            }
            "--predicate" => {
                predicate = next_value(&mut args, "--predicate")?.parse::<PredicateCase>()?
            }
            "--cycle" => cycle = parse_usize(next_value(&mut args, "--cycle")?, "--cycle", false)?,
            "--runs" => runs = parse_usize(next_value(&mut args, "--runs")?, "--runs", false)?,
            "--warmups" => {
                warmups = parse_usize(next_value(&mut args, "--warmups")?, "--warmups", true)?
            }
            "--workers" => {
                workers = parse_usize(next_value(&mut args, "--workers")?, "--workers", false)?
            }
            "--region" => region = next_value(&mut args, "--region")?,
            "--explain-plan" => explain_plan = true,
            "--header" => print_header = true,
            "--list-tables" => {
                for table in TABLES {
                    println!("{}\t{}", table.name, table.path);
                }
                return Ok(None);
            }
            "-h" | "--help" => {
                println!("{}", usage());
                return Ok(None);
            }
            _ => return Err(format!("unknown option `{arg}`\n\n{}", usage())),
        }
    }

    let table = if let Some(path) = path {
        OwnedTableSpec {
            name: custom_name,
            path,
            expected_files: expected,
        }
    } else {
        let mut table = preset(&table_name)?;
        if expected.is_some() {
            table.expected_files = expected;
        }
        table
    };
    let mut table = table;
    if predicate != PredicateCase::None && expected.is_none() {
        table.expected_files = None;
    }
    let method = method.ok_or_else(|| format!("--method is required\n\n{}", usage()))?;
    Ok(Some(Config {
        table,
        method,
        predicate,
        cycle,
        runs,
        warmups,
        workers,
        region,
        explain_plan,
        print_header,
    }))
}

fn selected_file_count(metadata: ScanMetadata) -> usize {
    metadata
        .scan_files
        .selection_vector()
        .iter()
        .filter(|selected| **selected)
        .count()
}

fn kernel_scan(scan: &Scan, engine: &dyn Engine) -> DeltaResult<usize> {
    scan.scan_metadata(engine)?
        .try_fold(0usize, |count, metadata| {
            Ok(count + selected_file_count(metadata?))
        })
}

fn parallel_leaf_scan(
    engine: Arc<dyn Engine>,
    state: Arc<delta_kernel::scan::ParallelState>,
    files: Vec<delta_kernel::FileMeta>,
    workers: usize,
) -> DeltaResult<usize> {
    let worker_count = workers.min(files.len()).max(1);
    let mut shards = (0..worker_count)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<delta_kernel::FileMeta>>>();
    for (index, file) in files.into_iter().enumerate() {
        shards[index % worker_count].push(file);
    }

    let handles = shards
        .into_iter()
        .map(|files| {
            let engine = Arc::clone(&engine);
            let state = Arc::clone(&state);
            thread::spawn(move || -> DeltaResult<usize> {
                ParallelScanMetadata::try_new(engine, state, files)?
                    .try_fold(0usize, |count, metadata| {
                        Ok(count + selected_file_count(metadata?))
                    })
            })
        })
        .collect::<Vec<_>>();

    let mut count = 0;
    for handle in handles {
        count += handle
            .join()
            .map_err(|_| Error::generic("kernel parallel metadata worker panicked"))??;
    }
    state.log_metrics();
    Ok(count)
}

fn kernel_parallel_scan(
    scan: &Scan,
    engine: Arc<dyn Engine>,
    workers: usize,
) -> DeltaResult<usize> {
    let mut sequential = scan.parallel_scan_metadata(Arc::clone(&engine))?;
    let mut count = sequential.try_fold(0usize, |count, metadata| {
        Ok::<usize, Error>(count + selected_file_count(metadata?))
    })?;
    match sequential.finish()? {
        AfterSequentialScanMetadata::Done => Ok(count),
        AfterSequentialScanMetadata::Parallel { state, files } => {
            count += parallel_leaf_scan(engine, Arc::new(*state), files, workers)?;
            Ok(count)
        }
    }
}

async fn datafusion_scan(scan: &Scan, executor: &DataFusionExecutor) -> DeltaResult<usize> {
    let dataframe = executor
        .scan_metadata(scan)
        .await
        .map_err(|error| Error::generic(error.to_string()))?;
    let mut batches = dataframe
        .execute_stream()
        .await
        .map_err(|error| Error::generic(error.to_string()))?;
    let mut count = 0;
    while let Some(batch) = batches
        .try_next()
        .await
        .map_err(|error| Error::generic(error.to_string()))?
    {
        count += batch.num_rows();
    }
    Ok(count)
}

async fn explain_datafusion_plan(
    scan: &Scan,
    executor: &DataFusionExecutor,
) -> Result<(), Box<dyn std::error::Error>> {
    let dataframe = executor.scan_metadata(scan).await?;
    eprintln!(
        "Logical plan:\n{}",
        dataframe.logical_plan().display_indent()
    );
    let physical = dataframe.create_physical_plan().await?;
    eprintln!(
        "Physical plan:\n{}",
        displayable(physical.as_ref()).indent(true)
    );
    Ok(())
}

async fn run_method(
    method: Method,
    snapshot: &Arc<Snapshot>,
    engine: Arc<dyn Engine>,
    executor: &DataFusionExecutor,
    workers: usize,
    predicate: Option<PredicateRef>,
) -> DeltaResult<usize> {
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;
    match method {
        Method::Kernel => kernel_scan(&scan, engine.as_ref()),
        Method::KernelParallel => kernel_parallel_scan(&scan, engine, workers),
        Method::DataFusion => datafusion_scan(&scan, executor).await,
    }
}

fn peak_rss_mib() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<f64>()
        .ok()?;
    Some(kib / 1024.0)
}

fn storage_options(region: &str) -> Vec<(String, String)> {
    let mut options = vec![("region".to_string(), region.to_string())];
    for (environment_name, option_name) in [
        ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        ("AWS_SESSION_TOKEN", "aws_session_token"),
    ] {
        if let Ok(value) = env::var(environment_name) {
            options.push((option_name.to_string(), value));
        }
    }
    options
}

fn validate_count(table: &OwnedTableSpec, actual: usize) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(expected) = table.expected_files {
        if actual != expected {
            return Err(format!(
                "{} returned {actual} live files; expected {expected}",
                table.name
            )
            .into());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = parse_args()? else {
        return Ok(());
    };
    let table_url = Url::parse(&config.table.path)?;
    let store = store_from_url_opts(&table_url, storage_options(&config.region))?;
    let (datafusion_store, _) =
        datafusion::object_store::parse_url_opts(&table_url, storage_options(&config.region))?;
    let engine: Arc<dyn Engine> = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot_start = Instant::now();
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let snapshot_elapsed = snapshot_start.elapsed();
    let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))?;
    executor.register_object_store(snapshot.table_root(), Arc::from(datafusion_store));
    let parallelism = config.method.parallelism(config.workers);

    eprintln!(
        "table={} method={} predicate={} cycle={} version={} snapshot_ms={:.3} parallelism={} warmups={} runs={}",
        config.table.name,
        config.method.as_str(),
        config.predicate.as_str(),
        config.cycle,
        snapshot.version(),
        snapshot_elapsed.as_secs_f64() * 1_000.0,
        parallelism,
        config.warmups,
        config.runs
    );

    if config.explain_plan {
        if !matches!(config.method, Method::DataFusion) {
            return Err("--explain-plan requires --method datafusion".into());
        }
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_predicate(config.predicate.predicate())
            .build()?;
        explain_datafusion_plan(&scan, &executor).await?;
    }

    for _ in 0..config.warmups {
        let count = run_method(
            config.method,
            &snapshot,
            Arc::clone(&engine),
            &executor,
            config.workers,
            config.predicate.predicate(),
        )
        .await?;
        validate_count(&config.table, count)?;
    }

    if config.print_header {
        println!("{CSV_HEADER}");
    }
    for run in 1..=config.runs {
        let start = Instant::now();
        let live_files = run_method(
            config.method,
            &snapshot,
            Arc::clone(&engine),
            &executor,
            config.workers,
            config.predicate.predicate(),
        )
        .await?;
        let elapsed = start.elapsed();
        validate_count(&config.table, live_files)?;
        let measurement = Measurement {
            run,
            elapsed,
            live_files,
            peak_rss_mib: peak_rss_mib(),
        };
        println!(
            "{},{},{},{},{},{:.3},{},{}",
            config.table.name,
            config.method.as_str(),
            config.predicate.as_str(),
            config.cycle,
            snapshot.version(),
            snapshot_elapsed.as_secs_f64() * 1_000.0,
            parallelism,
            measurement
        );
    }
    Ok(())
}
