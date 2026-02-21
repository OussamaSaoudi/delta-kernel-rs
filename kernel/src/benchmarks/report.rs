//! Benchmark report generation for producing `benchmark_report.json`.
//!
//! This module defines a unified report schema compatible with the Java kernel's
//! `WorkloadOutputFormat`, enabling cross-language comparison of benchmark results.
//!
//! Both JMH (Java) and Criterion (Rust) benchmarks produce identical JSON reports,
//! allowing Quicksilver to consume them through a single `JmhBenchmarkReport` parser.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::Serialize;

/// Top-level benchmark report matching the Java `WorkloadOutputFormat` schema.
#[derive(Serialize)]
pub struct BenchmarkReport {
    pub report_metadata: ReportMetadata,
    pub execution_environment: ExecutionEnvironment,
    pub benchmark_configuration: HashMap<String, String>,
    pub benchmarks: HashMap<String, BenchmarkDetails>,
}

/// Details for a single benchmark within the report.
#[derive(Serialize)]
pub struct BenchmarkDetails {
    pub spec: serde_json::Value,
    pub additional_params: HashMap<String, String>,
    pub time: TimingMetric,
    pub secondary_metrics: HashMap<String, serde_json::Value>,
}

/// Statistical timing metrics for a benchmark.
#[derive(Serialize)]
pub struct TimingMetric {
    /// Mean duration in ms/op.
    pub score: f64,
    /// Unit of the score (always "ms/op").
    pub score_unit: String,
    /// 95% confidence interval half-width.
    pub score_error: f64,
    /// [lower, upper] bounds of the 95% confidence interval.
    pub score_confidence: [f64; 2],
    /// Number of measured samples.
    pub sample_count: u64,
    /// Percentile values: p0.50, p0.90, p0.95, p0.99, p1.00.
    pub percentiles: HashMap<String, f64>,
}

/// Information about the execution environment.
#[derive(Serialize)]
pub struct ExecutionEnvironment {
    pub cpu_arch: String,
    pub cpu_cores: u64,
    pub os_name: String,
    pub os_version: String,
    pub max_memory_mb: u64,
    pub rustc_version: String,
    pub target_triple: String,
}

/// Report-level metadata.
#[derive(Serialize)]
pub struct ReportMetadata {
    pub generated_at: String,
    pub framework_version: String,
    pub report_version: String,
    pub benchmark_suite: String,
}

/// Collects per-invocation timing data for a single benchmark.
pub struct BenchmarkTimings {
    pub name: String,
    pub spec: serde_json::Value,
    pub durations: Vec<Duration>,
}

impl BenchmarkTimings {
    pub fn new(name: String, spec: serde_json::Value) -> Self {
        Self {
            name,
            spec,
            durations: Vec::new(),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        self.durations.push(duration);
    }
}

/// Compute percentiles from a slice of sorted values using linear interpolation.
///
/// Returns a map with keys: p0.50, p0.90, p0.95, p0.99, p1.00.
pub fn compute_percentiles(values: &[f64]) -> HashMap<String, f64> {
    let mut result = HashMap::new();
    if values.is_empty() {
        return result;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let percentiles = [
        ("p0.50", 0.50),
        ("p0.90", 0.90),
        ("p0.95", 0.95),
        ("p0.99", 0.99),
        ("p1.00", 1.00),
    ];

    let n = sorted.len();
    for (key, p) in &percentiles {
        let value = if n == 1 {
            sorted[0]
        } else {
            let rank = p * (n - 1) as f64;
            let lower = rank.floor() as usize;
            let upper = rank.ceil() as usize;
            let frac = rank - lower as f64;
            if lower == upper {
                sorted[lower]
            } else {
                sorted[lower] * (1.0 - frac) + sorted[upper] * frac
            }
        };
        result.insert(key.to_string(), value);
    }

    result
}

/// Compute 95% confidence interval for the mean using the t-distribution approximation.
///
/// Returns (score_error, [lower, upper]) where score_error is the half-width.
pub fn compute_confidence_interval(values: &[f64]) -> (f64, [f64; 2]) {
    let n = values.len();
    if n < 2 {
        let mean = values.first().copied().unwrap_or(0.0);
        return (0.0, [mean, mean]);
    }

    let mean: f64 = values.iter().sum::<f64>() / n as f64;
    let variance: f64 =
        values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    let std_err = variance.sqrt() / (n as f64).sqrt();

    // t-value for 95% CI approximation (using 1.96 for large n, adjusting for small n)
    let t_value = if n >= 30 {
        1.96
    } else {
        // Simple approximation: use common t-values for small sample sizes
        match n {
            2 => 12.706,
            3 => 4.303,
            4 => 3.182,
            5 => 2.776,
            6 => 2.571,
            7 => 2.447,
            8 => 2.365,
            9 => 2.306,
            10 => 2.262,
            11..=15 => 2.145,
            16..=20 => 2.086,
            21..=29 => 2.042,
            _ => 1.96,
        }
    };

    let error = t_value * std_err;
    (error, [mean - error, mean + error])
}

/// Detect the current execution environment.
pub fn detect_execution_environment() -> ExecutionEnvironment {
    ExecutionEnvironment {
        cpu_arch: std::env::consts::ARCH.to_string(),
        cpu_cores: std::thread::available_parallelism()
            .map(|p| p.get() as u64)
            .unwrap_or(1),
        os_name: std::env::consts::OS.to_string(),
        os_version: os_version(),
        max_memory_mb: max_memory_mb(),
        rustc_version: env!("CARGO_PKG_RUST_VERSION").to_string(),
        target_triple: target_triple(),
    }
}

fn os_version() -> String {
    // Try to read /etc/os-release on Linux
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/etc/os-release") {
            for line in content.lines() {
                if let Some(version) = line.strip_prefix("PRETTY_NAME=") {
                    return version.trim_matches('"').to_string();
                }
            }
        }
    }
    "unknown".to_string()
}

fn max_memory_mb() -> u64 {
    // Try to read /proc/meminfo on Linux
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    let kb_str = rest.trim().trim_end_matches("kB").trim();
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        return kb / 1024;
                    }
                }
            }
        }
    }
    0
}

fn target_triple() -> String {
    // Use compile-time target
    env!("TARGET").to_string()
}

/// Build a complete BenchmarkReport from collected timings.
pub fn build_report(
    suite_name: &str,
    timings: Vec<BenchmarkTimings>,
) -> BenchmarkReport {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_default();

    let mut benchmarks = HashMap::new();

    for timing in timings {
        let durations_ms: Vec<f64> = timing
            .durations
            .iter()
            .map(|d| d.as_secs_f64() * 1000.0)
            .collect();

        let sample_count = durations_ms.len() as u64;
        let mean = if durations_ms.is_empty() {
            0.0
        } else {
            durations_ms.iter().sum::<f64>() / durations_ms.len() as f64
        };

        let percentiles = compute_percentiles(&durations_ms);
        let (score_error, score_confidence) = compute_confidence_interval(&durations_ms);

        benchmarks.insert(
            timing.name,
            BenchmarkDetails {
                spec: timing.spec,
                additional_params: {
                    let mut m = HashMap::new();
                    m.insert("engine".to_string(), "default".to_string());
                    m
                },
                time: TimingMetric {
                    score: mean,
                    score_unit: "ms/op".to_string(),
                    score_error,
                    score_confidence,
                    sample_count,
                    percentiles,
                },
                secondary_metrics: HashMap::new(),
            },
        );
    }

    BenchmarkReport {
        report_metadata: ReportMetadata {
            generated_at: now,
            framework_version: "Criterion 0.5".to_string(),
            report_version: "1.0".to_string(),
            benchmark_suite: suite_name.to_string(),
        },
        execution_environment: detect_execution_environment(),
        benchmark_configuration: HashMap::new(),
        benchmarks,
    }
}

/// Write a BenchmarkReport to a JSON file.
pub fn write_report(report: &BenchmarkReport, path: &Path) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(path, json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_percentiles_basic() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let p = compute_percentiles(&values);
        assert!((p["p0.50"] - 50.5).abs() < 0.01);
        assert!((p["p1.00"] - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_compute_percentiles_single() {
        let values = vec![42.0];
        let p = compute_percentiles(&values);
        assert_eq!(p["p0.50"], 42.0);
        assert_eq!(p["p1.00"], 42.0);
    }

    #[test]
    fn test_compute_percentiles_empty() {
        let values: Vec<f64> = vec![];
        let p = compute_percentiles(&values);
        assert!(p.is_empty());
    }

    #[test]
    fn test_confidence_interval_single_sample() {
        let values = vec![100.0];
        let (error, ci) = compute_confidence_interval(&values);
        assert_eq!(error, 0.0);
        assert_eq!(ci, [100.0, 100.0]);
    }

    #[test]
    fn test_confidence_interval_identical_values() {
        let values = vec![50.0, 50.0, 50.0, 50.0, 50.0];
        let (error, ci) = compute_confidence_interval(&values);
        assert_eq!(error, 0.0);
        assert_eq!(ci, [50.0, 50.0]);
    }
}
