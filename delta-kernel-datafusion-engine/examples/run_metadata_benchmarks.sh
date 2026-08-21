#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

cycles="${CYCLES:-3}"
runs="${RUNS:-1}"
warmups="${WARMUPS:-0}"
workers="${WORKERS:-$(getconf _NPROCESSORS_ONLN)}"
region="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
version="${VERSION:-}"
output="${OUTPUT:-metadata-benchmark-results.csv}"

version_args=()
if [[ -n "$version" ]]; then
  version_args=(--version "$version")
fi

cargo build --locked --release \
  -p delta-kernel-datafusion-engine \
  --example metadata_benchmark

binary="target/release/examples/metadata_benchmark"
read -r -a tables <<< "${TABLES:-large_log_no_checkpoint large_log_checkpoint large_log_dvs}"
methods=(kernel kernel-parallel datafusion)
read -r -a predicates <<< "${PREDICATES:-none keep-all keep-some conjunctive skip-all}"

printf '%s%s\n' \
  'table,method,predicate,cycle,version,snapshot_ms,parallelism,run,elapsed_ms,live_files,' \
  'files_per_second,peak_rss_mib' \
  > "$output"

for table in "${tables[@]}"; do
  for predicate in "${predicates[@]}"; do
    for ((cycle = 1; cycle <= cycles; cycle++)); do
      for method in "${methods[@]}"; do
        "$binary" \
          --table "$table" \
          --method "$method" \
          --predicate "$predicate" \
          --cycle "$cycle" \
          --runs "$runs" \
          --warmups "$warmups" \
          --workers "$workers" \
          --region "$region" \
          "${version_args[@]}" \
          | tee -a "$output"
      done
    done
  done
done

echo "Results written to $output" >&2
