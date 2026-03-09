#!/usr/bin/env python3
"""Generate additional spec JSON files for existing Delta test tables.

Strategies:
A) Predicate specs for tables with suitable columns
B) Column projection specs for multi-column tables
C) Time-travel (version) specs for multi-version tables
D) Combined predicate + projection specs
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional

IMPROVED_DAT = Path(os.path.expanduser("~/delta-kernel-rs/improved_dat"))
WRITE_WORKLOADS = IMPROVED_DAT / "write_workloads"

# Predicates by data type
TYPE_PREDICATES = {
    "integer": [
        lambda col: f"{col} > 0",
        lambda col: f"{col} IS NOT NULL",
        lambda col: f"{col} = 1",
    ],
    "long": [
        lambda col: f"{col} > 0",
        lambda col: f"{col} IS NOT NULL",
    ],
    "short": [
        lambda col: f"{col} > 0",
        lambda col: f"{col} IS NOT NULL",
    ],
    "byte": [
        lambda col: f"{col} > 0",
        lambda col: f"{col} IS NOT NULL",
    ],
    "string": [
        lambda col: f"{col} IS NOT NULL",
        lambda col: f"{col} = 'a'",
    ],
    "boolean": [
        lambda col: f"{col} = true",
        lambda col: f"{col} IS NOT NULL",
    ],
    "double": [
        lambda col: f"{col} IS NOT NULL",
        lambda col: f"{col} > 0.0",
    ],
    "float": [
        lambda col: f"{col} IS NOT NULL",
        lambda col: f"{col} > 0.0",
    ],
    "date": [
        lambda col: f"{col} IS NOT NULL",
    ],
    "timestamp": [
        lambda col: f"{col} IS NOT NULL",
    ],
    "timestamp_ntz": [
        lambda col: f"{col} IS NOT NULL",
    ],
}

# Types that are "simple" and safe to project
SIMPLE_TYPES = {
    "integer", "long", "short", "byte", "string", "boolean",
    "double", "float", "date", "timestamp", "timestamp_ntz", "binary",
}

# Tables/prefixes to skip (error tables, corrupt tables, etc.)
SKIP_PREFIXES = {
    "err_", "corrupt_", "log_err_", "ct_corrupt", "ct_missing",
    "ct_invalid", "ct_duplicate", "ct_unknown", "ct_zero",
    "ct_gap", "ct_only_remove",
}


def get_field_type(field: dict) -> str:
    """Extract the type string from a schema field."""
    t = field.get("type", "")
    if isinstance(t, dict):
        # Complex type (struct, array, map)
        return t.get("type", "unknown")
    if isinstance(t, str):
        # Check for decimal
        if t.startswith("decimal"):
            return "decimal"
        return t
    return "unknown"


def is_simple_type(field: dict) -> bool:
    """Check if a field has a simple (non-nested) type."""
    return get_field_type(field) in SIMPLE_TYPES


def should_skip(name: str) -> bool:
    """Check if a table should be skipped (error/corrupt tables)."""
    name_lower = name.lower()
    for prefix in SKIP_PREFIXES:
        if name_lower.startswith(prefix):
            return True
    # Also skip if name contains "err" as a word
    if "_err_" in name_lower or name_lower.startswith("err_"):
        return True
    return False


def get_predicate_safe_name(col_name: str) -> str:
    """Convert a column name to a safe identifier for spec file names."""
    return col_name.replace(".", "_").replace(" ", "_").replace("`", "")


def load_table_info(table_dir: Path) -> Optional[dict]:
    """Load table_info.json from a table directory."""
    ti_path = table_dir / "table_info.json"
    if not ti_path.exists():
        return None
    try:
        with open(ti_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def existing_spec_names(table_dir: Path) -> set:
    """Get set of existing spec file basenames (without .json)."""
    specs_dir = table_dir / "specs"
    if not specs_dir.exists():
        return set()
    return {p.stem for p in specs_dir.glob("*.json")}


def write_spec(table_dir: Path, spec_name: str, spec: dict) -> bool:
    """Write a spec JSON file. Returns True if written (new), False if exists."""
    specs_dir = table_dir / "specs"
    specs_dir.mkdir(exist_ok=True)
    spec_path = specs_dir / f"{spec_name}.json"
    if spec_path.exists():
        return False
    with open(spec_path, "w") as f:
        json.dump(spec, f, indent=2)
    return True


def generate_predicate_specs(table_dir: Path, table_info: dict, existing: set) -> int:
    """Generate predicate filter specs for a table. Returns count of new specs."""
    name = table_info.get("name", table_dir.name)
    schema = table_info.get("schema", {})
    fields = schema.get("fields", [])

    if not fields:
        return 0

    count = 0
    for field in fields:
        col_name = field.get("name", "")
        if not col_name:
            continue

        col_type = get_field_type(field)
        predicates = TYPE_PREDICATES.get(col_type, [])

        for i, pred_fn in enumerate(predicates):
            predicate = pred_fn(col_name)
            safe_col = get_predicate_safe_name(col_name)
            spec_name = f"{name}_filter_{safe_col}_{i}"

            if spec_name in existing:
                continue

            spec = {"type": "read", "predicate": predicate}
            if write_spec(table_dir, spec_name, spec):
                count += 1
                existing.add(spec_name)

            # Only generate max 2 predicate specs per table to avoid bloat
            if count >= 2:
                return count

    return count


def generate_projection_specs(table_dir: Path, table_info: dict, existing: set) -> int:
    """Generate column projection specs for multi-column tables."""
    name = table_info.get("name", table_dir.name)
    schema = table_info.get("schema", {})
    fields = schema.get("fields", [])

    # Need at least 2 columns to make projection interesting
    if len(fields) < 2:
        return 0

    simple_fields = [f for f in fields if is_simple_type(f)]
    if not simple_fields:
        return 0

    count = 0

    # Single column projection
    col = simple_fields[0]["name"]
    spec_name = f"{name}_project_{get_predicate_safe_name(col)}"
    if spec_name not in existing:
        spec = {"type": "read", "columns": [col]}
        if write_spec(table_dir, spec_name, spec):
            count += 1
            existing.add(spec_name)

    # Two-column projection if we have enough columns
    if len(simple_fields) >= 3:
        cols = [simple_fields[0]["name"], simple_fields[-1]["name"]]
        safe_names = "_".join(get_predicate_safe_name(c) for c in cols)
        spec_name = f"{name}_project_{safe_names}"
        if spec_name not in existing:
            spec = {"type": "read", "columns": cols}
            if write_spec(table_dir, spec_name, spec):
                count += 1
                existing.add(spec_name)

    return count


def generate_version_specs(table_dir: Path, table_info: dict, existing: set) -> int:
    """Generate time-travel specs at earlier versions for multi-version tables."""
    name = table_info.get("name", table_dir.name)
    log_info = table_info.get("log_info", {})
    num_commits = log_info.get("num_commits", 1)

    if num_commits <= 1:
        return 0

    count = 0
    # Generate read and snapshot at version 0
    for v in range(min(num_commits - 1, 3)):  # At most versions 0, 1, 2
        # Read at version v
        spec_name = f"{name}_read_v{v}"
        if spec_name not in existing:
            spec = {"type": "read", "version": v}
            if write_spec(table_dir, spec_name, spec):
                count += 1
                existing.add(spec_name)

        # Snapshot at version v
        spec_name = f"{name}_snapshot_v{v}"
        if spec_name not in existing:
            spec = {"type": "snapshot", "version": v}
            if write_spec(table_dir, spec_name, spec):
                count += 1
                existing.add(spec_name)

    return count


def generate_combined_specs(table_dir: Path, table_info: dict, existing: set) -> int:
    """Generate combined predicate + projection specs."""
    name = table_info.get("name", table_dir.name)
    schema = table_info.get("schema", {})
    fields = schema.get("fields", [])

    if len(fields) < 3:
        return 0

    simple_fields = [f for f in fields if is_simple_type(f)]
    if len(simple_fields) < 2:
        return 0

    count = 0

    # Find a field with predicates
    pred_field = None
    pred_str = None
    for f in simple_fields:
        col_type = get_field_type(f)
        preds = TYPE_PREDICATES.get(col_type, [])
        if preds:
            pred_field = f
            pred_str = preds[0](f["name"])
            break

    if not pred_field or not pred_str:
        return 0

    # Project a different column
    other_cols = [f["name"] for f in simple_fields if f["name"] != pred_field["name"]]
    if not other_cols:
        return 0

    cols = other_cols[:2]  # Project up to 2 other columns
    safe_names = "_".join(get_predicate_safe_name(c) for c in cols)
    spec_name = f"{name}_combined_{safe_names}"
    if spec_name not in existing:
        spec = {"type": "read", "predicate": pred_str, "columns": cols}
        if write_spec(table_dir, spec_name, spec):
            count += 1
            existing.add(spec_name)

    return count


def process_table_dir(table_dir: Path) -> dict:
    """Process a single table directory, generating all additional specs."""
    table_info = load_table_info(table_dir)
    if table_info is None:
        return {"skipped": True, "reason": "no table_info"}

    name = table_info.get("name", table_dir.name)
    if should_skip(name):
        return {"skipped": True, "reason": "error/corrupt table"}

    existing = existing_spec_names(table_dir)
    stats = {"predicate": 0, "projection": 0, "version": 0, "combined": 0}

    stats["predicate"] = generate_predicate_specs(table_dir, table_info, existing)
    stats["projection"] = generate_projection_specs(table_dir, table_info, existing)
    stats["version"] = generate_version_specs(table_dir, table_info, existing)
    stats["combined"] = generate_combined_specs(table_dir, table_info, existing)

    total = sum(stats.values())
    return {"skipped": False, "new_specs": total, "details": stats}


def main():
    print("=" * 60)
    print("Delta Kernel Spec Generator")
    print("=" * 60)

    total_new = 0
    total_tables = 0
    strategy_totals = {"predicate": 0, "projection": 0, "version": 0, "combined": 0}

    # Process read spec tables
    print("\nProcessing read spec tables...")
    for table_dir in sorted(IMPROVED_DAT.iterdir()):
        if not table_dir.is_dir():
            continue
        if table_dir.name == "write_workloads":
            continue

        total_tables += 1
        result = process_table_dir(table_dir)
        if not result.get("skipped", False):
            new = result.get("new_specs", 0)
            total_new += new
            for k, v in result.get("details", {}).items():
                strategy_totals[k] += v

    # Process write workload tables
    print("Processing write workload tables...")
    if WRITE_WORKLOADS.exists():
        for table_dir in sorted(WRITE_WORKLOADS.iterdir()):
            if not table_dir.is_dir():
                continue

            total_tables += 1
            result = process_table_dir(table_dir)
            if not result.get("skipped", False):
                new = result.get("new_specs", 0)
                total_new += new
                for k, v in result.get("details", {}).items():
                    strategy_totals[k] += v

    print(f"\n{'=' * 60}")
    print(f"Tables processed: {total_tables}")
    print(f"New specs generated: {total_new}")
    print(f"\nBy strategy:")
    for strategy, count in strategy_totals.items():
        print(f"  {strategy}: {count}")

    # Count total specs now
    total_specs = 0
    for root, dirs, files in os.walk(IMPROVED_DAT):
        for f in files:
            if f.endswith(".json") and "/specs/" in os.path.join(root, f):
                total_specs += 1
    print(f"\nTotal spec files: {total_specs}")
    print("=" * 60)


if __name__ == "__main__":
    main()
