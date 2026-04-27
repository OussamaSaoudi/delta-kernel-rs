# KDF state management — design doc

Status: working design for the declarative-plans migration (PRs 3–6).

This doc is organized around three audiences:

- [§1 Infrastructure](#1-infrastructure) — the shape of the framework.
- [§2 Adding a new KDF](#2-adding-a-new-kdf) — author experience.
- [§3 Using a KDF](#3-using-a-kdf) — SM-author experience.
- [§4 Safety checks](#4-safety-checks) — what the framework validates automatically.
- [§5 Tracing](#5-tracing) — built-in observability.
- [§6 Design rationale](#6-design-rationale) — why this shape.
- [§7 Open items](#7-open-items) — deferred improvements.

---

## 1. Infrastructure

### What a KDF is

A **Kernel-Defined Function** is a chunk of stateful per-row logic the kernel
owns and an engine drives. Two shapes:

- **Filter KDF** — `(batch, incoming selection) → narrowed selection`. Used
  for log-replay dedup, data skipping, checkpoint reconciliation.
- **Consumer KDF** — `batch → Continue | Break`. Observes rows, accumulates
  state, returns early when satisfied. Used for reading hint files, building
  log segments, collecting sidecars.

Both types:

1. Run across N partitions (N=1 for global consumers; N≥1 for partitioned
   filters).
2. Each partition independently finalizes into an accumulated state.
3. The state machine gets a **typed output** — the reduction of all partition
   states into a single caller-facing value.

### Architecture (data flow)

```
 Factory (plans::consumers / plans::filters)
        │  state.consumer()   ─or─  state.filter().partitioned_by(expr)
        ▼
 KdfFactory<K, O> = { node: KdfNode<K>, extract: Fn(Vec<Any>) → Result<O> }
        │  plan.consume(factory)   ─or─   plan.filter_by(factory).stream()
        ▼
 Prepared<O> = { plan, token, extract }                ─ construction time ends ─
        │  phase.execute(prepared, "phase_name").await
        ▼
 Executor runs the plan tree; for each KDF node, per partition:
    Handle<K> = { token, ctx: TraceContext, partition, inner: Box<K> }
    handle.apply(batch, …)            // per batch
    handle.finish() → Box<Any>         // at end-of-child
        │  collected in PhaseKdfState: HashMap<KdfStateToken, Vec<StoredState>>
        ▼
 Prepared's extract closure: pull states by token, downcast Vec<Any> → Vec<Self>,
 call KdfOutput::into_output(parts) → typed O
        ▼
 SM receives: O  (e.g. Option<CheckpointHint>, AddRemoveDedupState, LogSegment)
```

### Full type inventory

Runtime behavior (object-safe — carried in the plan tree via `Box<dyn Kdf>`):

| Type | Role |
|---|---|
| `Kdf` | shared supertrait — `kdf_id() -> &'static str`, `finish() -> Box<Any>` |
| `FilterKdf: Kdf` | filter runtime — `apply(batch, incoming) -> selection`, `fresh_partition`, `clone_boxed` |
| `ConsumerKdf: Kdf` | consumer runtime — `apply(batch) -> Continue/Break`, `clone_boxed` |
| `KdfControl` | `Continue` / `Break` enum |

Typed output (static dispatch — one impl per state):

| Type | Role |
|---|---|
| `KdfOutput` | declares `type Output`, `fn into_output(Vec<Self>) -> Output`; default `.filter()` / `.consumer()` factory methods |

Identity:

| Type | Fields | Lifecycle stage |
|---|---|---|
| `KdfStateToken` | `{ kdf_id: &'static str, serial: u64 }` | stamped at plan-build time |
| `TraceContext` | `{ sm: &'static str, phase: &'static str }` | set at phase-execute time |

Value-level carriers (generic over `K: Kdf + ?Sized`):

| Generic | Aliases | Role |
|---|---|---|
| `KdfNode<K>` | `FilterByKDF`, `ConsumerByKDF` | lives in the plan tree; carries `{ initial_state, partitionable_by?, requires_ordering?, token }` |
| `KdfFactory<K, O>` | `TypedFilter<O>`, `TypedConsumer<O>` | factory output; `{ node, extract }` |
| `Handle<K>` | `FilterHandle`, `ConsumeHandle` | per-partition runtime + FFI interchange; `{ token, ctx, partition, inner }` |

Helpers:

| Function | Role |
|---|---|
| `downcast_all<S>(Vec<Any>, &KdfStateToken) -> Vec<S>` | erased → typed fan-in; error message carries the token |
| `take_single<S>(Vec<S>, &KdfStateToken) -> S` | single-partition unwrap with token-tagged error |

Downstream (SM framework, PR 6):

| Type | Role |
|---|---|
| `PhaseKdfState` | `HashMap<KdfStateToken, Vec<StoredState>>` — validates consistency |
| `StoredState` | `{ erased: Box<Any>, submitted_from: TraceContext, partition: u32 }` — per-submission record |
| `Prepared<O>` | `{ plan, token, extract }` — awaitable |
| `Filtered<O>` | post-filter chainable wrapper |

### Symbol count

- **2 standalone identity types** (`KdfStateToken`, `TraceContext`).
- **4 runtime traits** (`Kdf`, `FilterKdf`, `ConsumerKdf`, `KdfOutput`).
- **3 generic carriers** with 6 aliases for filter/consumer pairs.
- **1 enum** (`KdfControl`).
- **2 helpers**.

**12 named framework symbols.** Every filter/consumer pair is an alias over
a generic struct — no mirrored definitions.

---

## 2. Adding a new KDF

One state type per KDF. One file. Four impl blocks + one factory line.

### Example: a consumer KDF that reads `_last_checkpoint`

File: `plans/kdf/state/consumer/checkpoint_hint.rs`

```rust
use std::any::Any;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::plans::errors::DeltaError;
use crate::plans::kdf::{ConsumerKdf, Kdf, KdfControl, KdfOutput};
use crate::schema::{ColumnName, DataType};
use crate::{DeltaResult, EngineData, Version};

// 1. OUTPUT — the shape callers receive.
#[derive(Debug, Clone)]
pub struct CheckpointHint {
    pub version: Version,
    pub size: Option<i64>,
    // ...
}

// 2. STATE — what one partition accumulates.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointHintReaderState {
    version: Option<Version>,
    size: Option<i64>,
    processed: bool,
}

impl CheckpointHintReaderState {
    fn into_hint(self) -> Option<CheckpointHint> {
        self.version.map(|v| CheckpointHint { version: v, size: self.size })
    }
}

// 3. SHARED base (kdf_id + finish). Pure boilerplate — a derive could
//    generate this eventually.
impl Kdf for CheckpointHintReaderState {
    fn kdf_id(&self) -> &'static str { "consumer.checkpoint_hint" }
    fn finish(self: Box<Self>) -> Box<dyn Any + Send> { Box::new(*self) }
}

// 4. RUNTIME — the interesting part: how rows get consumed.
#[typetag::serde]
impl ConsumerKdf for CheckpointHintReaderState {
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        self.visit_rows_of(batch)?;
        Ok(if self.processed { KdfControl::Break } else { KdfControl::Continue })
    }
    fn clone_boxed(&self) -> Box<dyn ConsumerKdf> { Box::new(self.clone()) }
}

// 5. TYPED OUTPUT — what callers receive, and how to reduce across partitions.
impl KdfOutput for CheckpointHintReaderState {
    type Output = Option<CheckpointHint>;
    fn into_output(parts: Vec<Self>) -> Result<Option<CheckpointHint>, DeltaError> {
        // Use the framework helper; errors include the token for clarity.
        // (The Self token is accessible via crate::plans::kdf::current_token() within
        // the extract closure — plumbed by the framework.)
        Ok(crate::plans::kdf::take_single_bare(parts)?.into_hint())
    }
}

// ... RowVisitor impl (reads the row) ...
```

Then a **one-line factory** in `plans/consumers.rs`:

```rust
pub fn checkpoint_hint() -> TypedConsumer<Option<CheckpointHint>> {
    CheckpointHintReaderState::default().consumer()
}
```

### What you wrote vs. what's boilerplate

```
author wrote:                                 lines
───────────────────────────────────────────────────
Output struct (CheckpointHint)                   6
State struct + into_hint()                      14
impl Kdf (kdf_id, finish)                        5   ← pure boilerplate
impl ConsumerKdf (apply, clone_boxed)            6   ← apply is real logic
impl KdfOutput (type Output, into_output)        5   ← real: typed output + reducer
impl RowVisitor (cols + visit)                  15  ← real: row-by-row logic
factory (one liner)                              3
───────────────────────────────────────────────────
Total                                          ~54 lines
```

Of the ~54 lines, ~11 are pure boilerplate (`kdf_id`, `finish`,
`clone_boxed`). A future `#[derive(KdfBoilerplate)]` macro would erase them
and bring this down to ~43 lines (see §7). Not on the critical path.

### Files touched per new KDF

Exactly **3 files**:

1. **New file** `plans/kdf/state/{consumer,filter}/<name>.rs` — the actual KDF.
2. **One-line edit** `plans/kdf/state/{consumer,filter}/mod.rs` — declare the
   submodule + re-export the state type.
3. **One-line edit** `plans/{consumers,filters}.rs` — add the factory fn.

No enum variant to add, no global registry to touch, no `lib.rs` edit.
`typetag`'s `linkme`-based registration is fully automatic once the submodule
is declared.

### Adding a filter KDF is the same shape

Replace `ConsumerKdf` with `FilterKdf` and add the partitioning declaration
in the factory:

```rust
pub fn add_remove_dedup() -> TypedFilter<AddRemoveDedupState> {
    AddRemoveDedupState::new()
        .filter()
        .partitioned_by(Arc::new(coalesce_add_remove_path()))
}
```

---

## 3. Using a KDF

From the SM author's seat, using a KDF is **two lines**: build the plan with
a typed terminal, then await.

### Consumer — reads `_last_checkpoint` from the snapshot SM

```rust
let hint_file = FileMeta::new(log_root.join("_last_checkpoint")?, 0, 0);

// Build a plan with a typed consumer terminal.
let prep: Prepared<Option<CheckpointHint>> =
    DeclarativePlanNode::scan_json_as::<CheckpointHintRecord>(vec![hint_file])
        .consume(consumers::checkpoint_hint());

// Await. Returns Option<CheckpointHint> — no tokens, no downcast, no state machinery.
let hint: Option<CheckpointHint> = phase.execute(prep, "checkpoint_hint").await?;
```

### Filter — commit replay in the scan SM

```rust
let per_file = state.commits_newest_first().map(|c| {
    DeclarativePlanNode::scan_json(vec![c.location.clone()], commit_base_schema())
        .select(version_add_remove_cols(c.version), commit_with_version_schema())
});

// Union, filter with typed dedup, project, Results sink + typed extract.
let prep: Prepared<AddRemoveDedupState> = DeclarativePlanNode::union(per_file)?
    .filter_by(filters::add_remove_dedup())     // inserts FilterByKDF
    .select(proj_cols, out_schema)
    .stream();                                   // Results sink + extracted state

// Await. Returns the merged dedup state; rows already streamed via Results sink.
let dedup: AddRemoveDedupState =
    phase.execute(prep, "commit_replay").await?;
```

### What the SM author never sees

- `KdfStateToken`
- `PhaseKdfState`
- `Box<dyn Any>`
- Extraction closures
- Handles (`FilterHandle` / `ConsumeHandle`)
- Partitioning semantics (baked into factories)
- Per-KDF trace context setup (framework does it automatically from
  `phase.execute(..., "phase_name")`)

Just: **factory function → typed Prepared → typed await**. All KDF machinery
is invisible at the call site.

### Where factories live

- `plans::consumers::<name>` — one function per consumer KDF.
- `plans::filters::<name>` — one function per filter KDF.

Each is either a one-liner (global consumer) or a short chain (partitioned
filter configured via `.partitioned_by(...)`).

### Multi-KDF patterns

Rare but real. Handled at the SM-framework layer (PR 6), not at the KDF
framework:

- **Two plans, one phase** (concurrent execution): `Prepared::bundle(other)`
  → `Prepared<(A, B)>`.
- **Two KDFs, one tree**: drop down to the raw `.filter_by_kdf(node)` /
  `.consume_by_kdf(node)` + manual `Prepared::new(plan, extract)` escape
  hatch. The typed-factory ergonomics prioritize the 1-KDF common case.

---

## 4. Safety checks

The framework validates automatically at well-defined boundaries. Every
check fails loudly with a token-tagged error — there's no silent
miscompilation from framework bugs.

### Checks enforced

| # | What goes wrong | Where it's caught | Error message includes |
|---|---|---|---|
| 1 | Wrong concrete state type under a token | `downcast_all` | `"expected AddRemoveDedupState for filter.add_remove_dedup#42, partition 3 had wrong type"` |
| 2 | Handle submitted to wrong SM / phase | `PhaseKdfState::submit` | `"handle ctx scan.streaming::commit_replay does not match phase ctx snapshot::checkpoint_hint"` |
| 3 | Partitions came from inconsistent phase | `PhaseKdfState::take` | `"partitions under filter.add_remove_dedup#42 came from mismatched contexts"` |
| 4 | Wrong partition count for global KDF | `take_single` (via `into_output`) | `"filter.add_remove_dedup#42: expected 1 partition, got 3"` |
| 5 | Duplicate token in plan tree | plan validation (on `PlansChain::build`) | `"duplicate KDF token filter.add_remove_dedup#42 in plan tree"` |
| 6 | KDF not of expected trait kind | compile time | `Box<dyn FilterKdf>` vs `Box<dyn ConsumerKdf>` — can't mix |

Each check has zero cost in the success path (HashMap lookup, bounded
equality compare). Failures produce `DeltaError::DeltaCommandInvariantViolation`.

### What makes this possible

Each piece of identity earns its keep:

- **`KdfStateToken { kdf_id, serial }`** stamped at plan build. Keys
  `PhaseKdfState`. Display includes both parts (`filter.foo#42`) so logs
  and errors are human-readable.
- **`TraceContext { sm, phase }`** set at phase execute start. Stored
  alongside each submitted state for cross-check.
- **`partition: u32`** on each Handle. Enables per-partition attribution
  without contaminating the phase-level context.

### Error sample

```
DELTA_COMMAND_INVARIANT_VIOLATION: PhaseKdfState::submit:
  handle ctx scan.streaming::commit_replay does not match phase ctx
  snapshot::checkpoint_hint
  token=filter.add_remove_dedup#3
```

Actionable — tells you the wrong SM's handle tried to land in the wrong
phase's state.

---

## 5. Tracing

The framework instruments all lifecycle events with the `tracing` crate.
**KDF authors write zero tracing code**; spans and events are emitted by
the framework layer with full context.

### Instrumented sites

| Site | Level | Fields emitted |
|---|---|---|
| `Handle::apply` (filter) | `trace` | `sm, phase, partition, kdf_id, serial, rows, selected` |
| `Handle::apply` (consumer) | `trace` | `sm, phase, partition, kdf_id, serial, rows, control` |
| `Handle::finish` | `debug` | `sm, phase, partition, kdf_id, serial` |
| `PhaseKdfState::submit` | `debug` | `sm, phase, partition, kdf_id, serial` |
| `PhaseKdfState::take` | `debug` | `kdf_id, serial, partitions` |
| `Prepared::apply_extract` | `debug` | `kdf_id, serial, outcome` |

### Sample output (`RUST_LOG=delta_kernel::plans=debug`)

```
INFO  sm{name=snapshot}: sm started
DEBUG sm::phase{name=checkpoint_hint}: phase started
TRACE kdf.apply{sm=snapshot phase=checkpoint_hint partition=0
               kdf_id=consumer.checkpoint_hint serial=1
               rows=1 control=break}
DEBUG kdf.finish{... kdf_id=consumer.checkpoint_hint serial=1}: handle finished
DEBUG phase_state.submit{... serial=1}: submitted
DEBUG sm::phase{name=list_files}: phase started
TRACE kdf.apply{... kdf_id=consumer.log_segment serial=2 rows=12 control=continue}
...
```

Flat fields per line; no nested struct rendering needed. Aggregators and
grep pipelines both work. Per-partition drill-down is one field filter away.

### Cross-thread execution

Explicit fields (not relying on tracing's parent-span inheritance) — works
regardless of whether the executor runs KDFs on the caller's thread, a
worker pool, or across FFI. Redundant with parent spans on same-thread
execution; the redundancy is cheap and aids log aggregation.

---

## 6. Design rationale

Four non-obvious choices, each load-bearing:

### A. Two distinct identity types (`KdfStateToken` + `TraceContext`), not one

They're set at different lifecycle stages:

- `KdfStateToken` is stamped at plan-build time. Lives in the plan tree.
  Must be clonable, serializable, and stable across handoffs to engines.
- `TraceContext` is set at phase-execute time. Lives on handles and in
  submitted-state records. Never in the plan tree.

Merging them would either require nullable fields (awkward progressive
fill) or re-stamping identity at execute time (debugging gets worse, since
one KDF has two identities over its life). Keeping them separate matches
their lifetimes.

### B. `KdfNode<K>` and `KdfFactory<K, O>` are distinct types

- `KdfNode<K>` goes in the plan tree. Must be `Clone` + type-erased across
  the enum.
- `KdfFactory<K, O>` lives only at construction, carries a typed `FnOnce`
  extract closure.

Merging them would either erase the extract into `Box<dyn Any>` inside the
tree (losing typed output) or propagate `O` into `DeclarativePlanNode`
(breaking homogeneity).

### C. Object-safe `Kdf` + `FilterKdf` / `ConsumerKdf` subtraits

Shared methods (`kdf_id`, `finish`) live on the `Kdf` supertrait; different-
signature methods (`apply`) on the subtraits. Typetag registers on
subtraits only — each subtrait has its own deserializer registry. The
executor holds `Box<dyn FilterKdf>` (heterogeneous across concrete
filters); `Kdf` alone is never materialized as a trait object.

### D. `KdfOutput` as companion trait, not associated types on `FilterKdf` / `ConsumerKdf`

Associated types on the runtime traits would require `Box<dyn FilterKdf<Output = S>>`
with a fixed `S` per trait object — incompatible with heterogeneous
`Box<dyn FilterKdf>`. `KdfOutput` carries the typed `Output` via static
dispatch at the factory; the runtime traits stay type-erased.

---

## 7. Open items

Deferred improvements, none blocking the migration:

- **`#[derive(KdfBoilerplate)]` proc-macro** to auto-generate `kdf_id`,
  `finish`, `clone_boxed`. Saves ~11 LoC per KDF (~77 LoC across the stack).
  Lives in `delta_kernel_derive`. Revisit after PR 5.

- **Multi-KDF-in-one-tree combinator** (`.observing(...)` on `Filtered<O>`).
  Only needed by the scan SM's JSON checkpoint path. Defer; manual
  `Prepared::new` escape hatch covers it.

- **`phase.execute` observability hooks.** Per-KDF stats (rows processed,
  Continue/Break ratio, finalized state size) as metrics events. Co-lands
  with per-SM memory tracking follow-on.

- **Plan-build-time token uniqueness check.** Noted in §4 (#5) but not
  urgent — the framework's other checks catch downstream effects.
  Implement when `PlansChain::build` lands in PR 6.

- **Trace context propagation across FFI.** For distributed engines that
  run KDFs on a Spark executor, the submitted-state record must carry
  `TraceContext` across the wire. `serde` impl on `StoredState` handles it,
  but wire format needs a migration note for existing deployments (none
  yet). Track with the FFI stack.
