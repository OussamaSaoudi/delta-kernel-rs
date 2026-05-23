# Plans + DataFusion Architecture — Shipping Plan

Sequence for landing the new SSA-based plan IR + `Context`/`PlanBuilder` +
slim state-machine framework + DataFusion executor adaptation. Architecture
is fully prototyped on `stack/fsr` (PR4.5 → PR8 + cleanup refactors, all
unmerged). This plan is about getting that work into `main` as a sequence
of mergeable PRs.

## Current state (anchor)

Prototype landed locally; nothing merged. Already in the tree:

- `kernel/src/plans/ir/plan.rs` — `Plan`, `PlanNode`, `NodeKind`,
  `Ref`, `ResultPlan`, `JoinKind` (LeftAnti only).
- `kernel/src/plans/ir/nodes/mod.rs` — per-variant payloads
  (`ScanParquetNode`, `ScanJsonNode`, `ProjectNode`, `FilterNode`,
  `UnionNode`, `LoadNode`, `MaxByVersionNode`, `EquiJoinNode`,
  `ListFilesNode`, `ValuesNode`, `ReduceSink`, `ScanFileColumns`,
  `DvRef`).
- `kernel/src/plans/ir/schema_inference.rs` — `infer_expression_type`.
- `kernel/src/plans/kernel_reducers/` — `KernelReducer` trait, `Extractor<O>`,
  `KernelReducerToken`/`Kind`, `ReducerHandle`/`FinishedHandle`, and the three
  concrete impls (`CheckpointHintReader`, `MetadataProtocolReader`,
  `SidecarCollector`).
- `kernel/src/plans/state_machines/framework/`:
  - `state_machine.rs` — `StateMachine` trait, `NextStep<R>`,
    methods `get_step` / `submit`.
  - `step.rs` — `EngineRequest { SchemaQuery, Reduce }`.
  - `step_payload.rs` — `EngineResponse { Reducer(FinishedHandle), Schema, Empty }`.
  - `plan_context.rs` — `Context` (Rc<RefCell<ContextState>>), `PlanBuilder`,
    session_id stale-builder protection, dispatch (`reduce`, `schema_query`,
    `into_result_plan`), DCE via `Plan::reachable_from`.
  - `coroutine/` — `CoroutineSM` driver.
- `kernel/src/plans/state_machines/scan/`:
  - `reconciliation.rs` — `build_reconciliation` over `&Context`.
  - `full_state.rs` — `FullState::state_machine_ssa` alongside legacy.
  - `scan_plan.rs` — `build_scan_plan`, scan SMs using new Context.
  - `file_scan.rs` — legacy registry-based SMs (still present).
- `delta-kernel-datafusion-engine/src/`:
  - `compile/logical/` — per-NodeKind lowering to DataFusion `LogicalPlan`.
  - `compile/expr_translator.rs` — Kernel expressions → DataFusion expressions.
  - `executor/mod.rs` — `DataFusionExecutor` driving `EngineRequest::Reduce` +
    `SchemaQuery`, `drive_ssa_to_dataframe`.
  - `exec/load_exec.rs`, `load_helpers.rs`, `load_provider.rs` — Load operator
    physical-plan wiring (DV, nested file_meta paths).

What still has to happen for shipping:

- Each prototype chunk needs to land as its own reviewable PR against `main`.
- Legacy code (old IR, old SMs, `Step::Plans` engine path) must be deleted
  once new vertical is live.
- User-facing entry points need to route to the new SMs as their default.
- Acceptance workloads (DAT) green via the user-facing surface.

## Strategy

Three-phase shipping:

1. **Foundation** — IR + framework + reducers + Context land additively.
   Legacy code untouched.
2. **Verticals** — engine compile + driver + FSR + Scan SMs land. Both old
   and new pipelines work; new is reachable via `_ssa` entry points.
3. **Cutover & cleanup** — user-facing entry points switch to new pipelines;
   legacy SMs / IR / engine paths deleted.

Each PR exits with `cargo build --workspace --all-features` clean,
`cargo nextest run --workspace --all-features` green, clippy clean, fmt clean.

## Shipping order

### Phase 1 — Foundation (additive, can ship in parallel)

#### PR-1: IR types + schema inference

Land `plans/ir/plan.rs`, `plans/ir/nodes/mod.rs`, `plans/ir/schema_inference.rs`.

**Verify**: unit tests for each `NodeKind`, `Plan::reachable_from` DCE,
`infer_expression_type` covering every `Expression` variant. No call sites
in legacy code — additive.

**Risk**: low. Pure data types + a pure function. Nothing referenced
outside the new module.

#### PR-2: Kernel reducers (rename + impls)

Land `plans/kernel_reducers/` with trait + the three concrete reducers +
extractor. Includes the rename from `plans/kdf/`; the legacy `ConsumerKdf`
trait disappears, but the legacy SMs still construct concrete reducers
(CheckpointHintReader etc.) via the new module's path.

**Verify**: reducer unit tests pass; legacy SMs still build and run.

**Risk**: low–medium. Cross-cutting rename; touches every call site that
references the old `ConsumerKdf`/`KdfOutput` types. Mechanical.

**Note**: Could ship before PR-1 — no dependency.

#### PR-3: State-machine framework rename + slim

Land the renamed framework:

- `PhaseOperation` → `EngineRequest` (with `SchemaQuery` + `Reduce` variants,
  plus legacy `Plans(Vec<Plan>)` kept temporarily).
- `PhaseState`/`StepResult` → `EngineResponse` (typed enum).
- `AdvanceResult<R>` → `NextStep<R>`.
- `get_operation` → `get_step`; `advance` → `submit`; `phase_name` → `step_name`.
- `CoroutineSM` → `Coroutine` (or keep `CoroutineSM` if prototype kept it).
- `StateMachine` trait keeps its name.

**Verify**: existing SMs drive via renamed methods/types. Driver-loop tests.

**Risk**: medium. Touches every SM and the engine driver. Pure rename
+ reshape; semantics unchanged for legacy SMs.

**Note**: depends on PR-2 if framework types reference `KernelReducer`.

### Phase 2 — Verticals (depends on Phase 1)

#### PR-4: Context + PlanBuilder

Land `plan_context.rs`. Includes:

- `Context` with `Rc<RefCell<ContextState>>` + the `Engine` (coroutine yield) handle.
- `PlanBuilder` (the "cursor"; Clone via `Rc`).
- Source methods (`list_files`, `scan_parquet`, `scan_json`, `values`).
- Transform methods (`filter`, `project`, `project_with_schema`, `select`,
  `append_col_typed`, `insert_col_after`, `replace_col`, `drop_col`, `load`,
  `max_by_version`, `left_anti_join`, `union_all`, `union_ordered`).
- Dispatch methods (`reduce`, `schema_query`, `into_result_plan`).
- session_id stale-builder protection; DCE in `reduce`/`into_result_plan`
  via `Plan::reachable_from`.

**Verify**: unit tests per builder method (schema validation failures,
stale-builder rejection, DCE pruning). RefCell-discipline test under tokio
(borrow never crosses `.await`). Microbench of build cost vs legacy
`PlanBuilder`.

**Risk**: medium. The RefCell discipline is structural; review it
carefully. The session_id mechanism is novel; lock with tests.

#### PR-5: DataFusion compile path

Land `delta-kernel-datafusion-engine/src/compile/logical/` lowering each
`NodeKind` to a DataFusion `LogicalPlan`. `compile_plan(plan)` entry point.

**Verify**: per-variant compile tests in
`delta-kernel-datafusion-engine/tests/plan_compile.rs`. Composite plan
shapes (filter chain, joins, unions, MaxByVersion, Load with DV).

**Risk**: medium. Schema-mapping bugs hide here; tests must cover every
NodeKind shape.

**Note**: can ship parallel to PR-4 since both depend only on PR-1.

#### PR-6: Engine driver (Reduce dispatch + drive_ssa_to_dataframe)

Land `DataFusionExecutor::run_phase` handling `EngineRequest::Reduce`:
compile, execute subgraph feeding `terminal`, feed batches to reducer's
`apply`, finish into `FinishedHandle`, return `EngineResponse::Reducer`.
Plus `drive_ssa_to_dataframe`: drive an SM to completion + run terminal
`ResultPlan` + return a DataFrame. Keep old `Step::Plans` dispatch for now.

**Verify**: synthetic SMs round-trip through the driver. Integration tests
in `delta-kernel-datafusion-engine/tests/`.

**Risk**: medium. Streaming + reducer drain semantics need to be exact;
test corner cases (early Break, error mid-stream, empty batches).

**Note**: depends on PR-4 (uses Context/builder types) and PR-5 (uses
`compile_plan`).

#### PR-7: FSR vertical

Land:

- `state_machines/scan/reconciliation.rs` (`build_reconciliation`).
- `state_machines/scan/full_state.rs` — `FullState::state_machine_ssa` on
  the new Context, alongside the existing legacy SM.
- `action_pair.rs` shrunk to schema-only helpers (Pair / augment_add
  dissolved).
- Engine routes `full_state` through `drive_ssa_to_dataframe`.
- `tests/fsr_real.rs` exercises the new path end-to-end.

**Verify**:

- `tests/fsr_real.rs` green on representative tables: no checkpoint,
  checkpoint without sidecar, V2 multipart, with/without stats,
  with/without partition values.
- Parity tests against legacy FSR output (semantic equivalence).
- Existing FSR test suite green via either entry point.
- Performance within 10% of legacy on FSR benchmarks.

**Risk**: high. First user-visible milestone. Parity is the long pole —
build the parity test harness in this PR (or pre-PR).

#### PR-8: Scan vertical

Land:

- `state_machines/scan/scan_plan.rs` — `Scan::scan_state_machine_ssa`,
  `Scan::scan_metadata_state_machine_ssa`, `Scan::scan_data_state_machine_ssa`.
- Data-phase Load with nested file_meta (`col(["add", "path"])`); ensure
  engine `LoadExec` resolves nested-path file metadata.
- `scan_data_projection(&state_info)?` → `(Vec<Expr>, SchemaRef)` →
  `PlanBuilder::project_with_schema(...)`.
- `tests/scan_real.rs` exercises the new path.

**Verify**: full Scan test suite (data phase, column mapping, DVs,
partitions). Parity tests vs legacy.

**Risk**: medium (after PR-7's infrastructure is in place). The nested
file_meta path is the structural shift; validate end-to-end.

### Phase 3 — Cutover & cleanup

#### PR-9: Switch user-facing entry points to new SMs

Land:

- `Snapshot::full_state` / `Scan::scan_metadata` / `Scan::scan_data` route
  to the new `_ssa` state machines by default.
- Engine entry points (`drive_to_completion`, `drive_to_dataframe`) dispatch
  to the new path.
- Public API surface unchanged (no breaking changes for connector callers).

**Verify**: full integration tests green via the user-facing surface.
Acceptance workloads (DAT) green via the user-facing surface — this is
the first time acceptance runs through the new architecture end-to-end.

**Risk**: medium. Production-shape integration. If parity (PR-7, PR-8)
held, this is mechanical.

#### PR-10: Delete legacy state machines + engine compile path + `Step::Plans`

Land:

- Delete legacy `file_scan.rs` SMs (or shrink to nothing).
- Delete `Step::Plans` variant from `EngineRequest`.
- Delete legacy engine compile/dispatch path.
- Delete `RelationRegistry`, `RelationHandle`, `consume_phase` plumbing.

**Verify**: workspace builds + all tests green.

**Risk**: low. If PR-9 stuck, nothing user-visible references the legacy
path.

#### PR-11: Delete legacy IR types

Land:

- Delete `DeclarativePlanNode`, old `PlanBuilder` (the tree), `SinkType`,
  `LoadSink`, `ConsumeSink`, old `Plan { root, sink }`.
- Delete `action_pair.rs`'s `Pair` type if any remnants survive.
- Delete any remaining old-naming aliases.

**Verify**: no references to deleted types. Full workspace tests pass.

**Risk**: low.

#### PR-12: Polish sweeps (optional, can interleave)

Already in the prototype as separate commits; fold whichever haven't
landed yet:

- "point-edit Cursor primitives + Pair/augment_add dissolution"
- "consolidate scan/ pipeline helpers and remove redundancies"
- "normalize NodeKind to enum-of-structs and drop RelationHandle"
- "drop JoinKind::Inner and split Scan into ScanParquet + ScanJson"
- "collapse StepResult accumulator into typed StepPayload"
- "rename plan IR core types and SM<->engine protocol"

Mechanical sweeps; small individually.

## Critical-path risks

1. **Parity (PR-7, PR-8)** — semantic equivalence with legacy is the long
   pole. Build the parity test harness in PR-7 and reuse in PR-8.
2. **RefCell-across-await (PR-4)** — structural correctness; tested with
   dedicated tokio-driven tests.
3. **Performance (PR-4, PR-7, PR-8)** — RefCell + DCE + Rc overhead.
   Microbench at PR-4, end-to-end benches at PR-7/8.
4. **Nested file_meta resolution (PR-8)** — engine `LoadExec` may not
   support nested `ColumnName` paths today; verify or extend.
5. **`Step::Plans` ↔ `EngineRequest::Reduce` coexistence (PR-3 → PR-10)** —
   keep both paths alive until PR-10 deletes legacy.

## Parallelization

Day 1 (foundation, independent):
- PR-1 (IR + schema_inference)
- PR-2 (kernel_reducers rename)
- PR-3 (framework rename) — gated on PR-2 if framework references
  KernelReducer.

After PR-1 + PR-3 land:
- PR-4 (Context + PlanBuilder) and PR-5 (DataFusion compile path) in parallel.

After PR-4 + PR-5 land:
- PR-6 (Engine driver).

After PR-6 lands:
- PR-7 (FSR vertical) — sole shipping milestone for Phase 2.

After PR-7:
- PR-8 (Scan vertical) — reuses PR-7's infrastructure heavily.

After PR-8:
- PR-9 (cutover), PR-10/11 (deletes), PR-12 (sweeps).

Reasonable wall-clock: Phase 1 ~ 1 week (parallel), Phase 2 ~ 2–3 weeks
(PR-7 parity is the long pole), Phase 3 ~ 3–5 days. Total ~4–5 weeks for
one engineer; ~2–3 weeks for two engineers splitting on Phase 1 parallel
PRs and pair-reviewing Phase 2.

## Out of scope

- **CDF (Change Data Feed)** — architecture supports it; no implementation
  in this plan.
- **Write paths** — `Transaction` not migrated; untouched.
- **DataFusion-side optimization beyond what's needed to drive Reduce**.

## Testing

- **Per-PR unit tests** — local to the module touched (PR-1 IR tests, PR-4
  builder tests, etc.).
- **Engine compile tests** (`tests/plan_compile.rs`) — PR-5.
- **End-to-end SM tests** (`tests/fsr_real.rs`, `tests/scan_real.rs`) —
  PR-7, PR-8.
- **Parity tests** (new SM vs legacy SM on the same input) — PR-7
  (built/extended), reused in PR-8.
- **Acceptance workloads (DAT)** — gated on PR-9 (user-facing surface
  routes to new). First end-to-end validation via the user-facing API.
