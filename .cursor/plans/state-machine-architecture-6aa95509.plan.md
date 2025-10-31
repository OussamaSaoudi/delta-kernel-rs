<!-- 6aa95509-39e6-46a1-aafe-a16903b54e6a ccee28eb-fd8f-42c9-a731-ad761a514bab -->
# FFI Plan Visitor and State Machine Exposure

## Architecture: Visitor Pattern (Following Schema/Expression Model)

### Step 1: EnginePlanVisitor (Similar to EngineSchemaVisitor)

C++ engine provides visitor struct:

```c
// ffi/include/logical_plan.h
typedef struct EnginePlanVisitor {
    void* data;  // Opaque engine context
    
    // Allocate list to hold child plans
    usize (*make_plan_list)(void* data, usize reserve);
    
    // Visit Scan node (leaf)
    void (*visit_scan)(
        void* data,
        usize sibling_list_id,
        bool is_json,
        KernelStringSlice* file_paths,
        usize num_files,
        HandleSharedSchema schema
    );
    
    // Visit Filter node (has child)
    void (*visit_filter)(
        void* data,
        usize sibling_list_id,
        usize child_plan_id,
        void* filter_context  // RowFilter - opaque
    );
    
    // Visit FilterByExpression node
    void (*visit_filter_by_expression)(
        void* data,
        usize sibling_list_id,
        usize child_plan_id,
        HandleSharedExpression predicate
    );
    
    // Visit ParseJson node
    void (*visit_parse_json)(
        void* data,
        usize sibling_list_id,
        usize child_plan_id,
        KernelStringSlice json_column,
        HandleSharedSchema target_schema,
        KernelStringSlice output_column
    );
    
    // Visit Select node
    void (*visit_select)(
        void* data,
        usize sibling_list_id,
        usize child_plan_id,
        HandleSharedSchema output_schema
        // TODO: expressions list
    );
    
    // Visit Union node
    void (*visit_union)(
        void* data,
        usize sibling_list_id,
        usize left_plan_id,
        usize right_plan_id
    );
    
} EnginePlanVisitor;

// Kernel calls this, C++ builds its representation
usize visit_logical_plan(
    HandleSharedLogicalPlan plan,
    EnginePlanVisitor* visitor
);
```

### Step 2: Rust Implementation (ffi/src/logical_plan.rs)

```rust
#[handle_descriptor(target=LogicalPlanNode, mutable=false, sized=true)]
pub struct SharedLogicalPlan;

#[repr(C)]
pub struct EnginePlanVisitor {
    pub data: *mut c_void,
    pub make_plan_list: extern "C" fn(*mut c_void, usize) -> usize,
    pub visit_scan: extern "C" fn(
        *mut c_void,
        usize, // sibling_list_id
        bool,  // is_json
        *const KernelStringSlice, // file_paths
        usize, // num_files
        Handle<SharedSchema>,
    ),
    pub visit_filter: extern "C" fn(*mut c_void, usize, usize, *mut c_void),
    pub visit_filter_by_expression: extern "C" fn(
        *mut c_void, usize, usize, Handle<SharedExpression>
    ),
    pub visit_parse_json: extern "C" fn(
        *mut c_void, usize, usize,
        KernelStringSlice, Handle<SharedSchema>, KernelStringSlice
    ),
    // ... rest
}

#[no_mangle]
pub unsafe extern "C" fn visit_logical_plan(
    plan: Handle<SharedLogicalPlan>,
    visitor: &mut EnginePlanVisitor,
) -> usize {
    let plan = unsafe { plan.as_ref() };
    visit_plan_impl(plan, visitor, 0)  // 0 = root sibling list
}

fn visit_plan_impl(
    plan: &LogicalPlanNode,
    visitor: &mut EnginePlanVisitor,
    sibling_list_id: usize,
) -> usize {
    match plan {
        LogicalPlanNode::Scan(scan) => {
            let paths: Vec<_> = scan.files.iter()
                .map(|f| kernel_string_slice!(f.location.as_str()))
                .collect();
            (visitor.visit_scan)(
                visitor.data,
                sibling_list_id,
                scan.is_json,
                paths.as_ptr(),
                paths.len(),
                scan.schema.clone().into(),
            );
            sibling_list_id
        }
        LogicalPlanNode::FilterByExpression(filter) => {
            // Visit child first
            let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
            visit_plan_impl(&filter.child, visitor, child_list_id);
            
            // Then visit this node
            (visitor.visit_filter_by_expression)(
                visitor.data,
                sibling_list_id,
                child_list_id,
                Arc::new(filter.predicate.clone()).into(),
            );
            sibling_list_id
        }
        LogicalPlanNode::ParseJson(parse) => {
            let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
            visit_plan_impl(&parse.child, visitor, child_list_id);
            
            (visitor.visit_parse_json)(
                visitor.data,
                sibling_list_id,
                child_list_id,
                kernel_string_slice!(parse.json_column.as_str()),
                parse.target_schema.clone().into(),
                kernel_string_slice!(&parse.output_column),
            );
            sibling_list_id
        }
        // ... other nodes
    }
}
```

### Step 3: State Machine FFI (ffi/src/state_machine.rs)

```rust
#[handle_descriptor(target=StateMachinePhase<Scan>, mutable=false, sized=true)]
pub struct SharedStateMachinePhase;

#[repr(C)]
pub enum PhaseType {
    Terminus = 0,
    PartialResult = 1,
    Consume = 2,
}

// Build state machine from ScanBuilder
#[no_mangle]
pub unsafe extern "C" fn scan_builder_build_state_machine(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    predicate: Option<&mut EnginePredicate>,
) -> ExternResult<Handle<SharedStateMachinePhase>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    build_sm_impl(snapshot, predicate).into_extern_result(&engine.as_ref())
}

fn build_sm_impl(
    snapshot: SnapshotRef,
    predicate: Option<&mut EnginePredicate>,
) -> DeltaResult<Handle<SharedStateMachinePhase>> {
    let mut scan_builder = snapshot.scan_builder();
    if let Some(predicate) = predicate {
        let mut visitor_state = KernelExpressionVisitorState::default();
        let pred_id = (predicate.visitor)(predicate.predicate, &mut visitor_state);
        let predicate = unwrap_kernel_predicate(&mut visitor_state, pred_id);
        scan_builder = scan_builder.with_predicate(predicate.map(Arc::new));
    }
    let phase = scan_builder.build_state_machine()?;
    Ok(Arc::new(phase).into())
}

// Query phase type
#[no_mangle]
pub unsafe extern "C" fn phase_get_type(
    phase: Handle<SharedStateMachinePhase>
) -> PhaseType {
    let phase = unsafe { phase.as_ref() };
    match phase {
        StateMachinePhase::Terminus(_) => PhaseType::Terminus,
        StateMachinePhase::PartialResult(_) => PhaseType::PartialResult,
        StateMachinePhase::Consume(_) => PhaseType::Consume,
    }
}

// Get plan from phase (for PartialResult/Consume)
#[no_mangle]
pub unsafe extern "C" fn phase_get_plan(
    phase: Handle<SharedStateMachinePhase>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedLogicalPlan>> {
    let phase = unsafe { phase.as_ref() };
    let engine = unsafe { engine.as_ref() };
    phase_get_plan_impl(phase).into_extern_result(&engine)
}

// Check if phase is parallelizable
#[no_mangle]
pub unsafe extern "C" fn phase_is_parallelizable(
    phase: Handle<SharedStateMachinePhase>
) -> bool {
    let phase = unsafe { phase.as_ref() };
    match phase {
        StateMachinePhase::PartialResult(p) => p.is_parallelizable(),
        StateMachinePhase::Consume(p) => p.is_parallelizable(),
        _ => false,
    }
}

// Transition for PartialResult (no data needed)
#[no_mangle]
pub unsafe extern "C" fn phase_next_partial(
    phase: Handle<SharedStateMachinePhase>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedStateMachinePhase>> {
    // Takes ownership, transitions, returns new phase
    let engine = unsafe { engine.as_ref() };
    phase_next_partial_impl(phase).into_extern_result(&engine)
}

// Transition for Consume (needs Arrow data)
#[no_mangle]
pub unsafe extern "C" fn phase_next_consume(
    phase: Handle<SharedStateMachinePhase>,
    engine: Handle<SharedExternEngine>,
    array: *const ArrowArray,
    schema: *const ArrowSchema,
) -> ExternResult<Handle<SharedStateMachinePhase>> {
    let engine = unsafe { engine.as_ref() };
    phase_next_consume_impl(phase, array, schema).into_extern_result(&engine)
}

// Extract Scan from terminal phase
#[no_mangle]
pub unsafe extern "C" fn phase_get_terminal_scan(
    phase: Handle<SharedStateMachinePhase>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedScan>> {
    let phase = unsafe { phase.into_box() };
    let engine = unsafe { engine.as_ref() };
    match *phase {
        StateMachinePhase::Terminus(scan) => Ok(Arc::new(scan).into()),
        _ => Err(Error::generic("Not a terminal phase")),
    }.into_extern_result(&engine)
}
```

### Step 4: C++ Example (ffi/examples/state-machine-execution/)

```cpp
// state_machine_example.cpp
#include "delta_kernel_ffi.h"
#include <vector>
#include <memory>

// C++ engine's plan representation
struct MyCppPlan {
    enum Type { SCAN, FILTER, PARSE_JSON, ... };
    Type type;
    std::vector<std::unique_ptr<MyCppPlan>> children;
    // ... type-specific data
};

// Visitor implementation
struct PlanVisitorContext {
    std::vector<std::vector<std::unique_ptr<MyCppPlan>>> plan_lists;
    
    usize make_list(usize reserve) {
        plan_lists.emplace_back();
        plan_lists.back().reserve(reserve);
        return plan_lists.size() - 1;
    }
    
    void add_scan(usize list_id, bool is_json, ...) {
        auto plan = std::make_unique<MyCppPlan>();
        plan->type = MyCppPlan::SCAN;
        // ... populate
        plan_lists[list_id].push_back(std::move(plan));
    }
};

extern "C" {
    usize visitor_make_list(void* data, usize reserve) {
        return static_cast<PlanVisitorContext*>(data)->make_list(reserve);
    }
    
    void visitor_scan(void* data, usize list_id, bool is_json, ...) {
        static_cast<PlanVisitorContext*>(data)->add_scan(list_id, is_json, ...);
    }
    // ... other visitor functions
}

int main() {
    // Get initial phase
    auto phase = scan_builder_build_state_machine(snapshot, engine, predicate);
    
    // Execution loop
    while (phase_get_type(phase) != PHASE_TYPE_TERMINUS) {
        // Get plan
        auto plan_handle = phase_get_plan(phase, engine);
        
        // Visit plan to build C++ representation
        PlanVisitorContext ctx;
        EnginePlanVisitor visitor = {
            &ctx,
            visitor_make_list,
            visitor_scan,
            visitor_filter,
            visitor_filter_by_expression,
            visitor_parse_json,
            // ...
        };
        visit_logical_plan(plan_handle, &visitor);
        
        // Execute in C++ engine
        MyCppPlan* cpp_plan = ctx.plan_lists[0][0].get();
        ArrowArray* result = my_engine.execute(cpp_plan);
        
        // Transition
        if (phase_get_type(phase) == PHASE_TYPE_PARTIAL_RESULT) {
            phase = phase_next_partial(phase, engine);
        } else {
            phase = phase_next_consume(phase, engine, result->array, result->schema);
        }
    }
    
    // Extract final scan
    auto scan = phase_get_terminal_scan(phase, engine);
    visit_scan_files(scan, ...);
}
```

## Implementation Plan

1. **Add ffi/src/logical_plan.rs**

   - Define EnginePlanVisitor struct
   - Implement visit_logical_plan function
   - Handle recursive traversal

2. **Add ffi/src/state_machine.rs**

   - Expose phase types and transitions
   - scan_builder_build_state_machine()
   - phase_get_plan(), phase_next_*()

3. **Add Arrow C interface** (if not exists)

   - ArrowArray/ArrowSchema definitions
   - Conversion to/from EngineData

4. **Update ffi/examples/read-table**

   - Add state-machine-based execution path
   - Show plan visitor usage
   - Compare with current scan() approach

5. **Tests**

   - FFI roundtrip (Rust plan → C++ visitor → verify)
   - Phase transitions
   - End-to-end execution

## Benefits of Visitor Pattern

- **Familiar**: Matches existing schema/expression visitors
- **Flexible**: C++ builds any representation it wants
- **Efficient**: No serialization, direct function calls
- **Type-safe**: C++ controls memory layout
- **Streaming**: Can handle large plans incrementally

## Files to Create/Modify

- `ffi/src/logical_plan.rs` (new)
- `ffi/src/state_machine.rs` (new)
- `ffi/src/lib.rs` (add modules)
- `ffi/examples/read-table/state_machine.c` (new example)
- `ffi/examples/read-table/CMakeLists.txt` (update)