# FFI Exposure Plan - State Machine & Logical Plans

## Status: 📋 PLANNED (NOT YET IMPLEMENTED)

This document describes the plan for exposing state machine phases and logical plan nodes to FFI, enabling C++ engines to execute plans with custom logic.

## Goal

Enable C++ engines to:
1. Build state machine from ScanBuilder
2. Query phase type and properties
3. **Visit logical plans** using visitor pattern (like schema/expression)
4. Execute plans in C++ with custom logic
5. Transition phases (Rust orchestrates, C++ provides data for Consume)
6. Extract final Scan when terminal

## Design: Visitor Pattern

Following the existing FFI visitor patterns for schema and expressions.

### Why Visitor Pattern?

1. **Consistency** - Matches existing `EngineSchemaVisitor` and `EngineExpressionVisitor`
2. **Flexibility** - C++ builds any internal representation it wants
3. **Efficiency** - No serialization, direct function calls
4. **Type Safety** - C++ controls memory layout
5. **Familiar** - Same pattern developers already understand

### Existing Visitor Examples

**Schema Visitor:**
```c
typedef struct EngineSchemaVisitor {
    void* data;
    usize (*make_field_list)(void*, usize);
    void (*visit_struct)(void*, usize sibling_list, KernelStringSlice name, ...);
    void (*visit_long)(void*, usize sibling_list, KernelStringSlice name, ...);
    // ... other types
} EngineSchemaVisitor;

usize visit_schema(HandleSharedSchema, EngineSchemaVisitor*);
```

**Expression Visitor:**
```c
typedef struct EngineExpressionVisitor {
    void* data;
    usize (*make_field_list)(void*, usize);
    void (*visit_literal_long)(void*, usize sibling_list, int64_t value);
    void (*visit_add)(void*, usize sibling_list, usize child_list);
    // ... other ops
} EngineExpressionVisitor;

usize visit_expression(HandleSharedExpression, EngineExpressionVisitor*);
```

## Phase 1: EnginePlanVisitor

### C Header (ffi/include/logical_plan.h)

```c
typedef struct EnginePlanVisitor {
    void* data;  // Opaque engine context
    
    // Allocate list to hold child plans
    usize (*make_plan_list)(void* data, usize reserve);
    
    // Visit Scan node (leaf)
    void (*visit_scan)(
        void* data,
        usize sibling_list_id,
        bool is_json,
        const KernelStringSlice* file_paths,
        usize num_files,
        HandleSharedSchema schema
    );
    
    // Visit Filter node (has child + custom filter)
    void (*visit_filter)(
        void* data,
        usize sibling_list_id,
        usize child_plan_id,
        void* filter_context  // RowFilter - opaque to C++
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

### Rust Implementation (ffi/src/logical_plan.rs)

```rust
use crate::handle::Handle;
use crate::{kernel_string_slice, KernelStringSlice, SharedSchema};
use delta_kernel::kernel_df::LogicalPlanNode;
use std::os::raw::c_void;

#[handle_descriptor(target=LogicalPlanNode, mutable=false, sized=true)]
pub struct SharedLogicalPlan;

#[repr(C)]
pub struct EnginePlanVisitor {
    pub data: *mut c_void,
    pub make_plan_list: extern "C" fn(*mut c_void, usize) -> usize,
    
    pub visit_scan: extern "C" fn(
        *mut c_void,           // data
        usize,                 // sibling_list_id
        bool,                  // is_json
        *const KernelStringSlice, // file_paths
        usize,                 // num_files
        Handle<SharedSchema>,  // schema
    ),
    
    pub visit_filter: extern "C" fn(
        *mut c_void, usize, usize, *mut c_void
    ),
    
    pub visit_filter_by_expression: extern "C" fn(
        *mut c_void,
        usize,                 // sibling_list_id
        usize,                 // child_plan_id
        Handle<SharedExpression>, // predicate
    ),
    
    pub visit_parse_json: extern "C" fn(
        *mut c_void,
        usize,                 // sibling_list_id
        usize,                 // child_plan_id
        KernelStringSlice,     // json_column
        Handle<SharedSchema>,  // target_schema
        KernelStringSlice,     // output_column
    ),
    
    pub visit_select: extern "C" fn(
        *mut c_void, usize, usize, Handle<SharedSchema>
    ),
    
    pub visit_union: extern "C" fn(
        *mut c_void, usize, usize, usize
    ),
}

/// Visit a logical plan node and build engine representation
///
/// # Safety
/// Caller is responsible for passing valid plan handle and visitor
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
            // Collect file paths
            let paths: Vec<_> = scan.files.iter()
                .map(|f| kernel_string_slice!(f.location.as_str()))
                .collect();
            
            (visitor.visit_scan)(
                visitor.data,
                sibling_list_id,
                matches!(scan.scan_type, ScanType::Json),
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
            
            // Then visit this node with child list
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
        
        LogicalPlanNode::Filter(filter) => {
            let child_list_id = (visitor.make_plan_list)(visitor.data, 1);
            visit_plan_impl(&filter.child, visitor, child_list_id);
            
            // Filter context is opaque - C++ can't execute this directly
            let filter_ptr = &*filter.filter as *const _ as *mut c_void;
            (visitor.visit_filter)(
                visitor.data,
                sibling_list_id,
                child_list_id,
                filter_ptr,
            );
            sibling_list_id
        }
        
        // ... other node types
    }
}
```

## Phase 2: State Machine FFI

### C Header (ffi/include/state_machine.h)

```c
typedef struct SharedStateMachinePhase SharedStateMachinePhase;

typedef enum {
    PHASE_TYPE_TERMINUS = 0,
    PHASE_TYPE_PARTIAL_RESULT = 1,
    PHASE_TYPE_CONSUME = 2,
} PhaseType;

// Build state machine from ScanBuilder
ExternResult<HandleSharedStateMachinePhase> scan_builder_build_state_machine(
    HandleSharedSnapshot snapshot,
    HandleSharedExternEngine engine,
    EnginePredicate* predicate  // nullable
);

// Query phase type
PhaseType phase_get_type(HandleSharedStateMachinePhase phase);

// Check if parallelizable
bool phase_is_parallelizable(HandleSharedStateMachinePhase phase);

// Get plan from phase (for PartialResult/Consume)
ExternResult<HandleSharedLogicalPlan> phase_get_plan(
    HandleSharedStateMachinePhase phase,
    HandleSharedExternEngine engine
);

// Transition for PartialResult (no data needed)
ExternResult<HandleSharedStateMachinePhase> phase_next_partial(
    HandleSharedStateMachinePhase phase,
    HandleSharedExternEngine engine
);

// Transition for Consume (needs Arrow data)
ExternResult<HandleSharedStateMachinePhase> phase_next_consume(
    HandleSharedStateMachinePhase phase,
    HandleSharedExternEngine engine,
    const struct ArrowArray* array,
    const struct ArrowSchema* schema
);

// Extract Scan from terminal phase
ExternResult<HandleSharedScan> phase_get_terminal_scan(
    HandleSharedStateMachinePhase phase,
    HandleSharedExternEngine engine
);
```

### Rust Implementation (ffi/src/state_machine.rs)

```rust
use crate::handle::Handle;
use crate::{ExternResult, IntoExternResult, SharedExternEngine, SharedScan, SharedSnapshot};
use crate::expressions::kernel_visitor::{unwrap_kernel_predicate, KernelExpressionVisitorState};
use crate::scan::EnginePredicate;
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::state_machine::{StateMachinePhase, PartialResultPhase, ConsumePhase};
use delta_kernel::scan::Scan;
use delta_kernel::DeltaResult;
use std::sync::Arc;

#[handle_descriptor(target=StateMachinePhase<Scan>, mutable=false, sized=true)]
pub struct SharedStateMachinePhase;

#[repr(C)]
pub enum PhaseType {
    Terminus = 0,
    PartialResult = 1,
    Consume = 2,
}

/// Build state machine from snapshot
#[no_mangle]
pub unsafe extern "C" fn scan_builder_build_state_machine(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    predicate: Option<&mut EnginePredicate>,
) -> ExternResult<Handle<SharedStateMachinePhase>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    let engine = unsafe { engine.as_ref() };
    build_sm_impl(snapshot, predicate).into_extern_result(&engine)
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

/// Query phase type
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

/// Check if phase is parallelizable
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

/// Get plan from phase
#[no_mangle]
pub unsafe extern "C" fn phase_get_plan(
    phase: Handle<SharedStateMachinePhase>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedLogicalPlan>> {
    let phase = unsafe { phase.as_ref() };
    let engine = unsafe { engine.as_ref() };
    phase_get_plan_impl(phase).into_extern_result(&engine)
}

fn phase_get_plan_impl(
    phase: &StateMachinePhase<Scan>
) -> DeltaResult<Handle<SharedLogicalPlan>> {
    match phase {
        StateMachinePhase::PartialResult(p) => Ok(Arc::new(p.get_plan()?).into()),
        StateMachinePhase::Consume(p) => Ok(Arc::new(p.get_plan()?).into()),
        _ => Err(Error::generic("Terminal phase has no plan")),
    }
}

// ... transition functions (phase_next_partial, phase_next_consume, phase_get_terminal_scan)
```

## Phase 3: C++ Example

### Example (ffi/examples/read-table/state_machine_exec.cpp)

```cpp
#include "delta_kernel_ffi.h"
#include <vector>
#include <memory>
#include <iostream>

// C++ engine's plan representation
struct MyCppPlan {
    enum Type { SCAN, FILTER_EXPR, PARSE_JSON, SELECT, UNION };
    Type type;
    std::vector<std::unique_ptr<MyCppPlan>> children;
    
    // Type-specific data
    union {
        struct { /* scan data */ } scan;
        struct { void* predicate; } filter_expr;
        struct { /* parse json data */ } parse_json;
    };
};

// Visitor context
struct PlanVisitorContext {
    std::vector<std::vector<std::unique_ptr<MyCppPlan>>> plan_lists;
    
    usize make_list(usize reserve) {
        plan_lists.emplace_back();
        plan_lists.back().reserve(reserve);
        return plan_lists.size() - 1;
    }
    
    void add_scan(usize list_id, bool is_json, 
                  const KernelStringSlice* paths, usize num_paths,
                  void* schema) {
        auto plan = std::make_unique<MyCppPlan>();
        plan->type = MyCppPlan::SCAN;
        // ... populate from args
        plan_lists[list_id].push_back(std::move(plan));
    }
    
    void add_filter_by_expression(usize list_id, usize child_list_id, void* predicate) {
        auto plan = std::make_unique<MyCppPlan>();
        plan->type = MyCppPlan::FILTER_EXPR;
        plan->filter_expr.predicate = predicate;
        // Move child from child_list
        plan->children = std::move(plan_lists[child_list_id]);
        plan_lists[list_id].push_back(std::move(plan));
    }
    
    // ... other node types
};

// C callbacks for visitor
extern "C" {
    usize visitor_make_list(void* data, usize reserve) {
        return static_cast<PlanVisitorContext*>(data)->make_list(reserve);
    }
    
    void visitor_scan(void* data, usize list_id, bool is_json,
                     const KernelStringSlice* paths, usize num, void* schema) {
        static_cast<PlanVisitorContext*>(data)->add_scan(list_id, is_json, paths, num, schema);
    }
    
    void visitor_filter_by_expression(void* data, usize list_id, usize child, void* pred) {
        static_cast<PlanVisitorContext*>(data)->add_filter_by_expression(list_id, child, pred);
    }
    
    // ... other callbacks
}

int main(int argc, char** argv) {
    // Setup
    auto engine = /* ... */;
    auto snapshot = /* ... */;
    auto predicate = /* ... */;
    
    // Build state machine
    auto phase_result = scan_builder_build_state_machine(snapshot, engine, predicate);
    if (phase_result.tag != Ok) {
        std::cerr << "Failed to build state machine\n";
        return 1;
    }
    auto phase = phase_result.ok;
    
    std::cout << "Executing state machine phases...\n";
    
    // Execution loop
    while (phase_get_type(phase) != PHASE_TYPE_TERMINUS) {
        PhaseType type = phase_get_type(phase);
        bool parallel = phase_is_parallelizable(phase);
        
        std::cout << "Phase type: " << (int)type 
                  << ", parallelizable: " << parallel << "\n";
        
        // Get plan
        auto plan_result = phase_get_plan(phase, engine);
        if (plan_result.tag != Ok) {
            std::cerr << "Failed to get plan\n";
            return 1;
        }
        auto plan_handle = plan_result.ok;
        
        // Visit plan to build C++ representation
        PlanVisitorContext ctx;
        EnginePlanVisitor visitor = {
            &ctx,
            visitor_make_list,
            visitor_scan,
            nullptr,  // visit_filter (not needed for our example)
            visitor_filter_by_expression,
            visitor_parse_json,
            visitor_select,
            visitor_union,
        };
        
        usize root_list = visit_logical_plan(plan_handle, &visitor);
        MyCppPlan* cpp_plan = ctx.plan_lists[root_list][0].get();
        
        std::cout << "  Plan type: " << (int)cpp_plan->type << "\n";
        
        // Execute plan in C++ (custom engine logic here)
        // ArrowArray* result = my_custom_cpp_engine.execute(cpp_plan);
        
        // For now, just transition
        if (type == PHASE_TYPE_PARTIAL_RESULT) {
            auto next_result = phase_next_partial(phase, engine);
            if (next_result.tag != Ok) {
                std::cerr << "Transition failed\n";
                return 1;
            }
            phase = next_result.ok;
        } else {
            // PHASE_TYPE_CONSUME
            // Would need to pass Arrow data here
            std::cerr << "Consume phase not yet implemented\n";
            break;
        }
    }
    
    // Extract final scan
    auto scan_result = phase_get_terminal_scan(phase, engine);
    if (scan_result.tag != Ok) {
        std::cerr << "Failed to get terminal scan\n";
        return 1;
    }
    auto scan = scan_result.ok;
    
    std::cout << "State machine complete! Got scan.\n";
    
    // Use scan as normal
    // visit_scan_files(scan, ...);
    
    return 0;
}
```

## Implementation Steps

1. **Add ffi/src/logical_plan.rs**
   - Define `EnginePlanVisitor` struct
   - Implement `visit_logical_plan` function
   - Handle all node types recursively

2. **Add ffi/src/state_machine.rs**
   - Define `PhaseType` enum
   - Implement phase query functions
   - Implement transition functions
   - Add Arrow C interface conversion (if not exists)

3. **Update ffi/src/lib.rs**
   - Add `pub mod logical_plan;`
   - Add `pub mod state_machine;`
   - Export types

4. **Update ffi/examples/read-table**
   - Add `state_machine_exec.cpp` example
   - Update CMakeLists.txt
   - Add documentation

5. **Tests**
   - FFI roundtrip: Rust plan → C++ visitor → verify
   - Phase transitions: build → query → transition → terminal
   - End-to-end: Full execution loop

## Benefits

1. **C++ Control** - Engine implements custom execution
2. **Plan Inspection** - C++ can inspect/optimize before execution
3. **Consistent API** - Same visitor pattern as schema/expression
4. **Type Safety** - C++ owns memory, defines layout
5. **Streaming** - Phases enable incremental processing

## Open Questions

1. **Arrow C Interface:** Do we need to add ArrowArray/ArrowSchema conversion, or does it already exist?
2. **Filter Node:** Should we expose RowFilter to C++, or is it opaque?
3. **Expression Lists:** How to pass expression lists for Select node?
4. **Async Support:** Should transitions support async C++ execution?

## Next Actions

Once this plan is approved:
1. Implement ffi/src/logical_plan.rs
2. Implement ffi/src/state_machine.rs
3. Create C++ example
4. Write tests
5. Update documentation

---

**Status:** Ready for implementation review and approval.

