//! FFI bridge for protobuf-based plan/state machine communication.
//!
//! This module provides minimal FFI functions for:
//! - Serializing state machine phases to protobuf bytes
//! - Deserializing phase results from engines
//! - Advancing state machines
//!
//! Actual data transfer (batches) still uses Arrow C Data Interface.

use delta_kernel::plans::{
    AdvanceResult, AnyStateMachine, AsQueryPlan, DeclarativePlanNode, FileType, FilterByKDF,
    FilterKdfState, LogReplayPhase, LogReplayStateMachine, ScanNode, SelectNode,
    SnapshotBuildPhase, StateMachine,
};
use delta_kernel::proto_generated as proto;
use delta_kernel_ffi_macros::handle_descriptor;
use prost::Message;

use crate::handle::Handle;
use crate::IntoExternResult;

// =============================================================================
// Handle Types
// =============================================================================

/// Opaque handle to a LogReplayPhase state machine (legacy)
#[handle_descriptor(target=LogReplayPhase, mutable=true, sized=true)]
pub struct SharedLogReplayPhase;

/// Opaque handle to a SnapshotBuildPhase state machine (legacy)
#[handle_descriptor(target=SnapshotBuildPhase, mutable=true, sized=true)]
pub struct SharedSnapshotBuildPhase;

/// Opaque handle to a LogReplayStateMachine (type-specific API)
#[handle_descriptor(target=LogReplayStateMachine, mutable=true, sized=true)]
pub struct SharedLogReplayStateMachine;

/// Opaque handle to AnyStateMachine (generic API for all state machines)
#[handle_descriptor(target=AnyStateMachine, mutable=true, sized=true)]
pub struct SharedAnyStateMachine;

// =============================================================================
// Serialization Helpers
// =============================================================================

/// Result of serialization - contains pointer and length to protobuf bytes.
/// Caller must free with `free_proto_bytes`.
#[repr(C)]
pub struct ProtoBytes {
    pub ptr: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

impl ProtoBytes {
    fn from_vec(vec: Vec<u8>) -> Self {
        let mut vec = std::mem::ManuallyDrop::new(vec);
        Self {
            ptr: vec.as_mut_ptr(),
            len: vec.len(),
            capacity: vec.capacity(),
        }
    }

    fn empty() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }
}

/// Free protobuf bytes allocated by serialization functions.
///
/// # Safety
///
/// - `bytes` must have been returned by a serialization function
/// - Must only be called once per allocation
#[no_mangle]
pub unsafe extern "C" fn free_proto_bytes(bytes: ProtoBytes) {
    if !bytes.ptr.is_null() {
        drop(Vec::from_raw_parts(bytes.ptr, bytes.len, bytes.capacity));
    }
}

// =============================================================================
// LogReplayPhase FFI (Legacy)
// =============================================================================

/// Serialize a LogReplayPhase to protobuf bytes.
#[no_mangle]
pub unsafe extern "C" fn log_replay_phase_to_proto(
    phase: Handle<SharedLogReplayPhase>,
) -> ProtoBytes {
    let phase_ref = unsafe { phase.as_ref() };
    let proto_phase: proto::LogReplayPhase = phase_ref.into();
    let bytes = proto_phase.encode_to_vec();
    ProtoBytes::from_vec(bytes)
}

/// Check if a LogReplayPhase is complete.
#[no_mangle]
pub unsafe extern "C" fn log_replay_phase_is_complete(
    phase: Handle<SharedLogReplayPhase>,
) -> bool {
    let phase_ref = unsafe { phase.as_ref() };
    phase_ref.is_complete()
}

/// Get the phase name for a LogReplayPhase.
#[no_mangle]
pub unsafe extern "C" fn log_replay_phase_name(
    phase: Handle<SharedLogReplayPhase>,
) -> crate::KernelStringSlice {
    let phase_ref = unsafe { phase.as_ref() };
    let name = phase_ref.phase_name();
    crate::kernel_string_slice!(name)
}

/// Free a LogReplayPhase handle.
#[no_mangle]
pub unsafe extern "C" fn free_log_replay_phase(phase: Handle<SharedLogReplayPhase>) {
    phase.drop_handle();
}

// =============================================================================
// LogReplayStateMachine FFI (New Opaque Handle API)
// =============================================================================

/// Create a new log replay state machine for a table.
///
/// Returns an opaque handle. The engine drives via: get_plan → execute → advance → repeat.
///
/// # Safety
///
/// - `table_path` must be a valid UTF-8 string
/// - `allocate_error` must be a valid error allocation callback
#[no_mangle]
pub unsafe extern "C" fn create_log_replay_state_machine(
    table_path: crate::KernelStringSlice,
    allocate_error: crate::AllocateErrorFn,
) -> crate::ExternResult<Handle<SharedLogReplayStateMachine>> {
    create_log_replay_state_machine_impl(table_path).into_extern_result(&allocate_error)
}

fn create_log_replay_state_machine_impl(
    table_path: crate::KernelStringSlice,
) -> delta_kernel::DeltaResult<Handle<SharedLogReplayStateMachine>> {
    use crate::TryFromStringSlice;

    let path_str = unsafe { <&str>::try_from_slice(&table_path)? };
    let url = url::Url::parse(path_str)
        .map_err(|e| delta_kernel::Error::generic(format!("Invalid table path: {}", e)))?;

    let state_machine = LogReplayStateMachine::new(url);
    Ok(Handle::from(Box::new(state_machine)))
}

/// Get the current plan from the state machine.
///
/// Returns protobuf-encoded DeclarativePlanNode bytes.
/// Returns an error if the state machine is terminal.
///
/// # Safety
///
/// - `handle` must be a valid state machine handle
/// - `allocate_error` must be a valid error allocation callback
#[no_mangle]
pub unsafe extern "C" fn state_machine_get_plan(
    handle: Handle<SharedLogReplayStateMachine>,
    allocate_error: crate::AllocateErrorFn,
) -> crate::ExternResult<ProtoBytes> {
    state_machine_get_plan_impl(handle).into_extern_result(&allocate_error)
}

fn state_machine_get_plan_impl(
    handle: Handle<SharedLogReplayStateMachine>,
) -> delta_kernel::DeltaResult<ProtoBytes> {
    let sm = unsafe { handle.as_ref() };
    let plan = sm.get_plan()?;
    let proto_plan: proto::DeclarativePlanNode = (&plan).into();
    Ok(ProtoBytes::from_vec(proto_plan.encode_to_vec()))
}

/// Check if the state machine has reached a terminal state.
#[no_mangle]
pub unsafe extern "C" fn state_machine_is_terminal(
    handle: Handle<SharedLogReplayStateMachine>,
) -> bool {
    let sm = unsafe { handle.as_ref() };
    sm.is_terminal()
}

/// Get the current phase name for debugging.
#[no_mangle]
pub unsafe extern "C" fn state_machine_phase_name(
    handle: Handle<SharedLogReplayStateMachine>,
) -> crate::KernelStringSlice {
    let sm = unsafe { handle.as_ref() };
    let name = sm.phase_name();
    crate::kernel_string_slice!(name)
}

/// Advance the state machine with the executed plan result.
///
/// Returns true if the state machine is now terminal (complete), false if more phases remain.
///
/// # Safety
///
/// - `handle` must be a valid state machine handle
/// - `plan_bytes_ptr` must point to valid protobuf-encoded DeclarativePlanNode bytes
/// - Pass NULL/0 for plan_bytes if execution failed
/// - `allocate_error` must be a valid error allocation callback
#[no_mangle]
pub unsafe extern "C" fn state_machine_advance(
    handle: Handle<SharedLogReplayStateMachine>,
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
    allocate_error: crate::AllocateErrorFn,
) -> crate::ExternResult<bool> {
    state_machine_advance_impl(handle, plan_bytes_ptr, plan_bytes_len)
        .into_extern_result(&allocate_error)
}

fn state_machine_advance_impl(
    mut handle: Handle<SharedLogReplayStateMachine>,
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
) -> delta_kernel::DeltaResult<bool> {
    // Decode the executed plan from protobuf bytes
    let plan_result: delta_kernel::DeltaResult<DeclarativePlanNode> =
        if plan_bytes_ptr.is_null() || plan_bytes_len == 0 {
            Err(delta_kernel::Error::generic("Plan execution failed"))
        } else {
            let plan_bytes = unsafe { std::slice::from_raw_parts(plan_bytes_ptr, plan_bytes_len) };
            let proto_plan = proto::DeclarativePlanNode::decode(plan_bytes)
                .map_err(|e| delta_kernel::Error::generic(format!("Failed to decode plan: {}", e)))?;
            convert_proto_to_native_plan(&proto_plan)
        };

    let sm = unsafe { handle.as_mut() };
    let result = sm.advance(plan_result)?;

    Ok(result.is_done())
}

/// Free a state machine handle.
#[no_mangle]
pub unsafe extern "C" fn state_machine_free(handle: Handle<SharedLogReplayStateMachine>) {
    handle.drop_handle();
}

// =============================================================================
// AnyStateMachine FFI (Generic API)
// =============================================================================

/// Convert a LogReplayStateMachine to AnyStateMachine for generic handling.
#[no_mangle]
pub unsafe extern "C" fn log_replay_sm_into_any(
    handle: Handle<SharedLogReplayStateMachine>,
) -> Handle<SharedAnyStateMachine> {
    let sm = unsafe { handle.into_inner() };
    let any_sm = AnyStateMachine::LogReplay(*sm);
    Handle::from(Box::new(any_sm))
}

/// Get the current plan from any state machine (generic).
#[no_mangle]
pub unsafe extern "C" fn any_sm_get_plan(
    handle: Handle<SharedAnyStateMachine>,
    allocate_error: crate::AllocateErrorFn,
) -> crate::ExternResult<ProtoBytes> {
    any_sm_get_plan_impl(handle).into_extern_result(&allocate_error)
}

fn any_sm_get_plan_impl(
    handle: Handle<SharedAnyStateMachine>,
) -> delta_kernel::DeltaResult<ProtoBytes> {
    let sm = unsafe { handle.as_ref() };
    let plan = sm.get_plan()?;
    let proto_plan: proto::DeclarativePlanNode = (&plan).into();
    Ok(ProtoBytes::from_vec(proto_plan.encode_to_vec()))
}

/// Get operation type from any state machine.
#[no_mangle]
pub unsafe extern "C" fn any_sm_operation_type(handle: Handle<SharedAnyStateMachine>) -> i32 {
    let sm = unsafe { handle.as_ref() };
    match sm.operation_type() {
        delta_kernel::plans::OperationType::LogReplay => 1,
        delta_kernel::plans::OperationType::SnapshotBuild => 2,
        delta_kernel::plans::OperationType::Scan => 3,
    }
}

/// Get phase name from any state machine (generic).
#[no_mangle]
pub unsafe extern "C" fn any_sm_phase_name(
    handle: Handle<SharedAnyStateMachine>,
) -> crate::KernelStringSlice {
    let sm = unsafe { handle.as_ref() };
    let name = sm.phase_name();
    crate::kernel_string_slice!(name)
}

/// Advance any state machine with executed plan (generic).
///
/// Returns true if the state machine is now terminal (complete), false if more phases remain.
#[no_mangle]
pub unsafe extern "C" fn any_sm_advance(
    handle: Handle<SharedAnyStateMachine>,
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
    allocate_error: crate::AllocateErrorFn,
) -> crate::ExternResult<bool> {
    any_sm_advance_impl(handle, plan_bytes_ptr, plan_bytes_len).into_extern_result(&allocate_error)
}

fn any_sm_advance_impl(
    mut handle: Handle<SharedAnyStateMachine>,
    plan_bytes_ptr: *const u8,
    plan_bytes_len: usize,
) -> delta_kernel::DeltaResult<bool> {
    let plan_result = if plan_bytes_ptr.is_null() || plan_bytes_len == 0 {
        Err(delta_kernel::Error::generic("Plan execution failed"))
    } else {
        let plan_bytes = unsafe { std::slice::from_raw_parts(plan_bytes_ptr, plan_bytes_len) };
        let proto_plan = proto::DeclarativePlanNode::decode(plan_bytes)
            .map_err(|e| delta_kernel::Error::generic(format!("Failed to decode plan: {}", e)))?;
        convert_proto_to_native_plan(&proto_plan)
    };

    let sm = unsafe { handle.as_mut() };
    let result = sm.advance(plan_result)?;
    Ok(result.is_done())
}

/// Free any state machine handle (generic).
#[no_mangle]
pub unsafe extern "C" fn any_sm_free(handle: Handle<SharedAnyStateMachine>) {
    handle.drop_handle();
}

// =============================================================================
// SnapshotBuildPhase FFI (Legacy)
// =============================================================================

/// Serialize a SnapshotBuildPhase to protobuf bytes.
#[no_mangle]
pub unsafe extern "C" fn snapshot_build_phase_to_proto(
    phase: Handle<SharedSnapshotBuildPhase>,
) -> ProtoBytes {
    let phase_ref = unsafe { phase.as_ref() };

    use proto::snapshot_build_phase::Phase;
    let proto_phase = match phase_ref {
        SnapshotBuildPhase::ListFiles(p) => proto::SnapshotBuildPhase {
            phase: Some(Phase::ListFiles(proto::FileListingPhaseData {
                plan: None,
                query_plan: Some((&p.as_query_plan()).into()),
            })),
        },
        SnapshotBuildPhase::LoadMetadata(p) => proto::SnapshotBuildPhase {
            phase: Some(Phase::LoadMetadata(proto::MetadataLoadData {
                plan: None,
                query_plan: Some((&p.as_query_plan()).into()),
            })),
        },
        SnapshotBuildPhase::Ready(data) => proto::SnapshotBuildPhase {
            phase: Some(Phase::Ready(proto::SnapshotReady {
                version: data.version,
                table_schema: None,
            })),
        },
    };

    ProtoBytes::from_vec(proto_phase.encode_to_vec())
}

/// Check if a SnapshotBuildPhase is ready.
#[no_mangle]
pub unsafe extern "C" fn snapshot_build_phase_is_ready(
    phase: Handle<SharedSnapshotBuildPhase>,
) -> bool {
    unsafe { phase.as_ref() }.is_ready()
}

/// Get the phase name for a SnapshotBuildPhase.
#[no_mangle]
pub unsafe extern "C" fn snapshot_build_phase_name(
    phase: Handle<SharedSnapshotBuildPhase>,
) -> crate::KernelStringSlice {
    let name = unsafe { phase.as_ref() }.phase_name();
    crate::kernel_string_slice!(name)
}

/// Free a SnapshotBuildPhase handle.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot_build_phase(phase: Handle<SharedSnapshotBuildPhase>) {
    phase.drop_handle();
}

// =============================================================================
// KDF (Kernel-Defined Function) FFI
// =============================================================================
//
// State is always created in Rust (by state machines). Java holds only opaque u64 handles.
// The state handle encodes both function identity and state - no separate function_id parameter.

/// Serialize KDF state for distribution.
///
/// # Safety
///
/// - `state_ptr` must have been created by Rust via `FilterKdfState::into_raw()`
#[no_mangle]
pub unsafe extern "C" fn kdf_serialize(state_ptr: u64) -> ProtoBytes {
    if state_ptr == 0 {
        return ProtoBytes::empty();
    }
    
    let state = FilterKdfState::borrow_from_raw(state_ptr);
    match state.serialize() {
        Ok(bytes) => ProtoBytes::from_vec(bytes),
        Err(_) => ProtoBytes::empty(),
    }
}

/// Free KDF state.
///
/// # Safety
///
/// - `state_ptr` must have been created by Rust via `FilterKdfState::into_raw()`
/// - Must not be called more than once for the same pointer
#[no_mangle]
pub unsafe extern "C" fn kdf_free(state_ptr: u64) {
    FilterKdfState::free_raw(state_ptr);
}

/// Apply a KDF filter to a batch of data.
///
/// The state handle encodes both function identity and state - no separate function_id.
/// State is always created in Rust; this function borrows it for the apply operation.
///
/// Follows the Arrow C Data Interface pattern: Java allocates FFI structs and passes
/// their memory addresses. Rust reads inputs and writes output via pointers.
///
/// # Arguments
/// - `state_ptr`: Opaque handle to typed Rust state (from state machine plan)
/// - `input_batch_ptr`: Pointer to FFI_ArrowArray for input batch
/// - `input_schema_ptr`: Pointer to FFI_ArrowSchema for input batch schema
/// - `selection_array_ptr`: Pointer to FFI_ArrowArray for input selection
/// - `selection_schema_ptr`: Pointer to FFI_ArrowSchema for input selection schema
/// - `output_array_ptr`: Pointer to pre-allocated FFI_ArrowArray for output (Rust fills this)
/// - `output_schema_ptr`: Pointer to pre-allocated FFI_ArrowSchema for output (Rust fills this)
/// - `allocate_error`: Error allocation callback
///
/// # Memory Ownership Semantics
///
/// This function follows the Arrow C Data Interface specification with explicit ownership transfer:
///
/// **Input Arrays and Schemas (all 4 inputs):**
/// - Rust takes ownership via `std::ptr::read()`, which moves the data out of the caller's memory
/// - For arrays: `ffi::from_ffi()` consumes the array and will invoke the release callback when
///   the resulting ArrayData is dropped
/// - For schemas: We explicitly `drop()` after `from_ffi()` because `from_ffi()` only borrows
///   the schema reference. Without explicit drop, the schema's release callback would never fire,
///   causing memory leaks on the Java side.
///
/// **Output Structs:**
/// - Java pre-allocates empty FFI structs at output_array_ptr and output_schema_ptr
/// - Rust writes result data via `std::ptr::write()`, which copies the FFI struct bytes
/// - The result_data is kept alive via `std::mem::forget()` because the FFI structs reference it
/// - Java imports the result, triggering the release callback when the vector is closed
///
/// # Safety
///
/// - `state_ptr` must have been created by Rust via `FilterKdfState::into_raw()`
/// - All `*_ptr` arguments must be valid pointers to properly allocated Arrow C Data structs
/// - Input structs must contain valid exported Arrow data with proper release callbacks
/// - Output structs must be pre-allocated with sufficient space
/// - `allocate_error` must be a valid error allocator function
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn kdf_apply(
    state_ptr: u64,
    input_batch_ptr: usize,
    input_schema_ptr: usize,
    selection_array_ptr: usize,
    selection_schema_ptr: usize,
    output_array_ptr: usize,
    output_schema_ptr: usize,
    allocate_error: crate::AllocateErrorFn,
) -> crate::ExternResult<()> {
    kdf_apply_impl(
        state_ptr,
        input_batch_ptr,
        input_schema_ptr,
        selection_array_ptr,
        selection_schema_ptr,
        output_array_ptr,
        output_schema_ptr,
    )
    .into_extern_result(&allocate_error)
}

#[cfg(feature = "default-engine-base")]
fn kdf_apply_impl(
    state_ptr: u64,
    input_batch_ptr: usize,
    input_schema_ptr: usize,
    selection_array_ptr: usize,
    selection_schema_ptr: usize,
    output_array_ptr: usize,
    output_schema_ptr: usize,
) -> delta_kernel::DeltaResult<()> {
    use delta_kernel::arrow::array::{Array, BooleanArray, RecordBatch, StructArray};
    use delta_kernel::arrow::ffi::{self, FFI_ArrowArray, FFI_ArrowSchema};
    use delta_kernel::engine::arrow_data::ArrowEngineData;

    if state_ptr == 0 {
        return Err(delta_kernel::Error::generic("state_ptr must be non-zero"));
    }

    // Get typed state from raw pointer - ONE match here, then monomorphized execution
    let state = unsafe { FilterKdfState::borrow_mut_from_raw(state_ptr) };

    // Reinterpret pointers as Arrow FFI structs
    let input_batch_ptr = input_batch_ptr as *mut FFI_ArrowArray;
    let input_schema_ptr = input_schema_ptr as *mut FFI_ArrowSchema;
    let selection_array_ptr = selection_array_ptr as *mut FFI_ArrowArray;
    let selection_schema_ptr = selection_schema_ptr as *mut FFI_ArrowSchema;
    let output_array_ptr = output_array_ptr as *mut FFI_ArrowArray;
    let output_schema_ptr = output_schema_ptr as *mut FFI_ArrowSchema;

    // Take ownership of input batch AND schema (std::ptr::read moves the data out)
    let input_batch = unsafe { std::ptr::read(input_batch_ptr) };
    let input_schema = unsafe { std::ptr::read(input_schema_ptr) };

    // Import batch from C Data Interface
    let array_data = unsafe { ffi::from_ffi(input_batch, &input_schema)? };
    let record_batch = RecordBatch::from(StructArray::from(array_data));
    let engine_data = ArrowEngineData::new(record_batch);
    
    // Drop the schema now that we're done with it - this calls its release callback
    drop(input_schema);

    // Take ownership of selection input AND schema
    let selection_array = unsafe { std::ptr::read(selection_array_ptr) };
    let selection_schema = unsafe { std::ptr::read(selection_schema_ptr) };
    let sel_array_data = unsafe { ffi::from_ffi(selection_array, &selection_schema)? };
    let sel_array = BooleanArray::from(sel_array_data);
    
    // Drop the selection schema now that we're done with it
    drop(selection_schema);

    // Apply the filter - direct dispatch via enum, monomorphized execution
    let result_array = state.apply(&engine_data, sel_array)?;

    // Export result to Arrow FFI structs and write to output pointers
    let result_data = result_array.into_data();
    let (out_array, out_schema) = ffi::to_ffi(&result_data)?;

    // Write output to pre-allocated structs via pointers
    unsafe {
        std::ptr::write(output_array_ptr, out_array);
        std::ptr::write(output_schema_ptr, out_schema);
    }

    // Prevent result_data from being dropped - the FFI structs now own
    // references to this data via the release callback mechanism
    std::mem::forget(result_data);

    Ok(())
}

// =============================================================================
// Proto Conversion Helpers
// =============================================================================

/// Convert protobuf plan to native Rust plan.
///
/// For FilterByKDF, the state_ptr must be non-zero since state is always created in Rust.
fn convert_proto_to_native_plan(
    proto: &proto::DeclarativePlanNode,
) -> delta_kernel::DeltaResult<DeclarativePlanNode> {
    use std::sync::Arc;

    let empty_schema = Arc::new(delta_kernel::schema::StructType::new_unchecked(vec![]));

    match &proto.node {
        Some(proto::declarative_plan_node::Node::Scan(scan)) => {
            Ok(DeclarativePlanNode::Scan(ScanNode {
                file_type: if scan.file_type == proto::scan_node::FileType::Parquet as i32 {
                    FileType::Parquet
                } else {
                    FileType::Json
                },
                files: vec![],
                schema: empty_schema,
            }))
        }
        Some(proto::declarative_plan_node::Node::FilterByKdf(filter_plan)) => {
            let child = filter_plan
                .child
                .as_ref()
                .ok_or_else(|| delta_kernel::Error::generic("FilterByKDF missing child"))?;
            let child_plan = convert_proto_to_native_plan(child)?;

            let filter_node = filter_plan
                .node
                .as_ref()
                .ok_or_else(|| delta_kernel::Error::generic("FilterByKDF missing node"))?;

            // state_ptr must be non-zero - state is always created in Rust
            if filter_node.state_ptr == 0 {
                return Err(delta_kernel::Error::generic(
                    "FilterByKDF state_ptr must be non-zero - state is always created in Rust"
                ));
            }

            // Reconstruct typed state from raw pointer
            let state = unsafe { FilterKdfState::from_raw(filter_node.state_ptr) };

            Ok(DeclarativePlanNode::FilterByKDF {
                child: Box::new(child_plan),
                node: FilterByKDF { state },
            })
        }
        Some(proto::declarative_plan_node::Node::Select(select_plan)) => {
            let child = select_plan
                .child
                .as_ref()
                .ok_or_else(|| delta_kernel::Error::generic("Select missing child"))?;
            let child_plan = convert_proto_to_native_plan(child)?;

            Ok(DeclarativePlanNode::Select {
                child: Box::new(child_plan),
                node: SelectNode {
                    columns: vec![],
                    output_schema: empty_schema,
                },
            })
        }
        _ => Err(delta_kernel::Error::generic("Unsupported plan node type")),
    }
}

// =============================================================================
// Test Functions for FFI Testing
// =============================================================================

use delta_kernel::expressions::column_expr;
use delta_kernel::plans::*;
use delta_kernel::schema::{DataType, StructField, StructType};
use std::sync::Arc;

/// Create a sample LogReplayPhase (Commit phase) for testing.
#[no_mangle]
pub extern "C" fn create_test_log_replay_phase() -> ProtoBytes {
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("path", DataType::STRING, false),
        StructField::new("size", DataType::LONG, true),
    ]));

    let commit_plan = CommitPhasePlan {
        scan: ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: schema.clone(),
        },
        data_skipping: None,
        partition_prune_filter: None,
        dedup_filter: FilterByKDF::add_remove_dedup(),
        project: SelectNode {
            columns: vec![
                Arc::new(column_expr!("path")),
                Arc::new(column_expr!("size")),
            ],
            output_schema: schema,
        },
        sink: SinkNode::results(),
    };

    let phase = LogReplayPhase::Commit(commit_plan);
    let proto_phase: proto::LogReplayPhase = (&phase).into();
    ProtoBytes::from_vec(proto_phase.encode_to_vec())
}

/// Create a sample DeclarativePlanNode tree for testing.
#[no_mangle]
pub extern "C" fn create_test_declarative_plan() -> ProtoBytes {
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("json_col", DataType::STRING, true),
    ]));

    let parsed_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("parsed", DataType::STRING, true),
    ]));

    let scan = DeclarativePlanNode::Scan(ScanNode {
        file_type: FileType::Parquet,
        files: vec![],
        schema,
    });

    let parse_json = DeclarativePlanNode::ParseJson {
        child: Box::new(scan),
        node: ParseJsonNode {
            json_column: "json_col".to_string(),
            target_schema: parsed_schema,
            output_column: "parsed".to_string(),
        },
    };

    let filter = DeclarativePlanNode::FilterByKDF {
        child: Box::new(parse_json),
        node: FilterByKDF::add_remove_dedup(),
    };

    let proto_plan: proto::DeclarativePlanNode = (&filter).into();
    ProtoBytes::from_vec(proto_plan.encode_to_vec())
}

/// Create a complete LogReplayPhase for testing.
#[no_mangle]
pub extern "C" fn create_test_complete_phase() -> ProtoBytes {
    let phase = LogReplayPhase::Complete;
    let proto_phase: proto::LogReplayPhase = (&phase).into();
    ProtoBytes::from_vec(proto_phase.encode_to_vec())
}
