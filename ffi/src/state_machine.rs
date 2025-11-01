//! FFI interface for Delta Kernel state machines.
//!
//! This module exposes a unified state machine pattern to C/C++/Java clients, allowing
//! them to drive snapshot construction and scan execution with a single set of functions.

use std::any::Any;
use std::sync::Arc;

use delta_kernel::arrow::ffi;
use delta_kernel::arrow::ffi_stream::FFI_ArrowArrayStream;
use delta_kernel::state_machine::StateMachinePhase;
use delta_kernel::DeltaResult;
use delta_kernel_ffi_macros::handle_descriptor;

use crate::handle::Handle;
use crate::logical_plan::SharedLogicalPlan;
use crate::SharedSnapshot;

/// Opaque handle to a Scan
#[handle_descriptor(target=delta_kernel::scan::Scan, mutable=false, sized=true)]
pub struct SharedScan;

/// Internal wrapper for type-erased state machine phases
/// 
/// This allows us to store StateMachinePhase<Snapshot> or StateMachinePhase<Scan>
/// in a single handle type without exposing the generic parameter to FFI.
pub struct SharedPhaseInner {
    inner: Box<dyn Any + Send + Sync>,
    terminus_type: &'static str, // "snapshot" or "scan"
}

/// Opaque handle to any state machine phase (type-erased)
/// 
/// Can represent either a Snapshot or Scan state machine phase.
/// Use `phase_type()` to determine if it's Terminus/PartialResult/Consume.
#[handle_descriptor(target=SharedPhaseInner, mutable=true, sized=true)]
pub struct SharedPhase;

/// Phase type discriminant for state machines
#[repr(C)]
pub enum PhaseType {
    /// Terminal phase with final result
    Terminus = 0,
    /// Partial result phase (yields streaming data)
    PartialResult = 1,
    /// Consume phase (needs data from engine)
    Consume = 2,
}

// =============================================================================
// Generic Helper Functions (work for any StateMachinePhase<T>)
// =============================================================================

/// Generic function to get phase type from any StateMachinePhase<T>
fn get_phase_type<T>(phase: &StateMachinePhase<T>) -> PhaseType {
    match phase {
        StateMachinePhase::Terminus(_) => PhaseType::Terminus,
        StateMachinePhase::PartialResult(_) => PhaseType::PartialResult,
        StateMachinePhase::Consume(_) => PhaseType::Consume,
    }
}

/// Generic function to get plan from any non-Terminus phase
fn get_phase_plan<T>(phase: &StateMachinePhase<T>) -> Handle<SharedLogicalPlan>
where
    T: Send + Sync + 'static,
{
    match phase {
        StateMachinePhase::Consume(cp) => {
            let plan = cp.get_plan().expect("get_plan failed");
            Arc::new(plan).into()
        }
        StateMachinePhase::PartialResult(pp) => {
            let plan = pp.get_plan().expect("get_plan failed");
            Arc::new(plan).into()
        }
        StateMachinePhase::Terminus(_) => {
            panic!("Cannot get plan from Terminus phase");
        }
    }
}

/// Generic function to advance a Consume phase
fn advance_consume_phase<T>(
    phase: StateMachinePhase<T>,
    batches: Vec<DeltaResult<delta_kernel::arrow::array::RecordBatch>>,
    terminus_type: &'static str,
) -> Handle<SharedPhase>
where
    T: Send + Sync + 'static,
{
    let consume_phase = match phase {
        StateMachinePhase::Consume(cp) => cp,
        _ => panic!("Phase is not a Consume phase"),
    };

    let next_phase = consume_phase
        .next(Box::new(batches.into_iter()))
        .expect("ConsumePhase::next failed");

    let new_inner = SharedPhaseInner {
        inner: Box::new(next_phase),
        terminus_type,
    };

    Box::new(new_inner).into()
}

/// Generic function to advance a PartialResult phase
fn advance_partial_result_phase<T>(
    phase: StateMachinePhase<T>,
    terminus_type: &'static str,
) -> Handle<SharedPhase>
where
    T: Send + Sync + 'static,
{
    let partial_phase = match phase {
        StateMachinePhase::PartialResult(pp) => pp,
        _ => panic!("Phase is not a PartialResult phase"),
    };

    let next_phase = partial_phase
        .next()
        .expect("PartialResultPhase::next failed");

    let new_inner = SharedPhaseInner {
        inner: Box::new(next_phase),
        terminus_type,
    };

    Box::new(new_inner).into()
}

// =============================================================================
// Generic Phase Functions (work for both Snapshot and Scan)
// =============================================================================

/// Get the phase type of any state machine phase
///
/// # Safety
///
/// - `phase` must be a valid handle to a SharedPhase
///
/// # Returns
///
/// The discriminant indicating if this is Terminus, PartialResult, or Consume phase.
#[no_mangle]
pub unsafe extern "C" fn phase_type(phase: Handle<SharedPhase>) -> PhaseType {
    let phase_inner = unsafe { phase.as_ref() };
    
    if phase_inner.terminus_type == "snapshot" {
        let phase = phase_inner.inner.downcast_ref::<StateMachinePhase<delta_kernel::snapshot::Snapshot>>()
            .expect("Failed to downcast to Snapshot phase");
        get_phase_type(phase)
    } else if phase_inner.terminus_type == "scan" {
        let phase = phase_inner.inner.downcast_ref::<StateMachinePhase<delta_kernel::scan::Scan>>()
            .expect("Failed to downcast to Scan phase");
        get_phase_type(phase)
    } else {
        panic!("Unknown terminus type: {}", phase_inner.terminus_type);
    }
}

/// Get the logical plan from any non-Terminus phase
///
/// # Safety
///
/// - `phase` must be a valid handle to a SharedPhase
/// - The phase must NOT be a Terminus phase (check with `phase_type` first)
///
/// # Returns
///
/// A handle to the logical plan that must be executed by the engine.
/// If the phase is Terminus, behavior is undefined (will panic).
///
/// # Panics
///
/// Panics if the phase is Terminus or if get_plan() fails.
#[no_mangle]
pub unsafe extern "C" fn phase_get_plan(phase: Handle<SharedPhase>) -> Handle<SharedLogicalPlan> {
    let phase_inner = unsafe { phase.as_ref() };

    if phase_inner.terminus_type == "snapshot" {
        let phase = phase_inner.inner.downcast_ref::<StateMachinePhase<delta_kernel::snapshot::Snapshot>>()
            .expect("Failed to downcast to Snapshot phase");
        get_phase_plan(phase)
    } else if phase_inner.terminus_type == "scan" {
        let phase = phase_inner.inner.downcast_ref::<StateMachinePhase<delta_kernel::scan::Scan>>()
            .expect("Failed to downcast to Scan phase");
        get_phase_plan(phase)
    } else {
        panic!("Unknown terminus type: {}", phase_inner.terminus_type);
    }
}

/// Advance a Consume phase by providing data from the engine via Arrow C Stream Interface
///
/// # Safety
///
/// - `phase` must be a valid handle to a Consume phase (will be consumed/freed)
/// - `data_stream` must be a valid pointer to a FFI_ArrowArrayStream
/// - The phase must be of type Consume
///
/// # Returns
///
/// A new phase handle representing the next state.
///
/// # Note
///
/// This function consumes the input `phase` handle. Do not use it after calling this function.
/// The Arrow stream will be consumed and released by this function.
#[no_mangle]
pub unsafe extern "C" fn consume_phase_next(
    phase: Handle<SharedPhase>,
    data_stream: *mut FFI_ArrowArrayStream,
) -> Handle<SharedPhase> {
    let phase_inner = phase.into_inner();
    let terminus_type = phase_inner.terminus_type;

    // Take ownership of the Arrow stream
    let mut stream = unsafe { FFI_ArrowArrayStream::from_raw(data_stream) };

    // Convert stream to RecordBatch iterator
    let mut batches = Vec::new();
    loop {
        let mut array = ffi::FFI_ArrowArray::empty();
        let mut schema = ffi::FFI_ArrowSchema::empty();

        // Get schema on first iteration
        if batches.is_empty() {
            let schema_result = unsafe {
                (stream.get_schema.unwrap())(
                    &mut stream as *mut FFI_ArrowArrayStream,
                    &mut schema as *mut ffi::FFI_ArrowSchema,
                )
            };
            if schema_result != 0 {
                panic!("Failed to get schema from Arrow stream");
            }
        }

        // Get next batch
        let result = unsafe {
            (stream.get_next.unwrap())(
                &mut stream as *mut FFI_ArrowArrayStream,
                &mut array as *mut ffi::FFI_ArrowArray,
            )
        };

        if result != 0 {
            panic!("Failed to get next batch from Arrow stream");
        }

        // Check if stream is exhausted
        let array_data_result =
            unsafe { delta_kernel::arrow::array::ffi::from_ffi(array, &schema) };

        let array_data = match array_data_result {
            Ok(data) if data.len() > 0 => data,
            _ => break, // Stream exhausted or empty batch
        };

        let record_batch: delta_kernel::arrow::array::RecordBatch =
            delta_kernel::arrow::array::StructArray::from(array_data).into();

        batches.push(Ok(record_batch));
    }

    // Process based on terminus type
    let terminus_type = phase_inner.terminus_type;
    
    if terminus_type == "snapshot" {
        let phase = *phase_inner.inner.downcast::<StateMachinePhase<delta_kernel::snapshot::Snapshot>>()
            .expect("Failed to downcast to Snapshot phase");
        advance_consume_phase(phase, batches, terminus_type)
    } else if terminus_type == "scan" {
        let phase = *phase_inner.inner.downcast::<StateMachinePhase<delta_kernel::scan::Scan>>()
            .expect("Failed to downcast to Scan phase");
        advance_consume_phase(phase, batches, terminus_type)
    } else {
        panic!("Unknown terminus type: {}", terminus_type);
    }
}

/// Advance a PartialResult phase to the next phase
///
/// # Safety
///
/// - `phase` must be a valid handle to a PartialResult phase (will be consumed/freed)
/// - The phase must be of type PartialResult
///
/// # Returns
///
/// A new phase handle representing the next state.
///
/// # Note
///
/// This function consumes the input `phase` handle. Do not use it after calling this function.
#[no_mangle]
pub unsafe extern "C" fn partial_result_phase_next(
    phase: Handle<SharedPhase>,
) -> Handle<SharedPhase> {
    let phase_inner = phase.into_inner();
    let terminus_type = phase_inner.terminus_type;
    
    if terminus_type == "snapshot" {
        let phase = *phase_inner.inner.downcast::<StateMachinePhase<delta_kernel::snapshot::Snapshot>>()
            .expect("Failed to downcast to Snapshot phase");
        advance_partial_result_phase(phase, terminus_type)
    } else if terminus_type == "scan" {
        let phase = *phase_inner.inner.downcast::<StateMachinePhase<delta_kernel::scan::Scan>>()
            .expect("Failed to downcast to Scan phase");
        advance_partial_result_phase(phase, terminus_type)
    } else {
        panic!("Unknown terminus type: {}", terminus_type);
    }
}

/// Free a phase handle
///
/// # Safety
///
/// - `phase` must be a valid handle to a phase
#[no_mangle]
pub unsafe extern "C" fn free_phase(phase: Handle<SharedPhase>) {
    phase.drop_handle();
}

// =============================================================================
// Snapshot-Specific Functions
// =============================================================================

/// Create a snapshot state machine from a table path
///
/// # Safety
///
/// - `path` must be a valid KernelStringSlice
///
/// # Returns
///
/// The initial phase of the snapshot state machine (typically a ConsumePhase).
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_into_state_machine(
    path: crate::KernelStringSlice,
) -> Handle<SharedPhase> {
    let path_str = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(path.ptr as *const u8, path.len))
    };
    let url = url::Url::parse(path_str).expect("Invalid path URL");

    let builder = delta_kernel::Snapshot::builder_for(url);

    let initial_phase = builder
        .into_state_machine()
        .expect("Failed to create state machine");

    let phase_inner = SharedPhaseInner {
        inner: Box::new(initial_phase),
        terminus_type: "snapshot",
    };
    
    Box::new(phase_inner).into()
}

/// Get the Snapshot from a Terminus phase
///
/// # Safety
///
/// - `phase` must be a valid handle to a Terminus phase containing a Snapshot
/// - The phase must be of type Terminus
///
/// # Panics
///
/// Panics if the phase is not a Terminus phase or not a Snapshot phase.
#[no_mangle]
pub unsafe extern "C" fn snapshot_get_from_terminus(
    phase: Handle<SharedPhase>,
) -> Handle<SharedSnapshot> {
    let phase_inner = phase.into_inner();

    if phase_inner.terminus_type != "snapshot" {
        panic!("Not a snapshot phase");
    }

    let snapshot_phase = phase_inner
        .inner
        .downcast::<StateMachinePhase<delta_kernel::snapshot::Snapshot>>()
        .expect("Failed to downcast to Snapshot phase");

    match *snapshot_phase {
        StateMachinePhase::Terminus(snapshot) => Arc::new(snapshot).into(),
        _ => panic!("Phase is not a Terminus phase"),
    }
}

// =============================================================================
// Scan-Specific Functions
// =============================================================================

/// Create a scan state machine from a snapshot
///
/// # Safety
///
/// - `snapshot` must be a valid handle to a Snapshot
///
/// # Returns
///
/// The initial phase of the scan state machine (typically a PartialResultPhase for CommitReplay).
///
/// # Note
///
/// This function consumes the snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn scan_builder_into_state_machine(
    snapshot: Handle<SharedSnapshot>,
) -> Handle<SharedPhase> {
    let snapshot_arc: Arc<delta_kernel::Snapshot> = snapshot.into_inner();

    let builder = delta_kernel::scan::ScanBuilder::new(snapshot_arc);

    let initial_phase = builder
        .build_state_machine()
        .expect("Failed to create scan state machine");

    let phase_inner = SharedPhaseInner {
        inner: Box::new(initial_phase),
        terminus_type: "scan",
    };
    
    Box::new(phase_inner).into()
}

/// Get the Scan from a Terminus phase
///
/// # Safety
///
/// - `phase` must be a valid handle to a Terminus phase containing a Scan
/// - The phase must be of type Terminus
///
/// # Panics
///
/// Panics if the phase is not a Terminus phase or not a Scan phase.
#[no_mangle]
pub unsafe extern "C" fn scan_get_from_terminus(phase: Handle<SharedPhase>) -> Handle<SharedScan> {
    let phase_inner = phase.into_inner();

    if phase_inner.terminus_type != "scan" {
        panic!("Not a scan phase");
    }

    let scan_phase = phase_inner
        .inner
        .downcast::<StateMachinePhase<delta_kernel::scan::Scan>>()
        .expect("Failed to downcast to Scan phase");

    match *scan_phase {
        StateMachinePhase::Terminus(scan) => Arc::new(scan).into(),
        _ => panic!("Phase is not a Terminus phase"),
    }
}

// Note: free_scan() is already defined in scan.rs, so we don't redefine it here
