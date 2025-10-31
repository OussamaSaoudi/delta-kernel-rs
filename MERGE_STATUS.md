# Merge Status - State Machine + ParseJson

## Current Situation

**Merge:** Attempting to merge main into state_machine branch
**Build Status:** ❌ 23 compilation errors remaining
**Test Status:** Not yet testable

## What Was Working Before Merge

✅ State machine core (33 lines)
✅ Scan phases with StateInfo integration
✅ CommitDedupFilter and CheckpointDedupFilter  
✅ ScanBuilder::build_state_machine() API
✅ ParseJsonNode plan node
✅ Bug fix for BorrowMutError
✅ 2 functional tests passing:
   - `proof_state_machine_matches_current_implementation`
   - `test_parse_json_functional`

## API Changes from Master Merge

### Fixed ✅

1. ✅ `Scan` struct now uses `StateInfo` (fixed in phases.rs and mod.rs)
2. ✅ `StateInfo::try_new()` signature changed (4 params now)
3. ✅ `StructType::new()` → `try_new()` or `new_unchecked()` (mostly fixed)
4. ✅ `schema.nested_field()` removed → implemented manual path walking
5. ✅ `DataType::struct_type()` removed → use `DataType::Struct(Box::new(...))`
6. ✅ `get_log_schema()` → `get_commit_schema()`
7. ✅ `Snapshot::builder()` → `SnapshotBuilder::new()` (user fixed)
8. ✅ `checkpoint::log_replay` moved to `action_reconciliation`
9. ✅ `RefCell` in AddRemoveDedupVisitor (is_log_batch access fixed)

### Remaining Issues ⏸️

**Main Library:**
- Some StructType::new() calls in expressions and kernel_df (minor)
- CHECKPOINT/COMMIT_READ_SCHEMA visibility issue in kernel_df imports

**Test Code:** 
- StructType::new() → try_new() in all test code
- Snapshot API changes in tests
- Various test-specific fixes

## Estimated Remaining Work

**To get library compiling:** ~10-15 fixes
**To get tests passing:** ~20-30 more fixes (mechanical, just API updates)

## Files Modified (My Changes)

1. kernel/src/state_machine.rs - NEW
2. kernel/src/scan/phases.rs - NEW (mostly clean, 23 errors from tests)
3. kernel/src/kernel_df.rs - Enhanced (few errors)
4. kernel/src/scan/mod.rs - API added (few errors)
5. kernel/src/scan/log_replay.rs - Bug fixed + RefCell updates
6. kernel/src/expressions/mod.rs - ParseJson added (few errors)
7. kernel/src/lib.rs - Module added
8. kernel/src/async_df.rs - ParseJson case added

## Next Steps

1. Fix remaining 23 compilation errors (mostly mechanical)
2. Run tests to verify functionality after merge
3. Re-enable ignored tests once column merging works

## Key Question

Should I:
1. Continue fixing all 23 errors systematically?
2. Focus on getting just the main lib compiling (skip test fixes for now)?
3. Create a clean branch and reapply changes?

**I need guidance on priority given the volume of merge conflicts.**

