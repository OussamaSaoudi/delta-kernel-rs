#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "delta_kernel_ffi.h"
#include "plan.h"

// Forward declarations for visitor construction
size_t make_plan_list(void* data, uintptr_t reserve);
PlanItemList construct_logical_plan(SharedLogicalPlan* plan);

// Helper to extract filter from a FilterPlan
SharedRowFilter* extract_filter_from_plan(SharedLogicalPlan* plan) {
    PlanItemList root = construct_logical_plan(plan);
    
    // The test plan structure is: Filter -> Scan
    assert(root.len == 1);
    assert(root.list[0].type == Filter);
    
    struct FilterPlan* filter_plan = (struct FilterPlan*)root.list[0].ref;
    return filter_plan->filter;
}

// Helper to create Arrow batch with id column
void create_arrow_batch_with_ids(
    FFI_ArrowArray* array,
    FFI_ArrowSchema* schema,
    const int32_t* ids,
    int num_rows
) {
    // Schema: struct with one field "id" (int32)
    schema->format = "+s";
    schema->name = "";
    schema->metadata = NULL;
    schema->flags = 0;
    schema->n_children = 1;
    schema->children = (FFI_ArrowSchema**)malloc(sizeof(FFI_ArrowSchema*));
    schema->dictionary = NULL;
    schema->release = NULL;
    
    // Child: id field
    FFI_ArrowSchema* id_schema = (FFI_ArrowSchema*)malloc(sizeof(FFI_ArrowSchema));
    id_schema->format = "i"; // int32
    id_schema->name = "id";
    id_schema->metadata = NULL;
    id_schema->flags = 0; // non-nullable
    id_schema->n_children = 0;
    id_schema->children = NULL;
    id_schema->dictionary = NULL;
    id_schema->release = NULL;
    schema->children[0] = id_schema;
    
    // Array: struct array
    array->length = num_rows;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 1;
    array->n_children = 1;
    array->buffers = (const void**)malloc(sizeof(void*));
    array->buffers[0] = NULL; // No null bitmap for struct
    array->children = (FFI_ArrowArray**)malloc(sizeof(FFI_ArrowArray*));
    array->dictionary = NULL;
    array->release = NULL;
    
    // Child: id array
    FFI_ArrowArray* id_array = (FFI_ArrowArray*)malloc(sizeof(FFI_ArrowArray));
    id_array->length = num_rows;
    id_array->null_count = 0;
    id_array->offset = 0;
    id_array->n_buffers = 2;
    id_array->n_children = 0;
    id_array->buffers = (const void**)malloc(2 * sizeof(void*));
    id_array->buffers[0] = NULL; // No null bitmap
    
    // Copy id values
    int32_t* id_values = (int32_t*)malloc(num_rows * sizeof(int32_t));
    memcpy(id_values, ids, num_rows * sizeof(int32_t));
    id_array->buffers[1] = id_values;
    id_array->children = NULL;
    id_array->dictionary = NULL;
    id_array->release = NULL;
    array->children[0] = id_array;
}

// Helper to read boolean selection vector
void read_bool_array(FFI_ArrowArray* array, bool* output, int num_rows) {
    assert(array->length == num_rows);
    const uint8_t* data = (const uint8_t*)array->buffers[1];
    for (int i = 0; i < num_rows; i++) {
        output[i] = (data[i / 8] & (1 << (i % 8))) != 0;
    }
}

// Helper to create a pre-selection vector
void create_selection_vector(
    FFI_ArrowArray* array,
    const bool* values,
    int num_rows
) {
    array->length = num_rows;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 2;
    array->n_children = 0;
    array->buffers = (const void**)malloc(2 * sizeof(void*));
    array->buffers[0] = NULL; // No null bitmap
    
    // Allocate and fill boolean values (bit-packed)
    int num_bytes = (num_rows + 7) / 8;
    uint8_t* bool_data = (uint8_t*)calloc(num_bytes, 1);
    for (int i = 0; i < num_rows; i++) {
        if (values[i]) {
            bool_data[i / 8] |= (1 << (i % 8));
        }
    }
    array->buffers[1] = bool_data;
    array->children = NULL;
    array->dictionary = NULL;
    array->release = NULL;
}

int main() {
    printf("========================================\n");
    printf("Row Filter FFI Functional Test\n");
    printf("========================================\n\n");
    
    // Test 1: Extract filter from plan
    printf("Test 1: Extract filter handle from plan\n");
    SharedLogicalPlan* plan = get_testing_filter_plan();
    assert(plan != NULL);
    
    SharedRowFilter* filter = extract_filter_from_plan(plan);
    assert(filter != NULL);
    printf("  ✓ Successfully extracted filter handle\n");
    
    // Test 2: Query filter schema
    printf("\nTest 2: Query filter schema\n");
    SharedSchema* filter_schema = row_filter_get_schema(filter);
    assert(filter_schema != NULL);
    printf("  ✓ Filter schema retrieved\n");
    printf("  (Schema should contain column 'id' of type INTEGER)\n");
    
    // Test 3: Apply filter without pre-selection
    printf("\nTest 3: Apply filter to test data (id > 5)\n");
    const int num_rows = 10;
    int32_t test_ids[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    
    FFI_ArrowArray input_array;
    FFI_ArrowSchema input_schema;
    create_arrow_batch_with_ids(&input_array, &input_schema, test_ids, num_rows);
    printf("  Created test batch with %d rows (id: 0-9)\n", num_rows);
    
    FFI_ArrowArray output_array;
    memset(&output_array, 0, sizeof(FFI_ArrowArray));
    
    bool success = apply_row_filter(filter, &input_array, &input_schema, NULL, &output_array);
    assert(success);
    printf("  ✓ Filter applied successfully\n");
    
    // Read and verify output
    bool selection_vector[10];
    read_bool_array(&output_array, selection_vector, num_rows);
    
    printf("  Selection vector: [");
    for (int i = 0; i < num_rows; i++) {
        printf("%s", selection_vector[i] ? "T" : "F");
        if (i < num_rows - 1) printf(", ");
    }
    printf("]\n");
    
    // Verify: rows 0-5 should be FALSE (id <= 5), rows 6-9 should be TRUE (id > 5)
    int expected_true_count = 0;
    for (int i = 0; i < num_rows; i++) {
        bool expected = test_ids[i] > 5;
        if (expected) expected_true_count++;
        assert(selection_vector[i] == expected);
    }
    printf("  ✓ Selection vector correct: %d rows passed filter (id > 5)\n", expected_true_count);
    
    // Test 4: Apply filter with pre-selection vector
    printf("\nTest 4: Apply filter with pre-selection vector\n");
    // Pre-select only even indices: 0, 2, 4, 6, 8
    bool pre_selection[] = {true, false, true, false, true, false, true, false, true, false};
    FFI_ArrowArray pre_sel_array;
    create_selection_vector(&pre_sel_array, pre_selection, num_rows);
    
    printf("  Pre-selection: [");
    for (int i = 0; i < num_rows; i++) {
        printf("%s", pre_selection[i] ? "T" : "F");
        if (i < num_rows - 1) printf(", ");
    }
    printf("] (even indices only)\n");
    
    FFI_ArrowArray output_array2;
    memset(&output_array2, 0, sizeof(FFI_ArrowArray));
    
    success = apply_row_filter(filter, &input_array, &input_schema, &pre_sel_array, &output_array2);
    assert(success);
    printf("  ✓ Filter with pre-selection applied successfully\n");
    
    bool selection_vector2[10];
    read_bool_array(&output_array2, selection_vector2, num_rows);
    
    printf("  Final selection: [");
    for (int i = 0; i < num_rows; i++) {
        printf("%s", selection_vector2[i] ? "T" : "F");
        if (i < num_rows - 1) printf(", ");
    }
    printf("]\n");
    
    // Verify: should be TRUE only where pre_selection[i] AND test_ids[i] > 5
    // That's indices 6 and 8 only
    int final_true_count = 0;
    for (int i = 0; i < num_rows; i++) {
        bool expected = pre_selection[i] && test_ids[i] > 5;
        if (expected) final_true_count++;
        assert(selection_vector2[i] == expected);
    }
    printf("  ✓ Combined selection correct: %d rows passed (pre-selected AND id > 5)\n", final_true_count);
    assert(final_true_count == 2); // Only indices 6 and 8
    
    // Test 5: Edge cases
    printf("\nTest 5: Edge cases\n");
    
    // All values fail filter (id = 0-4)
    int32_t all_fail_ids[] = {0, 1, 2, 3, 4};
    FFI_ArrowArray fail_array;
    FFI_ArrowSchema fail_schema;
    create_arrow_batch_with_ids(&fail_array, &fail_schema, all_fail_ids, 5);
    
    FFI_ArrowArray output_fail;
    memset(&output_fail, 0, sizeof(FFI_ArrowArray));
    success = apply_row_filter(filter, &fail_array, &fail_schema, NULL, &output_fail);
    assert(success);
    
    bool fail_selection[5];
    read_bool_array(&output_fail, fail_selection, 5);
    int fail_count = 0;
    for (int i = 0; i < 5; i++) {
        assert(fail_selection[i] == false);
        if (!fail_selection[i]) fail_count++;
    }
    printf("  ✓ All-fail case: %d/5 rows rejected correctly\n", fail_count);
    
    // All values pass filter (id = 10-14)
    int32_t all_pass_ids[] = {10, 11, 12, 13, 14};
    FFI_ArrowArray pass_array;
    FFI_ArrowSchema pass_schema;
    create_arrow_batch_with_ids(&pass_array, &pass_schema, all_pass_ids, 5);
    
    FFI_ArrowArray output_pass;
    memset(&output_pass, 0, sizeof(FFI_ArrowArray));
    success = apply_row_filter(filter, &pass_array, &pass_schema, NULL, &output_pass);
    assert(success);
    
    bool pass_selection[5];
    read_bool_array(&output_pass, pass_selection, 5);
    int pass_count = 0;
    for (int i = 0; i < 5; i++) {
        assert(pass_selection[i] == true);
        if (pass_selection[i]) pass_count++;
    }
    printf("  ✓ All-pass case: %d/5 rows accepted correctly\n", pass_count);
    
    // Cleanup
    free_row_filter(filter);
    free_schema(filter_schema);
    free_logical_plan(plan);
    
    printf("\n========================================\n");
    printf("✓ ALL TESTS PASSED!\n");
    printf("========================================\n");
    printf("\nFunctional correctness verified:\n");
    printf("  - Filter handle extraction from plan\n");
    printf("  - Schema query\n");
    printf("  - Basic filter application (id > 5)\n");
    printf("  - Pre-selection vector combination\n");
    printf("  - Edge cases (all pass/fail)\n");
    printf("\nThe row filter FFI is fully functional!\n");
    
    return 0;
}
