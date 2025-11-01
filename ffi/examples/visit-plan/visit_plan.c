#include "delta_kernel_ffi.h"
#include "plan.h"
#include "plan_print.h"

int main() {
  // Get a test logical plan from the FFI
  HandleSharedLogicalPlan plan = get_testing_logical_plan();
  
  // Construct the C representation using the visitor
  PlanItemList plan_list = construct_logical_plan(plan);
  
  // Print the plan
  print_plan(plan_list);
  
  // Verify we got the expected complex structure
  if (plan_list.len != 1) {
    printf("ERROR: Expected 1 plan item, got %u\n", plan_list.len);
    return 1;
  }
  
  PlanItem root = plan_list.list[0];
  
  // Root should be a Union
  if (root.type != Union) {
    printf("ERROR: Expected Union node at root, got type %d\n", root.type);
    return 1;
  }
  
  struct UnionPlan* union_plan = root.ref;
  printf("\n✓ Root is Union node\n");
  
  // Left branch should have Select at top
  if (union_plan->left.len != 1 || union_plan->left.list[0].type != Select) {
    printf("ERROR: Expected Select in left branch\n");
    return 1;
  }
  printf("✓ Left branch starts with Select\n");
  
  struct SelectPlan* select = union_plan->left.list[0].ref;
  
  // Select should have FilterByExpression child
  if (select->child.len != 1 || select->child.list[0].type != FilterByExpression) {
    printf("ERROR: Expected FilterByExpression as child of Select\n");
    return 1;
  }
  printf("✓ Select contains FilterByExpression\n");
  
  struct FilterByExpressionPlan* filter_expr = select->child.list[0].ref;
  
  // FilterByExpression should have ParseJson child
  if (filter_expr->child.len != 1 || filter_expr->child.list[0].type != ParseJson) {
    printf("ERROR: Expected ParseJson as child of FilterByExpression\n");
    return 1;
  }
  printf("✓ FilterByExpression contains ParseJson\n");
  
  struct ParseJsonPlan* parse = filter_expr->child.list[0].ref;
  
  // Verify ParseJson fields
  if (strcmp(parse->json_column, "stats") != 0) {
    printf("ERROR: Expected json_column='stats', got '%s'\n", parse->json_column);
    return 1;
  }
  if (strcmp(parse->output_column, "parsed_stats") != 0) {
    printf("ERROR: Expected output_column='parsed_stats', got '%s'\n", parse->output_column);
    return 1;
  }
  printf("✓ ParseJson has correct column names (stats -> parsed_stats)\n");
  
  // ParseJson should have Scan child with Parquet files
  if (parse->child.len != 1 || parse->child.list[0].type != Scan) {
    printf("ERROR: Expected Scan as child of ParseJson\n");
    return 1;
  }
  
  struct ScanPlan* left_scan = parse->child.list[0].ref;
  if (left_scan->file_type != Parquet) {
    printf("ERROR: Expected Parquet file type in left scan\n");
    return 1;
  }
  if (left_scan->num_files != 2) {
    printf("ERROR: Expected 2 files in left scan, got %u\n", left_scan->num_files);
    return 1;
  }
  printf("✓ Left Scan has 2 Parquet files\n");
  
  // Right branch should have FirstNonNull at top
  if (union_plan->right.len != 1 || union_plan->right.list[0].type != FirstNonNull) {
    printf("ERROR: Expected FirstNonNull in right branch\n");
    return 1;
  }
  printf("✓ Right branch starts with FirstNonNull\n");
  
  struct FirstNonNullPlan* fnn = union_plan->right.list[0].ref;
  
  // Verify FirstNonNull columns
  if (fnn->num_columns != 2) {
    printf("ERROR: Expected 2 columns in FirstNonNull, got %u\n", fnn->num_columns);
    return 1;
  }
  if (strcmp(fnn->column_names[0], "id") != 0 || strcmp(fnn->column_names[1], "name") != 0) {
    printf("ERROR: Unexpected column names in FirstNonNull\n");
    return 1;
  }
  printf("✓ FirstNonNull has correct columns [id, name]\n");
  
  // FirstNonNull should have Scan child with JSON file
  if (fnn->child.len != 1 || fnn->child.list[0].type != Scan) {
    printf("ERROR: Expected Scan as child of FirstNonNull\n");
    return 1;
  }
  
  struct ScanPlan* right_scan = fnn->child.list[0].ref;
  if (right_scan->file_type != Json) {
    printf("ERROR: Expected Json file type in right scan\n");
    return 1;
  }
  if (right_scan->num_files != 1) {
    printf("ERROR: Expected 1 file in right scan, got %u\n", right_scan->num_files);
    return 1;
  }
  if (strcmp(right_scan->file_paths[0], "file:///path/to/file3.json") != 0) {
    printf("ERROR: Unexpected file path in right scan\n");
    return 1;
  }
  printf("✓ Right Scan has 1 JSON file\n");
  
  printf("\n");
  printf("========================================\n");
  printf("✓ ALL ASSERTIONS PASSED!\n");
  printf("========================================\n");
  printf("Successfully reconstructed complex plan:\n");
  printf("  - Union node at root\n");
  printf("  - Left: Select -> FilterByExpression -> ParseJson -> Scan(2 Parquet)\n");
  printf("  - Right: FirstNonNull -> Scan(1 JSON)\n");
  printf("  - Total node types tested: 6 (Union, Select, FilterByExpression, ParseJson, FirstNonNull, Scan)\n");
  printf("========================================\n");
  
  // Cleanup
  free_plan_list(plan_list);
  free_logical_plan(plan);
  
  return 0;
}

