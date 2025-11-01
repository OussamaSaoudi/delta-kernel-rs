#pragma once

#include "plan.h"
#include <stdio.h>

void print_plan_item(PlanItem item, int indent);

void print_indent(int indent) {
  for (int i = 0; i < indent; i++) {
    printf("  ");
  }
}

void print_plan_list(PlanItemList list, int indent) {
  for (size_t i = 0; i < list.len; i++) {
    print_plan_item(list.list[i], indent);
  }
}

void print_plan_item(PlanItem item, int indent) {
  print_indent(indent);
  
  switch (item.type) {
    case Scan: {
      struct ScanPlan* scan = item.ref;
      printf("Scan(\n");
      print_indent(indent + 1);
      printf("file_type: %s,\n", scan->file_type == Parquet ? "Parquet" : "Json");
      print_indent(indent + 1);
      printf("files: [\n");
      for (uint32_t i = 0; i < scan->num_files; i++) {
        print_indent(indent + 2);
        printf("\"%s\",\n", scan->file_paths[i]);
      }
      print_indent(indent + 1);
      printf("],\n");
      print_indent(indent + 1);
      printf("schema: <schema_handle>\n");
      print_indent(indent);
      printf(")\n");
      break;
    }
    case Filter: {
      struct FilterPlan* filter = item.ref;
      printf("Filter(\n");
      print_indent(indent + 1);
      printf("child:\n");
      print_plan_list(filter->child, indent + 2);
      print_indent(indent + 1);
      printf("filter: <opaque>\n");
      print_indent(indent);
      printf(")\n");
      break;
    }
    case Select: {
      struct SelectPlan* select = item.ref;
      printf("Select(\n");
      print_indent(indent + 1);
      printf("child:\n");
      print_plan_list(select->child, indent + 2);
      print_indent(indent + 1);
      printf("output_schema: <schema_handle>\n");
      print_indent(indent);
      printf(")\n");
      break;
    }
    case Union: {
      struct UnionPlan* union_plan = item.ref;
      printf("Union(\n");
      print_indent(indent + 1);
      printf("left:\n");
      print_plan_list(union_plan->left, indent + 2);
      print_indent(indent + 1);
      printf("right:\n");
      print_plan_list(union_plan->right, indent + 2);
      print_indent(indent);
      printf(")\n");
      break;
    }
    case ParseJson: {
      struct ParseJsonPlan* parse = item.ref;
      printf("ParseJson(\n");
      print_indent(indent + 1);
      printf("child:\n");
      print_plan_list(parse->child, indent + 2);
      print_indent(indent + 1);
      printf("json_column: \"%s\",\n", parse->json_column);
      print_indent(indent + 1);
      printf("output_column: \"%s\",\n", parse->output_column);
      print_indent(indent + 1);
      printf("target_schema: <schema_handle>\n");
      print_indent(indent);
      printf(")\n");
      break;
    }
    case FileListing: {
      struct FileListingPlan* listing = item.ref;
      printf("FileListing(\n");
      print_indent(indent + 1);
      printf("path: \"%s\"\n", listing->path);
      print_indent(indent);
      printf(")\n");
      break;
    }
    case FirstNonNull: {
      struct FirstNonNullPlan* fnn = item.ref;
      printf("FirstNonNull(\n");
      print_indent(indent + 1);
      printf("child:\n");
      print_plan_list(fnn->child, indent + 2);
      print_indent(indent + 1);
      printf("columns: [");
      for (uint32_t i = 0; i < fnn->num_columns; i++) {
        printf("\"%s\"", fnn->column_names[i]);
        if (i < fnn->num_columns - 1) printf(", ");
      }
      printf("]\n");
      print_indent(indent);
      printf(")\n");
      break;
    }
    default:
      printf("<unknown_plan_type>\n");
      break;
  }
}

void print_plan(PlanItemList plan) {
  printf("=== Logical Plan ===\n");
  print_plan_list(plan, 0);
  printf("====================\n");
}

