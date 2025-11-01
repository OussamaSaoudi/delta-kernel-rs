#pragma once

#include "delta_kernel_ffi.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/**
 * This module defines a simple model of a logical plan, used to verify
 * that the plan visitor correctly reconstructs the plan on the C++ side.
 */

/*************************************************************
 * Data Types
 ************************************************************/
enum PlanType {
  Scan,
  Filter,
  Select,
  Union,
  DataVisitor,
  ParseJson,
  FilterByExpression,
  FileListing,
  FirstNonNull,
  Unknown
};

enum FileType {
  Parquet = 0,
  Json = 1,
};

typedef struct {
  void* ref;
  enum PlanType type;
} PlanItem;

typedef struct {
  uint32_t len;
  PlanItem* list;
} PlanItemList;

struct ScanPlan {
  enum FileType file_type;
  char** file_paths;
  uint32_t num_files;
  HandleSharedSchema schema;
};

struct FilterPlan {
  PlanItemList child;
  void* filter_context;  // Opaque
};

struct SelectPlan {
  PlanItemList child;
  HandleSharedSchema output_schema;
};

struct UnionPlan {
  PlanItemList left;
  PlanItemList right;
};

struct DataVisitorPlan {
  PlanItemList child;
  void* visitor_context;  // Opaque
};

struct FilterPlan {
  PlanItemList child;
  HandleSharedRowFilter filter;  // Row filter handle
};

struct ParseJsonPlan {
  PlanItemList child;
  char* json_column;
  HandleSharedSchema target_schema;
  char* output_column;
};

struct FilterByExpressionPlan {
  PlanItemList child;
  HandleSharedPredicate predicate;
};

struct FileListingPlan {
  char* path;
};

struct FirstNonNullPlan {
  PlanItemList child;
  char** column_names;
  uint32_t num_columns;
};

typedef struct {
  size_t list_count;
  PlanItemList* lists;
} PlanBuilder;

/*************************************************************
 * Utility functions
 ************************************************************/
void put_plan_item(void* data, size_t sibling_list_id, void* ref, enum PlanType type) {
  PlanBuilder* data_ptr = (PlanBuilder*)data;
  PlanItem plan = { .ref = ref, .type = type };
  PlanItemList* list = &data_ptr->lists[sibling_list_id];
  list->list[list->len++] = plan;
}

PlanItemList get_plan_list(void* data, size_t list_id) {
  PlanBuilder* data_ptr = (PlanBuilder*)data;
  assert(list_id < data_ptr->list_count);
  return data_ptr->lists[list_id];
}

// utility to turn a slice into a char*
char* allocate_string(const KernelStringSlice slice) {
  return strndup(slice.ptr, slice.len);
}

/*************************************************************
 * Visitor Implementations
 ************************************************************/
void visit_plan_scan(
    void* data,
    uintptr_t sibling_list_id,
    uint8_t file_type,
    const KernelStringSlice* file_paths,
    uintptr_t num_files,
    HandleSharedSchema schema)
{
  struct ScanPlan* scan = malloc(sizeof(struct ScanPlan));
  scan->file_type = file_type;
  scan->num_files = num_files;
  scan->file_paths = malloc(sizeof(char*) * num_files);
  for (size_t i = 0; i < num_files; i++) {
    scan->file_paths[i] = allocate_string(file_paths[i]);
  }
  scan->schema = schema;
  put_plan_item(data, sibling_list_id, scan, Scan);
}

void visit_plan_filter(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    HandleSharedRowFilter filter)
{
  struct FilterPlan* filter_plan = malloc(sizeof(struct FilterPlan));
  filter_plan->child = get_plan_list(data, child_plan_id);
  filter_plan->filter = filter;
  put_plan_item(data, sibling_list_id, filter_plan, Filter);
}

void visit_plan_select(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    HandleSharedSchema output_schema)
{
  struct SelectPlan* select = malloc(sizeof(struct SelectPlan));
  select->child = get_plan_list(data, child_plan_id);
  select->output_schema = output_schema;
  put_plan_item(data, sibling_list_id, select, Select);
}

void visit_plan_union(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t left_plan_id,
    uintptr_t right_plan_id)
{
  struct UnionPlan* union_plan = malloc(sizeof(struct UnionPlan));
  union_plan->left = get_plan_list(data, left_plan_id);
  union_plan->right = get_plan_list(data, right_plan_id);
  put_plan_item(data, sibling_list_id, union_plan, Union);
}

void visit_plan_data_visitor(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    void* visitor_context)
{
  struct DataVisitorPlan* dv = malloc(sizeof(struct DataVisitorPlan));
  dv->child = get_plan_list(data, child_plan_id);
  dv->visitor_context = visitor_context;
  put_plan_item(data, sibling_list_id, dv, DataVisitor);
}

void visit_plan_parse_json(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    KernelStringSlice json_column,
    HandleSharedSchema target_schema,
    KernelStringSlice output_column)
{
  struct ParseJsonPlan* parse = malloc(sizeof(struct ParseJsonPlan));
  parse->child = get_plan_list(data, child_plan_id);
  parse->json_column = allocate_string(json_column);
  parse->target_schema = target_schema;
  parse->output_column = allocate_string(output_column);
  put_plan_item(data, sibling_list_id, parse, ParseJson);
}

void visit_plan_filter_by_expression(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    HandleSharedPredicate predicate)
{
  struct FilterByExpressionPlan* filter = malloc(sizeof(struct FilterByExpressionPlan));
  filter->child = get_plan_list(data, child_plan_id);
  filter->predicate = predicate;
  put_plan_item(data, sibling_list_id, filter, FilterByExpression);
}

void visit_plan_file_listing(
    void* data,
    uintptr_t sibling_list_id,
    KernelStringSlice path)
{
  struct FileListingPlan* listing = malloc(sizeof(struct FileListingPlan));
  listing->path = allocate_string(path);
  put_plan_item(data, sibling_list_id, listing, FileListing);
}

void visit_plan_first_non_null(
    void* data,
    uintptr_t sibling_list_id,
    uintptr_t child_plan_id,
    const KernelStringSlice* column_names,
    uintptr_t num_columns)
{
  struct FirstNonNullPlan* fnn = malloc(sizeof(struct FirstNonNullPlan));
  fnn->child = get_plan_list(data, child_plan_id);
  fnn->num_columns = num_columns;
  fnn->column_names = malloc(sizeof(char*) * num_columns);
  for (size_t i = 0; i < num_columns; i++) {
    fnn->column_names[i] = allocate_string(column_names[i]);
  }
  put_plan_item(data, sibling_list_id, fnn, FirstNonNull);
}

/*************************************************************
 * EnginePlanVisitor Implementation
 ************************************************************/
uintptr_t make_plan_list(void* data, uintptr_t reserve) {
  PlanBuilder* builder = data;
  int id = builder->list_count;
  builder->list_count++;
  builder->lists = realloc(builder->lists, sizeof(PlanItemList) * builder->list_count);
  PlanItem* list = reserve? calloc(reserve, sizeof(PlanItem)) : NULL;
  builder->lists[id].len = 0;
  builder->lists[id].list = list;
  return id;
}

PlanItemList construct_logical_plan(HandleSharedLogicalPlan plan) {
  PlanBuilder data = { 0 };
  make_plan_list(&data, 0); // list id 0 is the invalid/missing/empty list

  EnginePlanVisitor visitor = {
    .data = &data,
    .make_plan_list = make_plan_list,
    .visit_scan = visit_plan_scan,
    .visit_filter = visit_plan_filter,
    .visit_select = visit_plan_select,
    .visit_union = visit_plan_union,
    .visit_data_visitor = visit_plan_data_visitor,
    .visit_parse_json = visit_plan_parse_json,
    .visit_filter_by_expression = visit_plan_filter_by_expression,
    .visit_file_listing = visit_plan_file_listing,
    .visit_first_non_null = visit_plan_first_non_null,
  };
  
  uintptr_t top_level_id = visit_logical_plan(plan, &visitor);
  PlanItemList top_level_plan = data.lists[top_level_id];
  free(data.lists);
  return top_level_plan;
}

/*************************************************************
 * Cleanup
 ************************************************************/
void free_plan_list(PlanItemList list);

void free_plan_item(PlanItem item) {
  switch (item.type) {
    case Scan: {
      struct ScanPlan* scan = item.ref;
      for (uint32_t i = 0; i < scan->num_files; i++) {
        free(scan->file_paths[i]);
      }
      free(scan->file_paths);
      free_schema(scan->schema);
      free(scan);
      break;
    }
    case Filter: {
      struct FilterPlan* filter = item.ref;
      free_plan_list(filter->child);
      free(filter);
      break;
    }
    case Select: {
      struct SelectPlan* select = item.ref;
      free_plan_list(select->child);
      free_schema(select->output_schema);
      free(select);
      break;
    }
    case Union: {
      struct UnionPlan* union_plan = item.ref;
      free_plan_list(union_plan->left);
      free_plan_list(union_plan->right);
      free(union_plan);
      break;
    }
    case DataVisitor: {
      struct DataVisitorPlan* dv = item.ref;
      free_plan_list(dv->child);
      free(dv);
      break;
    }
    case ParseJson: {
      struct ParseJsonPlan* parse = item.ref;
      free_plan_list(parse->child);
      free(parse->json_column);
      free_schema(parse->target_schema);
      free(parse->output_column);
      free(parse);
      break;
    }
    case FilterByExpression: {
      struct FilterByExpressionPlan* filter = item.ref;
      free_plan_list(filter->child);
      free_kernel_predicate(filter->predicate);
      free(filter);
      break;
    }
    case FileListing: {
      struct FileListingPlan* listing = item.ref;
      free(listing->path);
      free(listing);
      break;
    }
    case FirstNonNull: {
      struct FirstNonNullPlan* fnn = item.ref;
      free_plan_list(fnn->child);
      for (uint32_t i = 0; i < fnn->num_columns; i++) {
        free(fnn->column_names[i]);
      }
      free(fnn->column_names);
      free(fnn);
      break;
    }
    case Unknown:
      break;
  }
}

void free_plan_list(PlanItemList list) {
  for (size_t i = 0; i < list.len; i++) {
    free_plan_item(list.list[i]);
  }
  free(list.list);
}

