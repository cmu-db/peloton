//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// raw_structure.h
//
// Identification: src/backend/bridge/ddl/raw_structure.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"

#include "postgres.h"
#include "nodes/nodes.h"

//===--------------------------------------------------------------------===//
// DDL raw data structures
//===--------------------------------------------------------------------===//

struct DDL_Info {
  NodeTag type;
};

// Used by CreateDb, DropDb
struct Database_Info : public DDL_Info {
  Oid database_oid;
};

// Used by CreateStmt, AlterTableStmt, IndexStmt
struct Relation_Info : public DDL_Info {
  Oid relation_oid;
};

struct Type_Info : public DDL_Info {
  Oid type_oid;
  int type_len;
};

struct raw_database_info;
struct raw_table_info;
struct raw_index_info;
struct raw_foreign_key_info;
struct raw_column_info;
struct raw_constraint_info;

struct raw_database_info {
  Oid database_oid;
  char *database_name;

  raw_table_info **raw_tables;
  raw_index_info **raw_indexes;
  raw_foreign_key_info **raw_foreignkeys;

  int table_count;
  int index_count;
  int foreignkey_count;
};

struct raw_table_info {
  Oid table_oid;
  char *table_name;

  // Column information
  raw_column_info **raw_columns;
  int column_count;
};

struct raw_index_info {
  char *index_name;
  Oid index_oid;

  char *table_name;

  peloton::IndexType method_type;
  peloton::IndexConstraintType constraint_type;

  bool unique_keys;

  char **key_column_names;
  int key_column_count;
};

struct raw_foreign_key_info {
  Oid source_table_id;  // a table that has a reference key
  Oid sink_table_id;    // a table that has a primary key

  int *source_column_offsets;
  int source_column_count;

  int *sink_column_offsets;
  int sink_column_count;

  // Refer:: http://www.postgresql.org/docs/9.4/static/catalog-pg-constraint.html
  // foreign key action
  char update_action;
  char delete_action;

  char *fk_name;
};


struct raw_column_info {
  peloton::ValueType column_type;

  size_t column_length;
  char *column_name;

  bool is_inlined;

  // Constraint information
  raw_constraint_info **raw_constraints;
  int constraint_count;
};

struct raw_constraint_info {
  peloton::ConstraintType constraint_type;
  char *constraint_name;

  // Cooked/transformed constraint expression
  Node *expr;
};

