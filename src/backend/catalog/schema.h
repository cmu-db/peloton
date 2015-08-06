//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// schema.h
//
// Identification: src/backend/catalog/schema.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/catalog/column.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//

class Schema {
 public:
  //===--------------------------------------------------------------------===//
  // Static factory methods to construct schema objects
  //===--------------------------------------------------------------------===//

  // Construct schema
  void CreateTupleSchema(const std::vector<ValueType> column_types,
                         const std::vector<oid_t> column_lengths,
                         const std::vector<std::string> column_names,
                         const std::vector<bool> is_inlined);

  // Construct schema from vector of Column
  Schema(const std::vector<Column> columns);

  // Copy schema
  static Schema *CopySchema(const Schema *schema);

  // Copy subset of columns in the given schema
  static Schema *CopySchema(const Schema *schema,
                            const std::vector<oid_t> &set);

  // Append two schema objects
  static Schema *AppendSchema(Schema *first, Schema *second);

  // Append subset of columns in the two given schemas
  static Schema *AppendSchema(Schema *first, std::vector<oid_t> &first_set,
                              Schema *second, std::vector<oid_t> &second_set);

  // Append given schemas.
  static Schema *AppendSchemaList(std::vector<Schema> &schema_list);

  // Append given schemas.
  static Schema *AppendSchemaPtrList(const std::vector<Schema *> &schema_list);

  // Append subsets of columns in the given schemas.
  static Schema *AppendSchemaPtrList(
      const std::vector<Schema *> &schema_list,
      const std::vector<std::vector<oid_t>> &subsets);

  // Compare two schemas
  bool operator==(const Schema &other) const;
  bool operator!=(const Schema &other) const;

  //===--------------------------------------------------------------------===//
  // Schema accessors
  //===--------------------------------------------------------------------===//

  inline size_t GetOffset(const oid_t column_id) const {
    return columns[column_id].column_offset;
  }

  inline ValueType GetType(const oid_t column_id) const {
    return columns[column_id].column_type;
  }

  // Returns fixed length
  inline size_t GetLength(const oid_t column_id) const {
    return columns[column_id].fixed_length;
  }

  inline size_t GetVariableLength(const oid_t column_id) const {
    return columns[column_id].variable_length;
  }

  inline bool IsInlined(const oid_t column_id) const {
    return columns[column_id].is_inlined;
  }

  const Column GetColumn(const oid_t column_id) const {
    return columns[column_id];
  }

  oid_t GetUninlinedColumn(const oid_t column_id) const {
    return uninlined_columns[column_id];
  }

  std::vector<Column> GetColumns() const { return columns; }

  // Return the number of columns in the schema for the tuple.
  inline oid_t GetColumnCount() const { return column_count; }

  oid_t GetUninlinedColumnCount() const { return uninlined_column_count; }

  // Return the number of bytes used by one tuple.
  inline oid_t GetLength() const { return length; }

  // Returns a flag indicating whether all columns are inlined
  bool IsInlined() const { return tuple_is_inlined; }

  void SetIndexedColumns(const std::vector<oid_t> &indexed_columns) {
    indexed_columns_ = indexed_columns;
  }

  std::vector<oid_t> GetIndexedColumns() const { return indexed_columns_; }

  // Get the nullability of the column at a given index.
  bool AllowNull(const oid_t column_id) const {
    for (auto constraint : columns[column_id].constraints) {
      if (constraint.GetType() == CONSTRAINT_TYPE_NOTNULL) return false;
    }
    return true;
  }

  // Add constraint for column by id
  void AddConstraint(oid_t column_id, const catalog::Constraint &constraint) {
    columns[column_id].AddConstraint(constraint);
  }

  // Add constraint for column by name
  void AddConstraint(std::string column_name,
                     const catalog::Constraint &constraint) {
    for (size_t column_itr = 0; column_itr < columns.size(); column_itr++) {
      if (columns[column_itr].column_name == column_name) {
        columns[column_itr].AddConstraint(constraint);
      }
    }
  }

  // Get a string representation of this schema
  friend std::ostream &operator<<(std::ostream &os, const Schema &schema);

 private:
  // size of fixed length columns
  size_t length;

  // all inlined and uninlined columns in the tuple
  std::vector<Column> columns;

  // keeps track of unlined columns
  std::vector<oid_t> uninlined_columns;

  // keep these in sync with the vectors above
  oid_t column_count = INVALID_OID;

  oid_t uninlined_column_count = INVALID_OID;

  // are all columns inlined
  bool tuple_is_inlined;

  // keeps track of indexed columns in original table
  std::vector<oid_t> indexed_columns_;
};

}  // End catalog namespace
}  // End peloton namespace
