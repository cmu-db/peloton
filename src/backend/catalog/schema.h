/*-------------------------------------------------------------------------
 *
 * tuple_schema.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/tuple_schema.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "backend/common/types.h"
#include "backend/catalog/column.h"

#include "nodes/nodes.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//

class Schema : public CatalogObject 	{

 public:

  // Construct schema from vector of ColumnInfo
  Schema(oid_t schema_oid,
         std::string schema_name,
         CatalogObject *parent,
         CatalogObject *root,
         const std::vector<Column> columns);

  //===--------------------------------------------------------------------===//
  // Static factory methods to construct schema objects
  //===--------------------------------------------------------------------===//

  // Copy schema
  static Schema *CopySchema(const Schema *schema);

  // Copy subset of columns in the given schema
  static Schema *CopySchema(
      const Schema *schema,
      const std::vector<oid_t>& set);

  // Append two schema objects
  static Schema *AppendSchema(Schema *first, Schema *second);

  // Append subset of columns in the two given schemas
  static Schema *AppendSchema(
      Schema *first,
      std::vector<oid_t>& first_set,
      Schema *second,
      std::vector<oid_t>& second_set);

  // Append given schemas.
  static Schema *AppendSchemaList(std::vector<Schema> &schema_list);

  // Append given schemas.
  static Schema *AppendSchemaPtrList(const std::vector<Schema *> &schema_list);

  // Append subsets of columns in the given schemas.
  static Schema *AppendSchemaPtrList(
      const std::vector<Schema *> &schema_list,
      const std::vector<std::vector<oid_t> > &subsets);

  // Compare two schemas
  bool operator== (const Schema &other) const;
  bool operator!= (const Schema &other) const;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  size_t GetOffset(const oid_t column_id) const {
    Column *column = GetChild(column_id);
    return column->GetOffset();
  }

  ValueType GetType(const oid_t column_id) const {
    Column *column = GetChild(column_id);
    return column->GetType();
  }

  // Returns fixed length
  size_t GetLength(const oid_t column_id) const {
    Column *column = GetChild(column_id);
    return column->GetFixedLength();
  }

  size_t GetVariableLength(const oid_t column_id) const {
    Column *column = GetChild(column_id);
    return column->GetVariableLength();
  }

  bool IsInlined(const oid_t column_id) const {
    Column *column = GetChild(column_id);
    return column->IsInlined();
  }

  const Column GetColumn(const oid_t column_id) const {
    Column *column = GetChild(column_id);
    // Make a copy here
    return *column;
  }

  // Offset based on list of uninlined columns
  oid_t GetUninlinedColumn(const oid_t column_id) const {
    return uninlined_columns[column_id];
  }

  // Return the number of columns in the schema for the tuple.
  oid_t GetColumnCount() const {
    return column_count;
  }

  oid_t GetUninlinedColumnCount() const {
    return uninlined_columns.size();
  }

  // Return the number of bytes used by one tuple.
  oid_t GetLength() const {
    return length;
  }

  // Returns a flag indicating whether all columns are inlined
  bool IsInlined() const {
    return tuple_is_inlined;
  }

  // Get a string representation of this schema
  friend std::ostream& operator<<(std::ostream& os, const Schema& schema);

 private:

  // Helper method to construct schema
  void CreateTupleSchema(const std::vector<oid_t> column_oids,
                         const std::vector<ValueType> column_types,
                         const std::vector<oid_t> column_lengths,
                         const std::vector<std::string> column_names,
                         const std::vector<bool> is_inlined);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // size of fixed length columns
  size_t length;

  // are all columns inlined
  bool tuple_is_inlined;

  // TODO: We currently need to ensure that this cached info is
  // always in sync. It is not safe for concurrent accesses.

  // track uninlined columns
  std::vector<oid_t> uninlined_columns;

  // column count
  oid_t column_count;

  // uninlined column count
  oid_t uninlined_column_count;

};

} // End catalog namespace
} // End peloton namespace
