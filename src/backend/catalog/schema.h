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
#include "backend/catalog/constraint.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column Information
//===--------------------------------------------------------------------===//

// TODO: Why is this different than catalog::ColumnInfo?
class ColumnInfo {
 friend class Constraint;

 ColumnInfo() = delete;

 public:

 // TODO :: Scrubbing unused constructors...
 // Configures (type, length, name, allow_null)
 ColumnInfo(ValueType column_type, oid_t column_length, std::string column_name, bool allow_null)
   : type(column_type), offset(0), name(column_name), allow_null(allow_null) {

      switch( type ){
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_DOUBLE:
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_TIMESTAMP:
          is_inlined = true;
          break;
        default:
          is_inlined = false;
          break;
      }

      if(is_inlined){
        fixed_length = column_length;
        variable_length = 0;
      }
      else{
        fixed_length = sizeof(uintptr_t);
        variable_length = column_length;
      }
    }

 // Configures (type, length, name, allow_null, constraint)
 ColumnInfo(ValueType column_type, oid_t column_length, std::string column_name, bool allow_null, std::vector<catalog::Constraint> column_constraint_vector)
   : type(column_type), offset(0), name(column_name), allow_null(allow_null), constraint_vector(column_constraint_vector){

      switch( type ){
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_DOUBLE:
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_TIMESTAMP:
          is_inlined = true;
          break;
        default:
          is_inlined = false;
          break;
      }

      if(is_inlined){
        fixed_length = column_length;
        variable_length = 0;
      }
      else{
        fixed_length = sizeof(uintptr_t);
        variable_length = column_length;
      }
    }



  // Configures ( type, length, name, allow_null, is_inlined )
  ColumnInfo(ValueType column_type, oid_t column_length, std::string column_name, bool allow_null, bool is_inlined)
 : type(column_type), offset(0), name(column_name), allow_null(allow_null), is_inlined(is_inlined){
  
    if(is_inlined){
      fixed_length = column_length;
      variable_length = 0;
    }
    else{
      fixed_length = sizeof(uintptr_t);
      variable_length = column_length;
    }
  }

  // Configure all members except in_inlined
  ColumnInfo(ValueType column_type, oid_t column_offset, oid_t column_length,
      std::string column_name, bool allow_null, std::vector<catalog::Constraint> column_constraint_vector)
    : type(column_type), offset(column_offset), name(column_name), allow_null(allow_null), 
    constraint_vector(column_constraint_vector){

      switch( type ){
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_DOUBLE:
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_TIMESTAMP:
          is_inlined = true;
          break;
        default:
          is_inlined = false;
          break;
      }

      if(is_inlined){
        fixed_length = column_length;
        variable_length = 0;
      }
      else{
        fixed_length = sizeof(uintptr_t);
        variable_length = column_length;
      }
    }

  // Configure all members
  ColumnInfo(ValueType column_type, oid_t column_offset,
             oid_t column_length, std::string column_name,
             bool allow_null, bool is_inlined,
             std::vector<catalog::Constraint> column_constraint_vector)
  : type(column_type), offset(column_offset),
    name(column_name),
    allow_null(allow_null), is_inlined(is_inlined), constraint_vector(column_constraint_vector){

    if(is_inlined){
      fixed_length = column_length;
      variable_length = 0;
    }
    else{
      fixed_length = sizeof(uintptr_t);
      variable_length = column_length;
    }
  }

  /// Compare two column info objects
  bool operator== (const ColumnInfo &other) const {
    if (other.allow_null != allow_null || other.type != type ||
        other.is_inlined != is_inlined) {
      return false;
    }
    return true;
  }

  bool operator!= (const ColumnInfo &other) const {
    return !(*this == other);
  }


  /// Get a string representation for debugging
  friend std::ostream& operator<< (std::ostream& os, const ColumnInfo& column_info);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  /// type of column
  ValueType type;

  /// offset of column in tuple
  size_t offset;

  /// if the column is not inlined, this is set to pointer size
  /// else, it is set to length of the fixed length column
  size_t fixed_length;

  /// if the column is inlined, this is set to 0
  /// else, it is set to length of the variable length column
  size_t variable_length;

  // column name
  std::string name;

  bool allow_null;
  bool is_inlined;

  // Constraints
  std::vector<Constraint> constraint_vector;
};

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//

class Schema	{
  friend class ColumnInfo;

 public:
  Schema() = delete;

  //===--------------------------------------------------------------------===//
  // Static factory methods to construct schema objects
  //===--------------------------------------------------------------------===//

  /// Construct schema
  void CreateTupleSchema(const std::vector<ValueType> column_types,
                         const std::vector<oid_t> column_lengths,
                         const std::vector<std::string> column_names,
                         const std::vector<bool> allow_null,
                         const std::vector<bool> is_inlined,
                         const std::vector<std::vector<Constraint>> constraint_vector_of_vectors);

  /// Construct schema from vector of ColumnInfo
  Schema(const std::vector<ColumnInfo> columns);

  /// Copy schema
  static Schema *CopySchema(const Schema *schema);

  /// Copy subset of columns in the given schema
  static Schema *CopySchema(
      const Schema *schema,
      const std::vector<oid_t>& set);

  /// Append two schema objects
  static Schema *AppendSchema(Schema *first, Schema *second);

  /// Append subset of columns in the two given schemas
  static Schema *AppendSchema(
      Schema *first,
      std::vector<oid_t>& first_set,
      Schema *second,
      std::vector<oid_t>& second_set);

  /// Append given schemas.
  static Schema *AppendSchemaList(std::vector<Schema> &schema_list);

  /// Append given schemas.
  static Schema *AppendSchemaPtrList(const std::vector<Schema *> &schema_list);

  /// Append subsets of columns in the given schemas.
  static Schema *AppendSchemaPtrList(
      const std::vector<Schema *> &schema_list,
      const std::vector<std::vector<oid_t> > &subsets);

  /// Compare two schemas
  bool operator== (const Schema &other) const;
  bool operator!= (const Schema &other) const;

  //===--------------------------------------------------------------------===//
  // Schema accessors
  //===--------------------------------------------------------------------===//

  inline size_t GetOffset(const oid_t column_id) const {
    return columns[column_id].offset;
  }

  inline ValueType GetType(const oid_t column_id) const {
    return columns[column_id].type;
  }

  /// Returns fixed length
  inline size_t GetLength(const oid_t column_id) const {
    return columns[column_id].fixed_length;
  }

  inline size_t GetVariableLength(const oid_t column_id) const {
    return columns[column_id].variable_length;
  }

  //// Get the nullability of the column at a given index.
  inline bool AllowNull(const oid_t column_id) const {
    return columns[column_id].allow_null;
  }


  inline bool IsInlined(const oid_t column_id) const {
    return columns[column_id].is_inlined;
  }

  const ColumnInfo GetColumnInfo(const oid_t column_id) const {
    return columns[column_id];
  }

  oid_t GetUninlinedColumnIndex(const oid_t column_id) const {
    return uninlined_columns[column_id];
  }

  std::vector<ColumnInfo> GetColumns() const {
    return columns;
  }

  /// Return the number of columns in the schema for the tuple.
  inline oid_t GetColumnCount() const {
    return column_count;
  }

  oid_t GetUninlinedColumnCount() const {
    return uninlined_column_count;
  }

  /// Return the number of bytes used by one tuple.
  inline oid_t GetLength() const {
    return length;
  }

  /// Returns a flag indicating whether all columns are inlined
  bool IsInlined() const {
    return tuple_is_inlined;
  }

  /// Get a string representation of this schema
  friend std::ostream& operator<<(std::ostream& os, const Schema& schema);

 private:
  // size of fixed length columns
  size_t length;

  // all inlined and uninlined columns in the tuple
  std::vector<ColumnInfo> columns;

  // keeps track of unlined columns
  std::vector<oid_t> uninlined_columns;

  // keep these in sync with the vectors above
  oid_t column_count;

  oid_t uninlined_column_count;

  /// are all columns inlined
  bool tuple_is_inlined;

};

} // End catalog namespace
} // End peloton namespace
