/*-------------------------------------------------------------------------
 *
 * column.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/catalog_object.h"
#include "backend/catalog/constraint.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//

class Column : public CatalogObject {
  friend class Constraint;

 public:

  Column(oid_t column_oid,
         std::string column_name,
         CatalogObject *parent,
         CatalogObject *root,
         ValueType value_type,
         oid_t column_offset,
         oid_t column_length,
         bool is_inlined)
 : CatalogObject(column_oid,
                 column_name,
                 parent,
                 root),
                 value_type(value_type),
                 column_offset(column_offset),
                 is_inlined(is_inlined){

    // Set the appropriate column length
    SetLength(column_length);

  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  oid_t GetOffset() const {
    return column_offset;
  }

  oid_t GetLength() const {
    if(is_inlined)
      return fixed_length;
    else
      return variable_length;
  }

  oid_t GetFixedLength() const {
    return fixed_length;
  }

  oid_t GetVariableLength() const {
    return variable_length;
  }

  ValueType GetType() const {
    return value_type;
  }

  bool IsInlined() const {
    return is_inlined;
  }

  // Compare two column objects
  bool operator== (const Column &other) const {
    if ( other.value_type != value_type || other.is_inlined != is_inlined){
      return false;
    }
    return true;
  }

  bool operator!= (const Column &other) const {
    return !(*this == other);
  }

  // Get a string representation for debugging
  friend std::ostream& operator<< (std::ostream& os, const Column& column);

 private:

  void SetLength(oid_t column_length);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // value type of column
  ValueType value_type;

  // offset of column in tuple
  oid_t column_offset;

  // if the column is not inlined, this is set to pointer size
  // else, it is set to length of the fixed length column
  oid_t fixed_length = INVALID_OID;

  // if the column is inlined, this is set to 0
  // else, it is set to length of the variable length column
  oid_t variable_length = INVALID_OID;

  // is the column inlined ?
  bool is_inlined;

};

} // End catalog namespace
} // End peloton namespace
