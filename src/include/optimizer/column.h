//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.h
//
// Identification: src/include/optimizer/column.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/column.h"

#include "common/internal_types.h"

namespace peloton {
namespace optimizer {

using ColumnID = int32_t;

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//
class Column {
 public:
  Column(ColumnID id, type::TypeId type, int size, std::string name,
         bool inlined);

  virtual ~Column() {}

  ColumnID ID() const;

  type::TypeId Type() const;

  int Size() const;

  std::string Name() const;

  bool Inlined() const;

  hash_t Hash() const;

  template <typename T>
  const T *As() const {
    if (typeid(*this) == typeid(T)) {
      return reinterpret_cast<const T *>(this);
    }
    return nullptr;
  }

 private:
  const ColumnID id;
  const type::TypeId type;
  const int size;
  const std::string name;
  const bool inlined;
};

//===--------------------------------------------------------------------===//
// TableColumn
//===--------------------------------------------------------------------===//
class TableColumn : public Column {
 public:
  TableColumn(ColumnID id, type::TypeId type, int size, std::string name,
              bool inlined, oid_t base_table, oid_t column_index);

  oid_t BaseTableOid() const;

  oid_t ColumnIndexOid() const;

 private:
  const oid_t base_table;
  const oid_t column_index;
};

//===--------------------------------------------------------------------===//
// ExprColumn
//===--------------------------------------------------------------------===//
class ExprColumn : public Column {
 public:
  ExprColumn(ColumnID id, type::TypeId type, int size, std::string name,
             bool inlined);
};

catalog::Column GetSchemaColumnFromOptimizerColumn(Column *column);

}  // namespace optimizer
}  // namespace peloton
