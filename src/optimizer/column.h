//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.h
//
// Identification: src/backend/optimizer/column.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/operator_node.h"
#include "backend/optimizer/query_operators.h"
#include "backend/optimizer/util.h"

#include "backend/common/types.h"

namespace peloton {
namespace optimizer {

using ColumnID = int32_t;

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//
class Column {
 public:
  Column(ColumnID id,
         ValueType type,
         int size,
         std::string name,
         bool inlined);

  virtual ~Column() {}

  ColumnID ID() const;

  ValueType Type() const;

  int Size() const;

  std::string Name() const;

  bool Inlined() const;

  hash_t Hash() const;

 private:
  const ColumnID id;
  const ValueType type;
  const int size;
  const std::string name;
  const bool inlined;
};

//===--------------------------------------------------------------------===//
// TableColumn
//===--------------------------------------------------------------------===//
class TableColumn : public Column {
 public:
  TableColumn(ColumnID id,
              ValueType type,
              int size,
              std::string name,
              bool inlined,
              oid_t base_table,
              oid_t column_index);

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
  ExprColumn(ColumnID id,
             ValueType type,
             int size,
             std::string name,
             bool inlined);
};

catalog::Column GetSchemaColumnFromOptimizerColumn(Column *column);

} /* namespace optimizer */
} /* namespace peloton */
