//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.cpp
//
// Identification: src/backend/optimizer/column.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/optimizer/column.h"
#include "backend/catalog/column.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//
Column::Column(ColumnID id,
               ValueType type,
               int size,
               std::string name,
               bool inlined)
  : id(id), type(type), size(size), name(name), inlined(inlined)
{
}

ColumnID Column::ID() const {
  return id;
}

ValueType Column::Type() const {
  return type;
}

int Column::Size() const {
  return size;
}

std::string Column::Name() const {
  return name;
}

bool Column::Inlined() const {
  return inlined;
}

hash_t Column::Hash() const {
  return util::Hash<ColumnID>(&id);
}

//===--------------------------------------------------------------------===//
// TableColumn
//===--------------------------------------------------------------------===//
TableColumn::TableColumn(ColumnID id,
                         ValueType type,
                         int size,
                         std::string name,
                         bool inlined,
                         oid_t base_table,
                         oid_t column_index)
  : Column(id, type, size, name, inlined),
    base_table(base_table),
    column_index(column_index)
{
}

oid_t TableColumn::BaseTableOid() const {
  return base_table;
}

oid_t TableColumn::ColumnIndexOid() const {
  return column_index;
}

//===--------------------------------------------------------------------===//
// ExprColumn
//===--------------------------------------------------------------------===//
ExprColumn::ExprColumn(ColumnID id,
                       ValueType type,
                       int size,
                       std::string name,
                       bool inlined)
  : Column(id, type, size, name, inlined)
{
}

catalog::Column GetSchemaColumnFromOptimizerColumn(Column *column) {
  return catalog::Column(column->Type(),
                         column->Size(),
                         column->Name(),
                         column->Inlined());
}

} /* namespace optimizer */
} /* namespace peloton */
