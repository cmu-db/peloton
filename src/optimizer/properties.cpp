//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// properties.cpp
//
// Identification: src/backend/optimizer/properties.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/optimizer/properties.h"

namespace peloton {
namespace optimizer {

PropertyColumns::PropertyColumns(std::vector<Column *> columns)
  : columns(columns)
{
}

PropertyType PropertyColumns::Type() const {
  return PropertyType::Columns;
}

PropertySort::PropertySort(std::vector<Column *> sort_columns,
                           std::vector<bool> sort_ascending)
  : sort_columns(sort_columns), sort_ascending(sort_ascending)
{
}

PropertyType PropertySort::Type() const {
  return PropertyType::Sort;
}


} /* namespace optimizer */
} /* namespace peloton */
