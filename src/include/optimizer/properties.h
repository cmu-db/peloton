//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// properties.h
//
// Identification: src/optimizer/properties.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/property.h"
#include "optimizer/column.h"

namespace peloton {
namespace optimizer {

class PropertyColumns : public Property {
 public:
  PropertyColumns(std::vector<Column *> columns);

  PropertyType Type() const override;

 private:
  std::vector<Column *> columns;
};

class PropertySort : public Property {
 public:
  PropertySort(std::vector<Column *> sort_columns,
               std::vector<bool> sort_ascending);

  PropertyType Type() const override;

 private:
  std::vector<Column *> sort_columns;
  std::vector<bool> sort_ascending;
};

} /* namespace optimizer */
} /* namespace peloton */
