//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// properties.h
//
// Identification: src/include/optimizer/properties.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "optimizer/column.h"
#include "optimizer/property.h"

namespace peloton {
namespace optimizer {

class PropertyColumns : public Property {
 public:
  PropertyColumns(std::vector<Column *> columns);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator==(const Property &r) const override;

 private:
  std::vector<Column *> columns;
};

class PropertySort : public Property {
 public:
  PropertySort(std::vector<Column *> sort_columns,
               std::vector<bool> sort_ascending);

  PropertyType Type() const override;

  // hash_t Hash() const override;

  // bool operator==(const Property &r) const override;

 private:
  std::vector<Column *> sort_columns;
  std::vector<bool> sort_ascending;
};

class PropertyPredicate : public Property {
 public:
  PropertyPredicate(expression::AbstractExpression *predicate);

  PropertyType Type() const override;

  // hash_t Hash() const override;

  // bool operator==(const Property &r) const override;

 private:
  expression::AbstractExpression *predicate_;
};

} /* namespace optimizer */
} /* namespace peloton */
