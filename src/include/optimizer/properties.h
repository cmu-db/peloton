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

#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "optimizer/column.h"
#include "optimizer/property.h"

namespace peloton {
namespace optimizer {

// Specifies the required sorting order of the query
class PropertySort : public Property {
 public:
  PropertySort(
      std::vector<expression::AbstractExpression*> sort_columns,
      std::vector<bool> sort_ascending);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  std::string ToString() const override;

  inline size_t GetSortColumnSize() const { return sort_columns_.size(); }

  inline expression::AbstractExpression* GetSortColumn(
      size_t idx) const {
    return sort_columns_[idx];
  }

  inline bool GetSortAscending(int idx) const { return sort_ascending_[idx]; }

 private:
  std::vector<expression::AbstractExpression*> sort_columns_;
  std::vector<bool> sort_ascending_;
};

}  // namespace optimizer
}  // namespace peloton
