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

// Specifies which columns should the executor return
class PropertyColumns : public Property {
 public:
  PropertyColumns(std::vector<Column *> columns);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  inline std::vector<Column *> &GetColumns() { return columns_; }

 private:
  std::vector<Column *> columns_;
};

// Specifies the output expressions of the query
class PropertyProjection : public Property {
 public:
  PropertyProjection(
      std::vector<std::unique_ptr<expression::AbstractExpression>> expressions);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  inline size_t GetProjectionListSize() { return expressions_.size(); }

  inline expression::AbstractExpression *GetProjection(size_t idx) {
    return expressions_[idx].get();
  }

 private:
  std::vector<std::unique_ptr<expression::AbstractExpression>> expressions_;
};

// Specifies the required sorting order of the query
class PropertySort : public Property {
 public:
  PropertySort(std::vector<Column *> sort_columns,
               std::vector<bool> sort_ascending);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

 private:
  std::vector<Column *> sort_columns;
  std::vector<bool> sort_ascending;
};

// Specifies the predicate that the tuples returned by the query should satisfy
class PropertyPredicate : public Property {
 public:
  PropertyPredicate(expression::AbstractExpression *predicate);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  inline expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

 private:
  std::unique_ptr<expression::AbstractExpression> predicate_;
};

} /* namespace optimizer */
} /* namespace peloton */
