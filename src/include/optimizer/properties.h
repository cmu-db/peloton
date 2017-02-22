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


#include "optimizer/column.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace optimizer {

// Specifies which columns should the executor return
class PropertyColumns : public Property {
 public:
  PropertyColumns(std::vector<expression::TupleValueExpression *> column_exprs);
  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  inline expression::TupleValueExpression *GetColumn(int idx) const { return column_exprs_[idx]; }

  inline size_t GetSize() const { return column_exprs_.size(); }

 private:
  std::vector<expression::TupleValueExpression *> column_exprs_;
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

  inline size_t GetProjectionListSize() const { return expressions_.size(); }

  inline expression::AbstractExpression *GetProjection(size_t idx) const {
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
