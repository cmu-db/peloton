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
#include "expression/expression_util.h"

namespace peloton {
namespace optimizer {

// Specifies which columns should the operator returns
class PropertyColumns : public Property {
 public:
  PropertyColumns(std::vector<std::shared_ptr<expression::AbstractExpression>>
                      column_exprs);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  std::string ToString() const override;

  inline std::shared_ptr<expression::AbstractExpression> GetColumn(
      size_t idx) const {
    return column_exprs_[idx];
  }

  inline size_t GetSize() const { return column_exprs_.size(); }

  bool HasStarExpression() const;

 private:
  std::vector<std::shared_ptr<expression::AbstractExpression>> column_exprs_;
};

// Specify which columns values are dictinct
// PropertyDistinct(col_a, col_b, col_c) should have
// distinct value for (col_a, col_b, col_c) in each return tuple
class PropertyDistinct : public Property {
 public:
  PropertyDistinct(std::vector<std::shared_ptr<expression::AbstractExpression>>
                       column_exprs);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  std::string ToString() const override;

  inline std::shared_ptr<expression::AbstractExpression> GetDistinctColumn(
      int idx) const {
    return distinct_column_exprs_[idx];
  }

  inline size_t GetSize() const { return distinct_column_exprs_.size(); }

 private:
  std::vector<std::shared_ptr<expression::AbstractExpression>>
      distinct_column_exprs_;
};
  
// Specify how many tuples to output
class PropertyLimit : public Property {
 public:
  PropertyLimit(int64_t offset, int64_t limit):offset_(offset), limit_(limit){}
  
  PropertyType Type() const override;
  
  hash_t Hash() const override;
  
  bool operator>=(const Property &r) const override;
  
  void Accept(PropertyVisitor *v) const override;
  
  std::string ToString() const override;
  
  inline int64_t GetLimit() const { return limit_; }
  
  inline int64_t GetOffset() const { return offset_; }
  
 private:
  int64_t offset_;
  int64_t limit_;
};

// Specifies the required sorting order of the query
class PropertySort : public Property {
 public:
  PropertySort(
      std::vector<std::shared_ptr<expression::AbstractExpression>> sort_columns,
      std::vector<bool> sort_ascending);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  std::string ToString() const override;

  inline size_t GetSortColumnSize() const { return sort_columns_.size(); }

  inline std::shared_ptr<expression::AbstractExpression> GetSortColumn(
      size_t idx) const {
    return sort_columns_[idx];
  }

  inline bool GetSortAscending(int idx) const { return sort_ascending_[idx]; }

 private:
  std::vector<std::shared_ptr<expression::AbstractExpression>> sort_columns_;
  std::vector<bool> sort_ascending_;
};

// Specifies the predicate that the tuples returned by the query should satisfy
class PropertyPredicate : public Property {
 public:
  PropertyPredicate(expression::AbstractExpression *predicate);

  PropertyType Type() const override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  void Accept(PropertyVisitor *v) const override;

  std::string ToString() const override;

  inline expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

 private:
  std::unique_ptr<expression::AbstractExpression> predicate_;
};

} /* namespace optimizer */
} /* namespace peloton */
