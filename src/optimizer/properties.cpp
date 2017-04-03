//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// properties.cpp
//
// Identification: src/optimizer/properties.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/properties.h"
#include "common/macros.h"
#include "optimizer/property_visitor.h"

namespace peloton {
namespace optimizer {

PropertyColumns::PropertyColumns(
    std::vector<expression::TupleValueExpression *> column_exprs)
    : column_exprs_(std::move(column_exprs)), is_star_(false) {
  LOG_TRACE("Size of column property: %ld", columns_.size());
}

PropertyColumns::PropertyColumns(bool is_star_expr) : is_star_(is_star_expr) {}

PropertyType PropertyColumns::Type() const { return PropertyType::COLUMNS; }

bool PropertyColumns::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::COLUMNS) return false;
  const PropertyColumns &r_columns =
      *reinterpret_cast<const PropertyColumns *>(&r);

  if (is_star_ != r_columns.is_star_) return false;

  // check that every column in the right hand side property exists in the left
  // hand side property
  for (auto r_column : r_columns.column_exprs_) {
    bool has_column = false;
    for (auto column : column_exprs_) {
      if (column->GetBoundOid() == r_column->GetBoundOid()) {
        has_column = true;
        break;
      }
    }
    if (has_column == false) return false;
  }

  return true;
}

hash_t PropertyColumns::Hash() const {
  // hash the type
  hash_t hash = Property::Hash();
  hash = util::CombineHashes(hash, util::Hash<bool>(&is_star_));
  for (auto col : column_exprs_) {
    auto bound_oid = col->GetBoundOid();
    hash = util::CombineHashes(
        hash, util::Hash<std::tuple<oid_t, oid_t, oid_t>>(&bound_oid));
  }
  return hash;
}

void PropertyColumns::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertyColumns *)this);
}

PropertySort::PropertySort(
    std::vector<expression::TupleValueExpression *> sort_columns,
    std::vector<bool> sort_ascending)
    : sort_columns_(sort_columns), sort_ascending_(sort_ascending) {}

PropertyType PropertySort::Type() const { return PropertyType::SORT; }

bool PropertySort::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::SORT) return false;
  const PropertySort &r_sort = *reinterpret_cast<const PropertySort *>(&r);

  // All the sorting orders in r must be satisfied
  size_t num_sort_columns = r_sort.sort_columns_.size();
  PL_ASSERT(num_sort_columns == r_sort.sort_ascending_.size());
  for (size_t i = 0; i < num_sort_columns; ++i) {
    if (sort_columns_[i] != r_sort.sort_columns_[i]) return false;
    if (sort_ascending_[i] != r_sort.sort_ascending_[i]) return false;
  }

  return true;
}

hash_t PropertySort::Hash() const {
  // hash the type
  hash_t hash = Property::Hash();

  // hash sorting columns
  size_t num_sort_columns = sort_columns_.size();
  for (size_t i = 0; i < num_sort_columns; ++i) {
    auto bound_oid = sort_columns_[i]->GetBoundOid();
    hash = util::CombineHashes(
        hash, util::Hash<std::tuple<oid_t, oid_t, oid_t>>(&bound_oid));
    hash = util::CombineHashes(hash, sort_ascending_[i]);
  }
  return hash;
}

void PropertySort::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertySort *)this);
}

PropertyPredicate::PropertyPredicate(expression::AbstractExpression *predicate)
    : predicate_(predicate){};

PropertyType PropertyPredicate::Type() const { return PropertyType::PREDICATE; }

// For now we always assume the predicate property of the child operator will
// satisfy the parent operator, so we don't check the exact expression of the
// predicate.
// TODO: Check the content of the member variable predicate_ to see whether it
// satisfies r.
bool PropertyPredicate::operator>=(const Property &r) const {
  return Property::operator>=(r);
}

// Same as operator>=, we directly hash the type.
// TODO: Hash the content of the predicate expression
hash_t PropertyPredicate::Hash() const { return Property::Hash(); }

void PropertyPredicate::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertyPredicate *)this);
}

PropertyProjection::PropertyProjection(
    std::vector<std::unique_ptr<expression::AbstractExpression>> expressions)
    : expressions_(std::move(expressions)){};

PropertyType PropertyProjection::Type() const { return PropertyType::PROJECT; }

// PropertyOutputExpressions is only used for projection operator. We also
// assume this is always going to be satisfied.
bool PropertyProjection::operator>=(const Property &r) const {
  return Property::operator>=(r);
}

hash_t PropertyProjection::Hash() const { return Property::Hash(); }

void PropertyProjection::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertyProjection *)this);
}

} /* namespace optimizer */
} /* namespace peloton */
