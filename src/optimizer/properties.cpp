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

PropertyColumns::PropertyColumns(std::vector<expression::TupleValueExpression *> column_exprs)
    : column_exprs_(std::move(column_exprs)) {
  LOG_TRACE("Size of column property: %ld", columns_.size());
}

PropertyType PropertyColumns::Type() const { return PropertyType::COLUMNS; }

bool PropertyColumns::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::COLUMNS) return false;
  const PropertyColumns &r_columns =
      *reinterpret_cast<const PropertyColumns *>(&r);

  // check that every column in the right hand side property exists in the left
  // hand side property
  for (auto column : column_exprs_) {
    bool has_column = false;
    for (auto r_column : r_columns.column_exprs_) {
      // TODO: Do not compare ptr directly
      if (column == r_column) {
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

  for (auto col: column_exprs_) {
    hash = util::CombineHashes(
        hash, util::Hash<std::tuple<oid_t, oid_t, oid_t >>(&col->bound_obj_id));
  }
  return hash;
}

void PropertyColumns::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertyColumns *)this);
}

PropertySort::PropertySort(std::vector<expression::TupleValueExpression *> sort_columns,
                           std::vector<bool> sort_ascending)
    : sort_columns(sort_columns), sort_ascending(sort_ascending) {}

PropertyType PropertySort::Type() const { return PropertyType::SORT; }

bool PropertySort::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::SORT) return false;
  const PropertySort &r_sort = *reinterpret_cast<const PropertySort *>(&r);

  // All the sorting orders in r must be satisfied
  size_t num_sort_columns = r_sort.sort_columns.size();
  PL_ASSERT(num_sort_columns == r_sort.sort_ascending.size());
  for (size_t i = 0; i < num_sort_columns; ++i) {
    // TODO: Do not compare ptr directly
    if (sort_columns[i] != r_sort.sort_columns[i]) return false;
    if (sort_ascending[i] != r_sort.sort_ascending[i]) return false;
  }

  return true;
}

hash_t PropertySort::Hash() const {
  // hash the type
  hash_t hash = Property::Hash();

  // hash sorting columns
  size_t num_sort_columns = sort_columns.size();
  for (size_t i = 0; i < num_sort_columns; ++i) {
    hash = util::CombineHashes(
        hash, util::Hash<std::tuple<oid_t, oid_t, oid_t >>(&(sort_columns[i]->bound_obj_id)));
    hash = util::CombineHashes(hash, sort_ascending[i]);
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
    std::vector<std::unique_ptr<expression::AbstractExpression> > expressions)
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
