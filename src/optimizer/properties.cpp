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

PropertyColumns::PropertyColumns(std::vector<Column *> columns)
    : columns(std::move(columns)) {}

PropertyType PropertyColumns::Type() const { return PropertyType::COLUMNS; }

bool PropertyColumns::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::COLUMNS) return false;
  const PropertyColumns &r_columns =
      *reinterpret_cast<const PropertyColumns *>(&r);

  // check that every column in the right hand side property exists in the left
  // hand side property
  for (auto column : columns) {
    bool has_column = false;
    for (auto r_column : r_columns.columns) {
      if (column->ID() == r_column->ID()) {
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

  // hash columns
  for (Column *col : columns) {
    hash = util::CombineHashes(hash, col->Hash());
  }
  return hash;
}

void PropertyColumns::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertyColumns *)this);
}

PropertySort::PropertySort(std::vector<Column *> sort_columns,
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
    if (sort_columns[i]->ID() != r_sort.sort_columns[i]->ID()) return false;
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
    hash = util::CombineHashes(hash, sort_columns[i]->Hash());
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

PropertyOutputExpressions::PropertyOutputExpressions(
    std::vector<std::unique_ptr<expression::AbstractExpression> > expressions)
    : expressions_(std::move(expressions)){};

PropertyType PropertyOutputExpressions::Type() const {
  return PropertyType::OUTPUT_EXPRESSION;
}

// PropertyOutputExpressions is only used for projection operator. We also
// assume this is always going to be satisfied.
bool PropertyOutputExpressions::operator>=(const Property &r) const {
  return Property::operator>=(r);
}

hash_t PropertyOutputExpressions::Hash() const { return Property::Hash(); }

void PropertyOutputExpressions::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertyOutputExpressions *)this);
}

} /* namespace optimizer */
} /* namespace peloton */
