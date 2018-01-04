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

#include "optimizer/property.h"
#include "optimizer/property_visitor.h"

namespace peloton {
namespace optimizer {

/*************** PropertySort *****************/

PropertySort::PropertySort(
    std::vector<expression::AbstractExpression *> sort_columns,
    std::vector<bool> sort_ascending)
    : sort_columns_(std::move(sort_columns)),
      sort_ascending_(std::move(sort_ascending)) {}

PropertyType PropertySort::Type() const { return PropertyType::SORT; }

bool PropertySort::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::SORT) return false;
  const PropertySort &r_sort = *reinterpret_cast<const PropertySort *>(&r);

  // All the sorting orders in r must be satisfied
  size_t l_num_sort_columns = sort_columns_.size();
  size_t r_num_sort_columns = r_sort.sort_columns_.size();
  PL_ASSERT(r_num_sort_columns == r_sort.sort_ascending_.size());
  // We want to ensure that Sort(a, b, c, d, e) >= Sort(a, b, c)
  if (l_num_sort_columns < r_num_sort_columns) {
    return false;
  }
  for (size_t idx = 0; idx < r_num_sort_columns; ++idx) {
    if (!sort_columns_[idx]->ExactlyEquals(*r_sort.sort_columns_[idx])) {
      return false;
    }
  }
  return true;
}

hash_t PropertySort::Hash() const {
  // hash the type
  hash_t hash = Property::Hash();

  // hash sorting columns
  size_t num_sort_columns = sort_columns_.size();
  for (size_t i = 0; i < num_sort_columns; ++i) {
    hash = HashUtil::CombineHashes(hash, sort_columns_[i]->Hash());
    hash = HashUtil::CombineHashes(hash, sort_ascending_[i]);
  }
  return hash;
}

void PropertySort::Accept(PropertyVisitor *v) const {
  v->Visit((const PropertySort *)this);
}

std::string PropertySort::ToString() const {
  std::string str = PropertyTypeToString(Type()) + ":(";
  size_t num_sort_columns = sort_columns_.size();
  for (size_t i = 0; i < num_sort_columns; ++i) {
    str += sort_columns_[i]->GetInfo() + ", " +
           (sort_ascending_[i] ? "asc" : "dsc");
  }
  return str + ")";
}

}  // namespace optimizer
}  // namespace peloton
