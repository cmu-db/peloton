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

namespace peloton {
namespace optimizer {

PropertyColumns::PropertyColumns(std::vector<Column *> columns)
    : columns(columns) {}

PropertyType PropertyColumns::Type() const { return PropertyType::COLUMNS; }

PropertySort::PropertySort(std::vector<Column *> sort_columns,
                           std::vector<bool> sort_ascending)
    : sort_columns(sort_columns), sort_ascending(sort_ascending) {}

PropertyType PropertySort::Type() const { return PropertyType::SORT; }

PropertyPredicate::PropertyPredicate(expression::AbstractExpression *predicate)
    : predicate_(predicate){};

PropertyType PropertyPredicate::Type() const { return PropertyType::PREDICATE; }

} /* namespace optimizer */
} /* namespace peloton */
