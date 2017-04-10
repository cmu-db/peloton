//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_enforcer.cpp
//
// Identification: src/optimizer/property_enforcer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/property_enforcer.h"
#include "optimizer/operators.h"
#include "optimizer/property.h"
#include "optimizer/properties.h"

namespace peloton {
namespace optimizer {

std::shared_ptr<OperatorExpression> PropertyEnforcer::EnforceProperty(
    std::shared_ptr<GroupExpression> gexpr, PropertySet *properties,
    std::shared_ptr<Property> property) {
  input_gexpr_ = gexpr;
  input_properties_ = properties;
  property->Accept(this);
  return output_expr_;
}

void PropertyEnforcer::Visit(const PropertyColumns *) {}

void PropertyEnforcer::Visit(const PropertyProjection *) {
  auto project_expr =
      std::make_shared<OperatorExpression>(PhysicalProject::make());

  project_expr->PushChild(
      std::make_shared<OperatorExpression>(input_gexpr_->Op()));

  output_expr_ = project_expr;
}

void PropertyEnforcer::Visit(const PropertySort *property) {
  std::vector<expression::TupleValueExpression *> sort_columns;
  std::vector<bool> sort_ascending;
  size_t column_size = property->GetSortColumnSize();
  for (size_t i = 0; i < column_size; ++i) {
    sort_columns.push_back(property->GetSortColumn(i));
    sort_ascending.push_back(property->GetSortAscending(i));
  }

  auto order_by_expr = std::make_shared<OperatorExpression>(
      PhysicalOrderBy::make(sort_columns, sort_ascending));

  order_by_expr->PushChild(
      std::make_shared<OperatorExpression>(input_gexpr_->Op()));

  output_expr_ = order_by_expr;
}

void PropertyEnforcer::Visit(const PropertyPredicate *) {}

} /* namespace optimizer */
} /* namespace peloton */
