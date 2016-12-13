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
void PropertyEnforcer::Visit(const PropertySort *) {}
void PropertyEnforcer::Visit(const PropertyPredicate *) {}

} /* namespace optimizer */
} /* namespace peloton */
