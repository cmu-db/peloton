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
#include "optimizer/property.h"

namespace peloton {
namespace optimizer {

std::shared_ptr<GroupExpression> PropertyEnforcer::EnforceProperty(
    std::shared_ptr<GroupExpression> gexpr, PropertySet *properties,
    std::shared_ptr<Property> property) {
  input_gexpr_ = gexpr;
  input_properties_ = properties;
  property->Accept(this);
  return output_gexpr_;
}

void PropertyEnforcer::Visit(const PropertyColumns *) {}
void PropertyEnforcer::Visit(const PropertyOutputExpressions *) {}
void PropertyEnforcer::Visit(const PropertySort *) {}
void PropertyEnforcer::Visit(const PropertyPredicate *) {}

} /* namespace optimizer */
} /* namespace peloton */
