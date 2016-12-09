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

namespace peloton {
namespace optimizer {

void PropertyEnforcer::Visit(const PropertyColumns *) {}
void PropertyEnforcer::Visit(const PropertyOutputExpressions *) {}
void PropertyEnforcer::Visit(const PropertySort *) {}
void PropertyEnforcer::Visit(const PropertyPredicate *) {}

} /* namespace optimizer */
} /* namespace peloton */
