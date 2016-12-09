//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_enforcer.h
//
// Identification: src/include/optimizer/property_enforcer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/property_visitor.h"

namespace peloton {
namespace optimizer {

class ColumnManager;

//===--------------------------------------------------------------------===//
// Property Visitor
//===--------------------------------------------------------------------===//

// Enforce missing physical properties to group expression
class PropertyEnforcer : public PropertyVisitor {
 public:
  PropertyEnforcer(ColumnManager &manager) : manager_(manager) {}

  virtual void Visit(const PropertyColumns *) override;
  virtual void Visit(const PropertyOutputExpressions *) override;
  virtual void Visit(const PropertySort *) override;
  virtual void Visit(const PropertyPredicate *) override;

 private:
  ColumnManager &manager_;
};

} /* namespace optimizer */
} /* namespace peloton */
