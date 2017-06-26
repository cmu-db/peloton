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

#include "optimizer/group_expression.h"
#include "optimizer/operator_expression.h"
#include "optimizer/property_visitor.h"

namespace peloton {
namespace optimizer {

class ColumnManager;
class PropertySet;

//===--------------------------------------------------------------------===//
// Property Visitor
//===--------------------------------------------------------------------===//

// Enforce missing physical properties to group expression
class PropertyEnforcer : public PropertyVisitor {
 public:
  PropertyEnforcer(ColumnManager &manager) : manager_(manager) {}

  std::shared_ptr<GroupExpression> EnforceProperty(
      std::shared_ptr<GroupExpression> gexpr, PropertySet *properties,
      std::shared_ptr<Property> property);

  virtual void Visit(const PropertyColumns *) override;
  virtual void Visit(const PropertySort *) override;
  virtual void Visit(const PropertyPredicate *) override;
  virtual void Visit(const PropertyDistinct *) override;
  virtual void Visit(const PropertyLimit *) override;

 private:
  ColumnManager &manager_;
  std::shared_ptr<GroupExpression> input_gexpr_;
  std::shared_ptr<GroupExpression> output_gexpr_;
  PropertySet *input_properties_;
};

} /* namespace optimizer */
} /* namespace peloton */
