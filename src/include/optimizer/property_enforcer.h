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

class PropertySet;

//===--------------------------------------------------------------------===//
// Property Visitor
//===--------------------------------------------------------------------===//

// Enforce missing physical properties to group expression
class PropertyEnforcer : public PropertyVisitor {
 public:


  std::shared_ptr<GroupExpression<Operator,OpType,OperatorExpression>> EnforceProperty(
      GroupExpression<Operator,OpType,OperatorExpression>* gexpr, Property* property);

  virtual void Visit(const PropertyColumns *) override;
  virtual void Visit(const PropertySort *) override;
  virtual void Visit(const PropertyDistinct *) override;
  virtual void Visit(const PropertyLimit *) override;

 private:
  GroupExpression<Operator,OpType,OperatorExpression>* input_gexpr_;
  std::shared_ptr<GroupExpression<Operator,OpType,OperatorExpression>> output_gexpr_;
};

}  // namespace optimizer
}  // namespace peloton
