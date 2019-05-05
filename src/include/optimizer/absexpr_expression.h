//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// absexpr_expression.h
//
// Identification: src/include/optimizer/absexpr_expression.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/abstract_node_expression.h"
#include "optimizer/abstract_node.h"
#include "expression/abstract_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

// AbsExpr_Container and AbsExpr_Expression provides and serves an analogous purpose
// to Operator and OperatorExpression. Each AbsExpr_Container wraps a single
// AbstractExpression node with the children placed inside the AbsExpr_Expression.
//
// This is done to export the correct interface from the wrapped AbstractExpression
// to the rest of the core rule/optimizer code/logic.
class AbsExpr_Container: public AbstractNode {
 public:
  // Default constructors
  AbsExpr_Container() = default;
  AbsExpr_Container(const AbsExpr_Container &other):
    AbstractNode() {
    expr = other.expr;
  }

  AbsExpr_Container(std::shared_ptr<expression::AbstractExpression> expr_) {
    expr = expr_;
  }

  OpType GetOpType() const {
    return OpType::Undefined;
  }

  // Return operator type
  ExpressionType GetExpType() const {
    if (IsDefined()) {
      return expr->GetExpressionType();
    }
    return ExpressionType::INVALID;
  }

  std::shared_ptr<expression::AbstractExpression> GetExpr() const {
    return expr;
  }

  // Dummy Accept
  void Accept(OperatorVisitor *v) const {
    (void)v;
    PELOTON_ASSERT(0);
  }

  // Operator contains Logical node
  bool IsLogical() const {
    return true;
  }

  // Operator contains Physical node
  bool IsPhysical() const {
    return false;
  }

  std::string GetName() const {
    if (IsDefined()) {
      return expr->GetExpressionName();
    }

    return "Undefined";
  }

  hash_t Hash() const {
    if (IsDefined()) {
      return expr->Hash();
    }
    return 0;
  }

  bool operator==(const AbstractNode &r) {
    if (r.GetExpType() != ExpressionType::INVALID) {
      const AbsExpr_Container &cnt = dynamic_cast<const AbsExpr_Container&>(r);
      return (*this == cnt);
    }

    return false;
  }

  bool operator==(const AbsExpr_Container &r) {
    if (IsDefined() && r.IsDefined()) {
      //TODO(): proper equality check when migrate to terrier
      // Equality check relies on performing the following:
      // - Check each node's ExpressionType
      // - Check other parameters for a given node
      // We believe that in terrier so long as the AbstractExpression
      // are children-less, operator== provides sufficient checking.
      // The reason behind why the children-less guarantee is required,
      // is that the "real" children are actually tracked by the
      // AbsExpr_Expression class.
      return false;
    } else if (!IsDefined() && !r.IsDefined()) {
      return true;
    }
    return false;
  }

  // Operator contains physical or logical operator node
  bool IsDefined() const {
    return expr != nullptr;
  }

  //TODO(): Function should use std::shared_ptr when migrate to terrier
  expression::AbstractExpression *CopyWithChildren(std::vector<expression::AbstractExpression*> children);

 private:
  // Internal wrapped AbstractExpression
  std::shared_ptr<expression::AbstractExpression> expr;
};


class AbsExpr_Expression: public AbstractNodeExpression {
 public:
  AbsExpr_Expression(std::shared_ptr<AbstractNode> n) {
    std::shared_ptr<AbsExpr_Container> cnt = std::dynamic_pointer_cast<AbsExpr_Container>(n);
    PELOTON_ASSERT(cnt != nullptr);

    node = n;
  }

  // Disallow copy and move constructor
  DISALLOW_COPY_AND_MOVE(AbsExpr_Expression);

  void PushChild(std::shared_ptr<AbstractNodeExpression> op) {
    children.push_back(op);
  }

  void PopChild() {
    children.pop_back();
  }

  const std::vector<std::shared_ptr<AbstractNodeExpression>> &Children() const {
    return children;
  }

  const std::shared_ptr<AbstractNode> Node() const {
    // Integrity constraint
    std::shared_ptr<AbsExpr_Container> cnt = std::dynamic_pointer_cast<AbsExpr_Container>(node);
    PELOTON_ASSERT(cnt != nullptr);

    return node;
  }

  const std::string GetInfo() const {
    //TODO(): create proper info statement?
    return "";
  }

 private:
  std::shared_ptr<AbstractNode> node;
  std::vector<std::shared_ptr<AbstractNodeExpression>> children;
};

}  // namespace optimizer
}  // namespace peloton
