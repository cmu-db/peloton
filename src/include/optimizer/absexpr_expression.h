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

// AbstractExpression Definition
#include "expression/abstract_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

// (TODO): rethink the AbsExpr_Container/Expression approach in comparion to abstract
// Most of the core rule/optimizer code relies on the concept of an Operator /
// OperatorExpression and the interface that the two functions respectively expose.
//
// The annoying part is that an AbstractExpression blends together an Operator
// and OperatorExpression. Second part, the AbstractExpression does not export the
// correct interface that the rest of the system depends on.
//
// As an extreme level of simplification (sort of hacky), an AbsExpr_Container is
// analogous to Operator and wraps a single AbstractExpression node. AbsExpr_Expression
// is analogous to OperatorExpression.
//
// AbsExpr_Container does *not* handle memory correctly w.r.t internal instantiations
// from Rule transformation. This is since Peloton itself mixes unique_ptrs and
// hands out raw pointers which makes adding a shared_ptr here extremely problematic.
// terrier uses only shared_ptr when dealing with AbstractExpression trees.

class AbsExpr_Container {
 public:
  AbsExpr_Container();

  AbsExpr_Container(const expression::AbstractExpression *expr) {
    node = expr;
  }

  // Return operator type
  ExpressionType GetType() const {
    if (IsDefined()) {
      return node->GetExpressionType();
    }
    return ExpressionType::INVALID;
  }

  const expression::AbstractExpression *GetExpr() const {
    return node;
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
      return node->GetExpressionName();
    }

    return "Undefined";
  }

  hash_t Hash() const {
    if (IsDefined()) {
      return node->Hash();
    }
    return 0;
  }

  bool operator==(const AbsExpr_Container &r) {
    if (IsDefined() && r.IsDefined()) {
      // (TODO): need a better way to determine deep equality

      // NOTE:
      // Without proper equality determinations, the groups will
      // not be assigned correctly. Arguably, terrier does this
      // better because a blind ExactlyEquals on different types
      // of ConstantValueExpression under Peloton will crash!

      // For now, just return (false).
      // I don't anticipate this will affect correctness, just
      // performance, since duplicate trees will have to evaluated
      // over and over again, rather than being able to "borrow"
      // a previous tree's rewrite.
      //
      // Probably not worth to create a "validator" since porting
      // this to terrier anyways (?). == does not check Value
      // so it's broken. ExactlyEqual requires precondition checking.
      return false;
    } else if (!IsDefined() && !r.IsDefined()) {
      return true;
    }
    return false;
  }

  // Operator contains physical or logical operator node
  bool IsDefined() const {
    return node != nullptr;
  }

  //(TODO): fix memory management once go to terrier
  expression::AbstractExpression *Rebuild(std::vector<expression::AbstractExpression*> children) {
    switch (GetType()) {
      case ExpressionType::COMPARE_EQUAL:
      case ExpressionType::COMPARE_NOTEQUAL:
      case ExpressionType::COMPARE_LESSTHAN:
      case ExpressionType::COMPARE_GREATERTHAN:
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      case ExpressionType::COMPARE_LIKE:
      case ExpressionType::COMPARE_NOTLIKE:
      case ExpressionType::COMPARE_IN:
      case ExpressionType::COMPARE_DISTINCT_FROM: {
        PELOTON_ASSERT(children.size() == 2);
        return new expression::ComparisonExpression(GetType(), children[0], children[1]);
      }
      case ExpressionType::CONJUNCTION_AND:
      case ExpressionType::CONJUNCTION_OR: {
        PELOTON_ASSERT(children.size() == 2);
        return new expression::ConjunctionExpression(GetType(), children[0], children[1]);
      }
      case ExpressionType::VALUE_CONSTANT: {
        PELOTON_ASSERT(children.size() == 0);
        auto cve = static_cast<const expression::ConstantValueExpression*>(node);
        return new expression::ConstantValueExpression(cve->GetValue());
      }
      default: {
        int type = static_cast<int>(GetType());
        LOG_ERROR("Unimplemented Rebuild() for %d found", type);
        return nullptr;
      }
    }
  }

 private:
  const expression::AbstractExpression *node;
};

class AbsExpr_Expression {
 public:
  AbsExpr_Expression(AbsExpr_Container op): op(op) {};

  void PushChild(std::shared_ptr<AbsExpr_Expression> op) {
    children.push_back(op);
  }

  void PopChild() {
    children.pop_back();
  }

  const std::vector<std::shared_ptr<AbsExpr_Expression>> &Children() const {
    return children;
  }

  const AbsExpr_Container &Op() const {
    return op;
  }

 private:
  AbsExpr_Container op;
  std::vector<std::shared_ptr<AbsExpr_Expression>> children;
};

}  // namespace optimizer
}  // namespace peloton

