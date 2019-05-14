//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// group_marker_expression.h
//
// Identification: src/include/expression/group_marker_expression.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "optimizer/group_expression.h"
#include "util/hash_util.h"

namespace peloton {

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace expression {

//===----------------------------------------------------------------------===//
// GroupMarkerExpression
//===----------------------------------------------------------------------===//

/**
 * When binding expressions to specific patterns, we allow for a "wildcard".
 * This GroupMarkerExpression serves to encapsulate and represent an expression
 * that was bound successfully to a "wildcard" pattern node.
 *
 * This class contains a single GroupID that can be used as a lookup into the
 * Memo class for the actual expression. In short, this GroupMarkerExpression
 * serves as an indirection wrapper pointing to the actual expression.
 */
class GroupMarkerExpression : public AbstractExpression {
 public:
  GroupMarkerExpression(optimizer::GroupID group_id) :
    AbstractExpression(ExpressionType::GROUP_MARKER),
    group_id_(group_id) {};

  optimizer::GroupID GetGroupID() { return group_id_; }

  AbstractExpression *Copy() const override {
    return new GroupMarkerExpression(group_id_);
  }

  type::Value Evaluate(const AbstractTuple *tuple1,
                       const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const {
    (void)tuple1;
    (void)tuple2;
    (void)context;
    PELOTON_ASSERT(0);
  }

  void Accept(SqlNodeVisitor *) {
    PELOTON_ASSERT(0);
  }

 protected:
  optimizer::GroupID group_id_;

  GroupMarkerExpression(const GroupMarkerExpression &other)
      : AbstractExpression(other), group_id_(other.group_id_) {}
};

}  // namespace expression
}  // namespace peloton
