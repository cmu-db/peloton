//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// subquery_expression.h
//
// Identification: src/include/expression/subquery_expression.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

#include "common/internal_types.h"
#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "common/exception.h"
#include "planner/binding_context.h"
#include "parser/select_statement.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {

class SubqueryExpression : public AbstractExpression {
 public:
  SubqueryExpression();

  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override;

  AbstractExpression *Copy() const override;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  int DeriveDepth() override;

  // Accessors

  void SetSubSelect(parser::SelectStatement *select) {
    select_ = std::shared_ptr<parser::SelectStatement>(select);
  }

  std::shared_ptr<parser::SelectStatement> GetSubSelect() const {
    return select_;
  }

 protected:
  std::shared_ptr<parser::SelectStatement> select_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline SubqueryExpression::SubqueryExpression()
    : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID) {}

inline type::Value SubqueryExpression::Evaluate(
    UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
    UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
    UNUSED_ATTRIBUTE executor::ExecutorContext *context) const {
  // Only a place holder
  return type::ValueFactory::GetBooleanValue(false);
}

inline AbstractExpression *SubqueryExpression::Copy() const {
  // Hack. May need to implement deep copy parse tree node in the future
  auto new_expr = new SubqueryExpression();
  new_expr->select_ = this->select_;
  new_expr->depth_ = this->depth_;
  return new_expr;
}

inline int SubqueryExpression::DeriveDepth() {
  for (auto &select_ele : select_->select_list) {
    auto select_depth = select_ele->DeriveDepth();
    if (select_depth >= 0 && (depth_ == -1 || select_depth < depth_))
      depth_ = select_depth;
  }
  if (select_->where_clause != nullptr) {
    auto where_depth = select_->where_clause->GetDepth();
    if (where_depth >= 0 && where_depth < depth_) depth_ = where_depth;
  }
  return depth_;
}

}  // namespace expression
}  // namespace peloton
