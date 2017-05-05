//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// case_expression.h
//
// Identification: src/include/expression/case_expression.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// CaseExpression
//===----------------------------------------------------------------------===//

class CaseExpression : public AbstractExpression {
 public:
  using AbsExprPtr = std::unique_ptr<AbstractExpression>;
  using WhenClause = std::pair<AbsExprPtr,AbsExprPtr>;

  CaseExpression(type::Type::TypeId type_id, AbsExprPtr argument,
                 std::vector<WhenClause> &when_clauses,
                 AbsExprPtr dflt_expression)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, type_id),
        argument_(std::move(argument)),
        clauses_(std::move(when_clauses)),
        dfltexpr_(std::move(dflt_expression)) {
    // If the argument is provided, then we create When Clauses based on it
    // Otherwise, we assume that the When Clauses provided are sufficient
    if (argument_ != nullptr) {
      for (uint32_t i = 0; i < clauses_.size(); i++)
		clauses_[i].first.reset(
           new peloton::expression::ComparisonExpression(
              peloton::ExpressionType::COMPARE_EQUAL, argument_->Copy(),
              clauses_[i].first->Copy()));
    }
  }
  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    for (auto &clause : clauses_) {
      auto result = clause.first->Evaluate(tuple1, tuple2, context);
      if (result.IsTrue())
        return clause.second->Evaluate(tuple1, tuple2, context);
    }
    if (dfltexpr_ == nullptr)
      return type::ValueFactory::GetNullValueByType(return_value_type_);
    return dfltexpr_->Evaluate(tuple1, tuple2, context);
  }

  AbstractExpression* Copy() const override { 
    std::vector<WhenClause> copies;
    for (auto &clause : clauses_)
      copies.push_back(WhenClause(AbsExprPtr(clause.first->Copy()),
                                  AbsExprPtr(clause.second->Copy())));
	return new CaseExpression(return_value_type_, nullptr, copies,
                              dfltexpr_ != nullptr ?
                                  AbsExprPtr(dfltexpr_->Copy()) : nullptr);
  }

  virtual void Accept(SqlNodeVisitor* v) { v->Visit(this); }

  size_t GetWhenClauseSize() const { return clauses_.size(); }

  const std::vector<WhenClause> &GetWhenClauses() const { return clauses_; }

  AbstractExpression *GetWhenClauseCond(int index) const {
    if (index < 0 || index >= (int)clauses_.size())
      return nullptr;
    return clauses_[index].first.get();
  }

  AbstractExpression *GetWhenClauseResult(int index) const {
    if (index < 0 || index >= (int)clauses_.size())
      return nullptr;
    return clauses_[index].second.get();
  }

  AbstractExpression *GetDefault() const { return dfltexpr_.get(); };

  // Attribute binding
  void PerformBinding(const std::vector<const planner::BindingContext *> &
                          binding_contexts) override {
    for (auto &clause : clauses_) {
      clause.first->PerformBinding(binding_contexts);
      clause.second->PerformBinding(binding_contexts);
    }
    if (dfltexpr_ != nullptr)
      dfltexpr_->PerformBinding(binding_contexts);
  }

 private:
  AbsExprPtr argument_;
  std::vector<WhenClause> clauses_;
  AbsExprPtr dfltexpr_;
};

}  // End expression namespace
}  // End peloton namespace
