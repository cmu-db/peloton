
#include "expression/abstract_expression.h"

#include "common/internal_types.h"
#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "common/exception.h"
#include "planner/binding_context.h"
#include "util/string_util.h"
#include "parser/select_statement.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {

class SubqueryExpression : public AbstractExpression {
 public:
  SubqueryExpression() : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID) {}

  virtual type::Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
                               UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                               UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    // Only a place holder
    return type::ValueFactory::GetBooleanValue(false);
  }

  virtual AbstractExpression *Copy() const override {
//    throw Exception("Copy() not supported in subquery_expression");
    // Hack. May need to implement deep copy parse tree node in the future
    auto new_expr = new SubqueryExpression();
    new_expr->select_ = this->select_;
    new_expr->depth_ = this->depth_;
    return new_expr;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  void SetSubSelect(parser::SelectStatement* select) {
    select_ = std::shared_ptr<parser::SelectStatement>(select);
  }

  std::shared_ptr<parser::SelectStatement> GetSubSelect() const { return select_; }

  virtual int DeriveDepth() override {
    for (auto& select_ele : select_->select_list) {
      auto select_depth = select_ele->DeriveDepth();
      if (select_depth >= 0 && (depth_ == -1 || select_depth < depth_))
        depth_ = select_depth;
    }
    if (select_->where_clause != nullptr) {
      auto where_depth = select_->where_clause->GetDepth();
      if (where_depth >=0 && where_depth < depth_)
        depth_ = where_depth;
    }
    return depth_;
  }

 protected:
  std::shared_ptr<parser::SelectStatement> select_;

};

}  // namespace expression
}  // namespace peloton
