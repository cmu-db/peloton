//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// absexpr_test.cpp
//
// Identification: test/optimizer/absexpr_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <algorithm>
#include <vector>
#include "common/harness.h"

#include "function/functions.h"
#include "optimizer/operators.h"
#include "optimizer/rewriter.h"
#include "expression/aggregate_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/subquery_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/star_expression.h"
#include "expression/case_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/function_expression.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "optimizer/rule_rewrite.h"
#include "parser/postgresparser.h"

namespace peloton {

namespace test {

using namespace optimizer;

class AbsExprTest : public PelotonTest {
 public:
  // Returns expression of (Constant = Tuple Value)
  expression::AbstractExpression *GetTVEqualCVExpression(std::string col, int val) {
    auto val_e = GetConstantExpression(val);
    auto tuple = new expression::TupleValueExpression(std::move(col));
    auto cmp = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, val_e, tuple
    );

    return cmp;
  }

  // Returns ConstantExpression(val)
  expression::AbstractExpression *GetConstantExpression(int val) {
    auto value = type::ValueFactory::GetIntegerValue(val);
    return new expression::ConstantValueExpression(value);
  }
};

TEST_F(AbsExprTest, CompareTest) {
  std::vector<ExpressionType> compares = {
    ExpressionType::COMPARE_EQUAL,
    ExpressionType::COMPARE_NOTEQUAL,
    ExpressionType::COMPARE_LESSTHAN,
    ExpressionType::COMPARE_GREATERTHAN,
    ExpressionType::COMPARE_LESSTHANOREQUALTO,
    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
    ExpressionType::COMPARE_LIKE,
    ExpressionType::COMPARE_NOTLIKE,
    ExpressionType::COMPARE_IN,
    ExpressionType::COMPARE_DISTINCT_FROM
  };

  auto left = new expression::ParameterValueExpression(0);
  auto right = new expression::ParameterValueExpression(1);
  for (auto type : compares) {
    auto cmp_expr = std::make_shared<expression::ComparisonExpression>(type, left->Copy(), right->Copy());
    AbsExprNode op = AbsExprNode(cmp_expr);
    expression::AbstractExpression *rebuilt = op.CopyWithChildren({left->Copy(), right->Copy()});
    EXPECT_TRUE(rebuilt != nullptr);

    EXPECT_EQ(cmp_expr->GetExpressionType(), rebuilt->GetExpressionType());
    EXPECT_EQ(cmp_expr->GetValueType(), rebuilt->GetValueType());
    EXPECT_EQ(cmp_expr->GetChildrenSize(), rebuilt->GetChildrenSize());

    EXPECT_EQ(*(cmp_expr->GetChild(0)), *(rebuilt->GetChild(0)));
    EXPECT_EQ(*(cmp_expr->GetChild(1)), *(rebuilt->GetChild(1)));

    auto l_child = dynamic_cast<const expression::ParameterValueExpression*>(rebuilt->GetChild(0));
    auto r_child = dynamic_cast<const expression::ParameterValueExpression*>(rebuilt->GetChild(1));
    EXPECT_TRUE(l_child != nullptr && r_child != nullptr);
    EXPECT_TRUE(l_child->GetValueIdx() == 0 && r_child->GetValueIdx() == 1);

    delete rebuilt;
  }

  delete left;
  delete right;
}

TEST_F(AbsExprTest, ConjunctionTest) {
  std::vector<ExpressionType> compares = {
    ExpressionType::CONJUNCTION_AND,
    ExpressionType::CONJUNCTION_OR
  };

  auto tval = type::ValueFactory::GetBooleanValue(true);
  auto fval = type::ValueFactory::GetBooleanValue(false);
  auto left = new expression::ConstantValueExpression(tval);
  auto right = new expression::ConstantValueExpression(fval);
  for (auto type : compares) {
    auto cmp_expr = std::make_shared<expression::ConjunctionExpression>(type, left->Copy(), right->Copy());
    AbsExprNode op = AbsExprNode(cmp_expr);
    expression::AbstractExpression *rebuilt = op.CopyWithChildren({left->Copy(), right->Copy()});
    EXPECT_TRUE(rebuilt != nullptr);

    EXPECT_EQ(cmp_expr->GetExpressionType(), rebuilt->GetExpressionType());
    EXPECT_EQ(cmp_expr->GetValueType(), rebuilt->GetValueType());
    EXPECT_EQ(cmp_expr->GetChildrenSize(), rebuilt->GetChildrenSize());

    auto l_child = dynamic_cast<const expression::ConstantValueExpression*>(rebuilt->GetChild(0));
    auto r_child = dynamic_cast<const expression::ConstantValueExpression*>(rebuilt->GetChild(1));
    EXPECT_TRUE(l_child != nullptr && r_child != nullptr);
    EXPECT_TRUE(l_child->ExactlyEquals(*left) && r_child->ExactlyEquals(*right));
    delete rebuilt;
  }

  delete left;
  delete right;
}

TEST_F(AbsExprTest, OperatorTest) {
  std::vector<ExpressionType> operators = {
    ExpressionType::OPERATOR_PLUS,
    ExpressionType::OPERATOR_MINUS,
    ExpressionType::OPERATOR_MULTIPLY,
    ExpressionType::OPERATOR_DIVIDE,
    ExpressionType::OPERATOR_CONCAT,
    ExpressionType::OPERATOR_MOD,
  };

  std::vector<ExpressionType> single_ops = {
    ExpressionType::OPERATOR_NOT,
    ExpressionType::OPERATOR_IS_NULL,
    ExpressionType::OPERATOR_IS_NOT_NULL,
    ExpressionType::OPERATOR_EXISTS
  };

  auto left = GetConstantExpression(25);
  auto right = GetConstantExpression(30);
  for (auto type : operators) {
    auto op_expr = std::make_shared<expression::OperatorExpression>(type, type::TypeId::INTEGER, left->Copy(), right->Copy());
    op_expr->DeduceExpressionType();

    AbsExprNode op = AbsExprNode(op_expr);
    expression::AbstractExpression *rebuilt = op.CopyWithChildren({left->Copy(), right->Copy()});
    EXPECT_TRUE(rebuilt != nullptr);
    rebuilt->DeduceExpressionType();

    EXPECT_EQ(op_expr->GetExpressionType(), rebuilt->GetExpressionType());
    EXPECT_EQ(op_expr->GetValueType(), rebuilt->GetValueType());
    EXPECT_EQ(op_expr->GetChildrenSize(), rebuilt->GetChildrenSize());

    auto l_child = dynamic_cast<const expression::ConstantValueExpression*>(rebuilt->GetChild(0));
    auto r_child = dynamic_cast<const expression::ConstantValueExpression*>(rebuilt->GetChild(1));
    EXPECT_TRUE(l_child != nullptr && r_child != nullptr);
    EXPECT_TRUE(l_child->ExactlyEquals(*(op_expr->GetChild(0))));
    EXPECT_TRUE(r_child->ExactlyEquals(*(op_expr->GetChild(1))));
    EXPECT_TRUE(l_child->ExactlyEquals(*left));
    EXPECT_TRUE(r_child->ExactlyEquals(*right));

    delete rebuilt;
  }

  for (auto type : single_ops) {
    auto op_expr = std::make_shared<expression::OperatorExpression>(type, type::TypeId::INTEGER, left->Copy(), nullptr);
    op_expr->DeduceExpressionType();

    AbsExprNode op = AbsExprNode(op_expr);
    expression::AbstractExpression *rebuilt = op.CopyWithChildren({left->Copy()});
    EXPECT_TRUE(rebuilt != nullptr);
    rebuilt->DeduceExpressionType();

    EXPECT_EQ(op_expr->GetExpressionType(), rebuilt->GetExpressionType());
    EXPECT_EQ(op_expr->GetValueType(), rebuilt->GetValueType());
    EXPECT_EQ(op_expr->GetChildrenSize(), rebuilt->GetChildrenSize());

    auto l_child = dynamic_cast<const expression::ConstantValueExpression*>(rebuilt->GetChild(0));
    EXPECT_TRUE(l_child != nullptr);
    EXPECT_TRUE(l_child->ExactlyEquals(*(op_expr->GetChild(0))));
    EXPECT_TRUE(l_child->ExactlyEquals(*left));

    delete rebuilt;
  }

  delete left;
  delete right;
}

TEST_F(AbsExprTest, OperatorUnaryMinusTest) {
  auto left = GetConstantExpression(25);
  auto unary = std::make_shared<expression::OperatorUnaryMinusExpression>(left->Copy());

  AbsExprNode op = AbsExprNode(unary);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({left->Copy()});
  EXPECT_TRUE(rebuilt != nullptr);
    
  EXPECT_EQ(unary->GetExpressionType(), rebuilt->GetExpressionType());
  EXPECT_EQ(unary->GetValueType(), rebuilt->GetValueType());
  EXPECT_EQ(unary->GetChildrenSize(), rebuilt->GetChildrenSize());
  EXPECT_TRUE(unary->GetChild(0)->ExactlyEquals(*(rebuilt->GetChild(0))));
  EXPECT_TRUE(left->ExactlyEquals(*(rebuilt->GetChild(0))));
  delete rebuilt;
  delete left;
}

TEST_F(AbsExprTest, StarTest) {
  auto expr = std::make_shared<expression::StarExpression>();
  AbsExprNode op = AbsExprNode(expr);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({});

  EXPECT_EQ(*expr, *rebuilt);
  delete rebuilt;
}

TEST_F(AbsExprTest, ValueConstantTest) {
  auto cv_expr = dynamic_cast<expression::ConstantValueExpression*>(GetConstantExpression(721));
  auto expr = std::shared_ptr<expression::ConstantValueExpression>(cv_expr);
  AbsExprNode op = AbsExprNode(expr);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({});

  EXPECT_EQ(*expr, *rebuilt); // this does not check value
  EXPECT_EQ(expr->GetValueType(), rebuilt->GetValueType());

  auto lvalue = expr->GetValue();

  auto rebuilt_val = dynamic_cast<expression::ConstantValueExpression*>(rebuilt);
  auto rvalue = rebuilt_val->GetValue();
  EXPECT_TRUE(lvalue.CheckComparable(expr->GetValue()));
  EXPECT_TRUE(lvalue.CheckComparable(rvalue));
  EXPECT_TRUE(lvalue.CompareEquals(expr->GetValue()) == CmpBool::CmpTrue);
  EXPECT_TRUE(lvalue.CompareEquals(rvalue) == CmpBool::CmpTrue);
  delete rebuilt;
}

TEST_F(AbsExprTest, ValueParameterTest) {
  auto expr = std::make_shared<expression::ParameterValueExpression>(15);
  AbsExprNode op = AbsExprNode(expr);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({});

  EXPECT_EQ(*expr, *rebuilt); // does not check value_idx_

  auto rebuilt_val = dynamic_cast<expression::ParameterValueExpression*>(rebuilt);
  EXPECT_EQ(expr->GetValueIdx(), rebuilt_val->GetValueIdx());
  delete rebuilt;
}

TEST_F(AbsExprTest, ValueTupleTest) {
  auto expr_col = std::make_shared<expression::TupleValueExpression>("col");
  expr_col->SetTupleValueExpressionParams(type::TypeId::INTEGER, 1, 1);
  expr_col->SetTableName("tbl");

  AbsExprNode op = AbsExprNode(expr_col);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({});

  EXPECT_EQ(*expr_col, *rebuilt); // checks tbl_name, col_name

  auto rebuilt_val = dynamic_cast<expression::TupleValueExpression*>(rebuilt);
  EXPECT_EQ(rebuilt_val->GetColumnId(), expr_col->GetColumnId());
  EXPECT_EQ(rebuilt_val->GetTableName(), expr_col->GetTableName());
  EXPECT_EQ(rebuilt_val->GetColumnName(), expr_col->GetColumnName());
  EXPECT_EQ(rebuilt_val->GetTupleId(), expr_col->GetTupleId());
  EXPECT_EQ(rebuilt_val->GetValueType(), expr_col->GetValueType());
  delete rebuilt;
}

TEST_F(AbsExprTest, AggregateNodeTest) {
  std::vector<ExpressionType> aggregates = {
    ExpressionType::AGGREGATE_COUNT,
    ExpressionType::AGGREGATE_SUM,
    ExpressionType::AGGREGATE_MIN,
    ExpressionType::AGGREGATE_MAX,
    ExpressionType::AGGREGATE_AVG
  };

  // Generic aggregation
  for (auto type : aggregates) {
    auto child = new expression::TupleValueExpression("col_a");
    auto agg_expr = std::make_shared<expression::AggregateExpression>(type, true, child->Copy());
    agg_expr->DeduceExpressionType();

    AbsExprNode op = AbsExprNode(agg_expr);
    expression::AbstractExpression *rebuilt = op.CopyWithChildren({child->Copy()});
    EXPECT_TRUE(rebuilt != nullptr);

    rebuilt->DeduceExpressionType();
    EXPECT_EQ(agg_expr->GetExpressionType(), rebuilt->GetExpressionType());
    EXPECT_EQ(agg_expr->GetValueType(), rebuilt->GetValueType());
    EXPECT_EQ(agg_expr->GetChildrenSize(), rebuilt->GetChildrenSize());
    EXPECT_EQ(*(agg_expr->GetChild(0)), *(rebuilt->GetChild(0)));

    EXPECT_TRUE(agg_expr->distinct_);
    EXPECT_TRUE(rebuilt->distinct_);

    delete rebuilt;
    delete child;
  }
    
  // COUNT (*) Aggregation
  auto child = new expression::StarExpression();
  auto agg_expr = std::make_shared<expression::AggregateExpression>(ExpressionType::AGGREGATE_COUNT, true, child);

  agg_expr->DeduceExpressionType();
  EXPECT_TRUE(agg_expr->GetExpressionType() == ExpressionType::AGGREGATE_COUNT_STAR);

  AbsExprNode op = AbsExprNode(agg_expr);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({});
  rebuilt->DeduceExpressionType();

  EXPECT_EQ(agg_expr->GetExpressionType(), rebuilt->GetExpressionType());
  EXPECT_EQ(agg_expr->GetValueType(), rebuilt->GetValueType());
  EXPECT_EQ(agg_expr->distinct_, rebuilt->distinct_);
  EXPECT_EQ(rebuilt->GetChildrenSize(), 0);
  delete rebuilt;
}

TEST_F(AbsExprTest, CaseExpressionTest) {
  auto where1 = expression::CaseExpression::AbsExprPtr(GetTVEqualCVExpression("col_a", 1));
  auto where2 = expression::CaseExpression::AbsExprPtr(GetTVEqualCVExpression("col_b", 2));
  auto where3 = expression::CaseExpression::AbsExprPtr(GetTVEqualCVExpression("col_c", 3));
  auto def_c = expression::CaseExpression::AbsExprPtr(GetConstantExpression(4));

  auto res1 = expression::CaseExpression::AbsExprPtr(GetConstantExpression(1));
  auto res2 = expression::CaseExpression::AbsExprPtr(GetConstantExpression(2));
  auto res3 = expression::CaseExpression::AbsExprPtr(GetConstantExpression(3));
  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(std::move(where1), std::move(res1)));
  clauses.push_back(expression::CaseExpression::WhenClause(std::move(where2), std::move(res2)));
  clauses.push_back(expression::CaseExpression::WhenClause(std::move(where3), std::move(res3)));

  auto expr = std::make_shared<expression::CaseExpression>(type::TypeId::INTEGER, clauses, std::move(def_c));
  AbsExprNode op = AbsExprNode(expr);
  expression::AbstractExpression *rebuilt = op.CopyWithChildren({});

  // Checks every clause except for ConstantValue values
  EXPECT_EQ(*expr, *rebuilt);

  auto rebuilt_c = dynamic_cast<expression::CaseExpression*>(rebuilt);
  EXPECT_TRUE(rebuilt_c->GetWhenClauseSize() == 3);

  // Check each when clause
  for (int i = 0; i < 3; i++) {
    auto res = rebuilt_c->GetWhenClauseResult(i);
    auto res_c = dynamic_cast<expression::ConstantValueExpression*>(res);
    EXPECT_TRUE(res_c != nullptr && res_c->GetValue().GetTypeId() == type::TypeId::INTEGER);
    EXPECT_TRUE(type::ValuePeeker::PeekInteger(res_c->GetValue()) == (i + 1));

    auto cond = rebuilt_c->GetWhenClauseCond(i);
    EXPECT_TRUE(cond->GetExpressionType() == ExpressionType::COMPARE_EQUAL);
    EXPECT_TRUE(cond->GetChildrenSize() == 2);

    auto rval = cond->GetChild(0);
    EXPECT_TRUE(rval->GetExpressionType() == ExpressionType::VALUE_CONSTANT);

    auto rval_c = dynamic_cast<const expression::ConstantValueExpression*>(rval);
    EXPECT_TRUE(rval_c != nullptr && rval_c->GetValue().GetTypeId() == type::TypeId::INTEGER);
    EXPECT_TRUE(type::ValuePeeker::PeekInteger(rval_c->GetValue()) == (i + 1));
  }

  // Check default clause
  auto def_expr = dynamic_cast<expression::ConstantValueExpression*>(rebuilt_c->GetDefault());
  EXPECT_TRUE(def_expr != nullptr);
  EXPECT_TRUE(def_expr->GetValue().GetTypeId() == type::TypeId::INTEGER);
  EXPECT_TRUE(type::ValuePeeker::PeekInteger(def_expr->GetValue()) == 4);
  delete rebuilt;
}

TEST_F(AbsExprTest, SubqueryTest) {
  std::vector<std::unique_ptr<parser::SQLStatement>> stmts;
  {
    auto parser = parser::PostgresParser::GetInstance();
    auto query = "SELECT * from foo";
    std::unique_ptr<parser::SQLStatementList> stmt_list(parser.BuildParseTree(query).release());
    stmts = std::move(stmt_list->statements);
  }

  EXPECT_TRUE(stmts.size() == 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::SELECT);
  parser::SelectStatement *sel = dynamic_cast<parser::SelectStatement*>(stmts[0].release());
  EXPECT_TRUE(sel != nullptr);

  auto expr = std::make_shared<expression::SubqueryExpression>();
  expr->SetSubSelect(sel);

  AbsExprNode container = AbsExprNode(expr);
  expression::AbstractExpression *rebuild = container.CopyWithChildren({});

  EXPECT_EQ(rebuild->GetExpressionType(), expr->GetExpressionType());
  EXPECT_EQ(rebuild->GetChildrenSize(), expr->GetChildrenSize());

  auto rebuild_s = dynamic_cast<expression::SubqueryExpression*>(rebuild);
  EXPECT_TRUE(rebuild_s != nullptr);
  EXPECT_TRUE(rebuild_s->GetSubSelect() == expr->GetSubSelect());
  delete rebuild;
}

TEST_F(AbsExprTest, FunctionExpressionTest) {
  std::vector<expression::AbstractExpression*> child1;
  std::vector<expression::AbstractExpression*> child2;
  std::vector<type::TypeId> types;
  for (int i = 0; i < 10; i++) {
    child1.push_back(GetConstantExpression(i));
    child2.push_back(GetConstantExpression(i));
    types.push_back(type::TypeId::INTEGER);
  }

  auto func = [](const std::vector<type::Value> &val) {
    (void)val;
    return type::ValueFactory::GetIntegerValue(5);
  };
  function::BuiltInFuncType func_ptr = { static_cast<OperatorId>(1), func };
  auto expr = std::make_shared<expression::FunctionExpression>("func", child1);
  expr->SetBuiltinFunctionExpressionParameters(func_ptr, type::TypeId::INTEGER, types);

  AbsExprNode container = AbsExprNode(expr);
  expression::AbstractExpression* rebuild = container.CopyWithChildren(child2);

  EXPECT_EQ(rebuild->GetExpressionType(), expr->GetExpressionType());
  EXPECT_EQ(rebuild->GetChildrenSize(), expr->GetChildrenSize());

  auto rebuild_c = dynamic_cast<expression::FunctionExpression*>(rebuild);
  EXPECT_TRUE(rebuild_c != nullptr);
  EXPECT_EQ(rebuild_c->GetFuncName(), expr->GetFuncName());
  EXPECT_EQ(rebuild_c->GetFunc().op_id, expr->GetFunc().op_id);
  EXPECT_EQ(rebuild_c->GetFunc().impl, expr->GetFunc().impl);
  EXPECT_EQ(rebuild_c->GetArgTypes(), expr->GetArgTypes());
  EXPECT_EQ(rebuild_c->IsUDF(), expr->IsUDF());

  for (int i = 0; i < 10; i++) {
    auto l_child = rebuild_c->GetChild(i);
    auto r_child = expr->GetChild(i);
    EXPECT_TRUE(l_child != r_child && l_child->GetExpressionType() == ExpressionType::VALUE_CONSTANT);

    auto l_cast = dynamic_cast<const expression::ConstantValueExpression*>(l_child);
    auto r_cast = dynamic_cast<const expression::ConstantValueExpression*>(r_child);
    EXPECT_TRUE(l_cast != nullptr && r_cast != nullptr);
    EXPECT_TRUE(l_cast->ExactlyEquals(*r_cast));
    EXPECT_TRUE(type::ValuePeeker::PeekInteger(l_cast->GetValue()) == i);
    EXPECT_TRUE(type::ValuePeeker::PeekInteger(r_cast->GetValue()) == i);
  }

  delete rebuild;
}

}  // namespace test
}  // namespace peloton
