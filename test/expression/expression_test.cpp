//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_test.cpp
//
// Identification: test/expression/expression_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>
#include <queue>

#include "common/harness.h"

#include "expression/abstract_expression.h"
#include "common/types.h"
#include "common/value_peeker.h"
#include "storage/tuple.h"

#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/vector_expression.h"
#include "expression/operator_expression.h"
#include "expression/case_expression.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Expression Tests
//===--------------------------------------------------------------------===//

class ExpressionTest : public PelotonTest {};

/*
   Description of test:

   1. This test defines a data structure for each expression type with
   unique fields.

   3. Using this utilities, the test defines several expressions (in
   std::queue) formats and PL_ASSERTs on the expected result.

 */

/*
 *  Abstract expression mock object
 */
class AE {
 public:
  AE(ExpressionType et, ValueType vt, int vs)
      : m_type(et),
        m_valueType(vt),
        m_valueSize(vs),
        left(nullptr),
        right(nullptr) {}

  virtual ~AE() {
    delete left;
    delete right;
  }

  ExpressionType m_type;  // TYPE
  ValueType m_valueType;  // VALUE_TYPE
  int m_valueSize;        // VALUE_SIZE

  // to build a tree
  AE *left;
  AE *right;
};

/*
 * constant value expression mock object
 */
class CV : public AE {
 public:
  CV(ExpressionType et, ValueType vt, int vs, int64_t v)
      : AE(et, vt, vs), m_intValue(v) {
    m_stringValue = nullptr;
    m_doubleValue = 0.0;
  }

  CV(ExpressionType et, ValueType vt, int vs, char *v)
      : AE(et, vt, vs), m_stringValue(strdup(v)) {
    m_intValue = 0;
    m_doubleValue = 0.0;
  }

  CV(ExpressionType et, ValueType vt, int vs, double v)
      : AE(et, vt, vs), m_doubleValue(v) {
    m_stringValue = nullptr;
    m_intValue = 0;
  }

  ~CV() {}

  int64_t m_intValue;    // VALUE
  char *m_stringValue;   // VALUE
  double m_doubleValue;  // VALUE
};

/*
 * parameter value expression mock object
 */
class PV : public AE {
 public:
  PV(ExpressionType et, ValueType vt, int vs, int pi)
      : AE(et, vt, vs), m_paramIdx(pi) {}

  int m_paramIdx;  // PARAM_IDX
};

/*
 * tuple value expression mock object
 */
class TV : public AE {
 public:
  TV(ExpressionType et, ValueType vt, int vs, int ci, const char *tn,
     const char *cn, const char *ca)
      : AE(et, vt, vs),
        m_columnIdx(ci),
        m_tableName(strdup(tn)),
        m_colName(strdup(cn)),
        m_colAlias(strdup(ca)) {}

  ~TV() {
    delete m_tableName;
    delete m_colName;
    delete m_colAlias;
  }

  int m_columnIdx;    // COLUMN_IDX
  char *m_tableName;  // TABLE_NAME
  char *m_colName;    // COLUMN_NAME
  char *m_colAlias;   // COLUMN_ALIAS
};

/*
   helpers to build trivial left-associative trees
   that is (a, *, b, +, c) returns (a * b) + c
   and (a, +, b, * c) returns (a + b) * c
 */

AE *Join(AE *op, AE *left, AE *right) {
  op->left = left;
  op->right = right;
  return op;
}

AE *MakeTree(AE *tree, std::queue<AE *> &q) {
  if (!q.empty()) {
    AE *left, *right, *op;
    if (tree) {
      left = tree;
    } else {
      left = q.front();
      q.pop();
    }

    op = q.front();
    q.pop();
    right = q.front();
    q.pop();

    tree = MakeTree(Join(op, left, right), q);
  }
  return tree;
}

TEST_F(ExpressionTest, SimpleFilter) {
  // WHERE id = 20

  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(20));
  expression::ComparisonExpression<expression::CmpEq> *equal =
      new expression::ComparisonExpression<expression::CmpEq>(
          EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  // TUPLE

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  tuple->SetValue(0, ValueFactory::GetIntegerValue(20), nullptr);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(45), nullptr);

  LOG_INFO("%s", equal->GetInfo().c_str());
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), true);

  tuple->SetValue(0, ValueFactory::GetIntegerValue(50), nullptr);
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), false);

  // delete the root to destroy the full tree.
  delete equal;

  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, SimpleFilterCopyTest) {
  // WHERE id = 20

  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(20));
  expression::ComparisonExpression<expression::CmpEq> *o_equal =
      new expression::ComparisonExpression<expression::CmpEq>(
          EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  expression::ComparisonExpression<expression::CmpEq> *equal =
      dynamic_cast<expression::ComparisonExpression<expression::CmpEq> *>(
          o_equal->Copy());

  // TUPLE

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  tuple->SetValue(0, ValueFactory::GetIntegerValue(20), nullptr);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(45), nullptr);

  LOG_INFO("%s", equal->GetInfo().c_str());
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), true);

  tuple->SetValue(0, ValueFactory::GetIntegerValue(50), nullptr);
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), false);

  // delete the root to destroy the full tree.
  delete equal;
  delete o_equal;

  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, SimpleInFilter) {
  // WHERE id in (15, 20)

  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp1 =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(15));
  expression::ConstantValueExpression *const_val_exp2 =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(20));

  std::vector<expression::AbstractExpression *> vec_const_exprs;
  vec_const_exprs.push_back(const_val_exp1);
  vec_const_exprs.push_back(const_val_exp2);

  expression::VectorExpression *vec_exp =
      new expression::VectorExpression(VALUE_TYPE_ARRAY, vec_const_exprs);

  expression::ComparisonExpression<expression::CmpIn> *equal =
      new expression::ComparisonExpression<expression::CmpIn>(
          EXPRESSION_TYPE_COMPARE_IN, tup_val_exp, vec_exp);

  // TUPLE

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  tuple->SetValue(0, ValueFactory::GetIntegerValue(20), nullptr);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(45), nullptr);

  LOG_INFO("%s", equal->GetInfo().c_str());
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), true);

  tuple->SetValue(0, ValueFactory::GetIntegerValue(50), nullptr);
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), false);

  // delete the root to destroy the full tree.
  delete equal;

  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, SimpleInFilterCopyTest) {
  // WHERE id in (15, 20)

  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp1 =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(15));
  expression::ConstantValueExpression *const_val_exp2 =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(20));

  std::vector<expression::AbstractExpression *> vec_const_exprs;
  vec_const_exprs.push_back(const_val_exp1);
  vec_const_exprs.push_back(const_val_exp2);

  expression::VectorExpression *vec_exp =
      new expression::VectorExpression(VALUE_TYPE_ARRAY, vec_const_exprs);

  expression::ComparisonExpression<expression::CmpIn> *o_equal =
      new expression::ComparisonExpression<expression::CmpIn>(
          EXPRESSION_TYPE_COMPARE_IN, tup_val_exp, vec_exp);

  expression::ComparisonExpression<expression::CmpIn> *equal =
      dynamic_cast<expression::ComparisonExpression<expression::CmpIn> *>(
          o_equal->Copy());

  // TUPLE

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "B", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  tuple->SetValue(0, ValueFactory::GetIntegerValue(20), nullptr);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(45), nullptr);

  LOG_INFO("%s", equal->GetInfo().c_str());
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), true);

  tuple->SetValue(0, ValueFactory::GetIntegerValue(50), nullptr);
  EXPECT_EQ(equal->Evaluate(tuple, NULL, NULL).IsTrue(), false);

  // delete the root to destroy the full tree.
  delete equal;
  delete o_equal;

  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, SimpleCase) {
  // CASE WHEN i=1 THEN 2 ELSE 3 END

  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp_1 =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(1));
  expression::ConstantValueExpression *const_val_exp_2 =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(2));
  expression::ConstantValueExpression *const_val_exp_3 =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(3));

  expression::ComparisonExpression<expression::CmpEq> *when_cond =
      new expression::ComparisonExpression<expression::CmpEq>(
          EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp_1);

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbstractExprPtr(when_cond),
      expression::CaseExpression::AbstractExprPtr(const_val_exp_2)));

  expression::CaseExpression *case_expression = new expression::CaseExpression(
      VALUE_TYPE_INTEGER, clauses,
      expression::CaseExpression::AbstractExprPtr(const_val_exp_3));

  // TUPLE

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "i", true);
  catalog::Column column2(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "f", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  tuple->SetValue(0, ValueFactory::GetIntegerValue(1), nullptr);
  tuple->SetValue(1, ValueFactory::GetDoubleValue(1.5), nullptr);

  Value result = case_expression->Evaluate(tuple, nullptr, nullptr);

  // Test with A = 1, should get 2
  EXPECT_EQ(ValuePeeker::PeekAsInteger(result), 2);

  tuple->SetValue(0, ValueFactory::GetIntegerValue(2), nullptr);
  tuple->SetValue(1, ValueFactory::GetDoubleValue(-1.5), nullptr);

  result = case_expression->Evaluate(tuple, nullptr, nullptr);

  // Test with A = 2, should get 3
  EXPECT_EQ(ValuePeeker::PeekAsInteger(result), 3);

  delete case_expression;
  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, SimpleCaseCopyTest) {
  // CASE WHEN i=1 THEN 2 ELSE 3 END

  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp_1 =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(1));
  expression::ConstantValueExpression *const_val_exp_2 =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(2));
  expression::ConstantValueExpression *const_val_exp_3 =
      new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(3));

  expression::ComparisonExpression<expression::CmpEq> *when_cond =
      new expression::ComparisonExpression<expression::CmpEq>(
          EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp_1);

  std::vector<expression::CaseExpression::WhenClause> clauses;
  clauses.push_back(expression::CaseExpression::WhenClause(
      expression::CaseExpression::AbstractExprPtr(when_cond),
      expression::CaseExpression::AbstractExprPtr(const_val_exp_2)));

  expression::CaseExpression *o_case_expression =
      new expression::CaseExpression(
          VALUE_TYPE_INTEGER, clauses,
          expression::CaseExpression::AbstractExprPtr(const_val_exp_3));

  expression::CaseExpression *case_expression =
      dynamic_cast<expression::CaseExpression *>(o_case_expression->Copy());
  // TUPLE

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "i", true);
  catalog::Column column2(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "f", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  tuple->SetValue(0, ValueFactory::GetIntegerValue(1), nullptr);
  tuple->SetValue(1, ValueFactory::GetDoubleValue(1.5), nullptr);

  Value result = case_expression->Evaluate(tuple, nullptr, nullptr);

  // Test with A = 1, should get 2
  EXPECT_EQ(ValuePeeker::PeekAsInteger(result), 2);

  tuple->SetValue(0, ValueFactory::GetIntegerValue(2), nullptr);
  tuple->SetValue(1, ValueFactory::GetDoubleValue(-1.5), nullptr);

  result = case_expression->Evaluate(tuple, nullptr, nullptr);

  // Test with A = 2, should get 3
  EXPECT_EQ(ValuePeeker::PeekAsInteger(result), 3);

  delete case_expression;
  delete o_case_expression;
  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, UnaryMinus) {
  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp_int =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::TupleValueExpression *tup_val_exp_double =
      new expression::TupleValueExpression(VALUE_TYPE_DOUBLE, 0, 1);

  // TUPLE
  expression::OperatorUnaryMinusExpression *unary_minus_int =
      new expression::OperatorUnaryMinusExpression(tup_val_exp_int);

  expression::OperatorUnaryMinusExpression *unary_minus_double =
      new expression::OperatorUnaryMinusExpression(tup_val_exp_double);

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "i", true);
  catalog::Column column2(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "f", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  // Test with A = 1, should get -1
  tuple->SetValue(0, ValueFactory::GetIntegerValue(1), nullptr);
  Value result = unary_minus_int->Evaluate(tuple, nullptr, nullptr);
  EXPECT_EQ(ValuePeeker::PeekAsInteger(result), -1);

  // Test with A = 1.5, should get -1.5
  tuple->SetValue(1, ValueFactory::GetDoubleValue(1.5), nullptr);
  result = unary_minus_double->Evaluate(tuple, nullptr, nullptr);
  EXPECT_EQ(ValuePeeker::PeekDouble(result), -1.5);

  delete unary_minus_double;
  delete unary_minus_int;
  delete schema;
  delete tuple;
}

TEST_F(ExpressionTest, UnaryMinusCopyTest) {
  // EXPRESSION

  expression::TupleValueExpression *tup_val_exp_int =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::TupleValueExpression *tup_val_exp_double =
      new expression::TupleValueExpression(VALUE_TYPE_DOUBLE, 0, 1);

  // TUPLE
  expression::OperatorUnaryMinusExpression *o_unary_minus_int =
      new expression::OperatorUnaryMinusExpression(tup_val_exp_int);

  expression::OperatorUnaryMinusExpression *o_unary_minus_double =
      new expression::OperatorUnaryMinusExpression(tup_val_exp_double);

  expression::OperatorUnaryMinusExpression *unary_minus_int =
      dynamic_cast<expression::OperatorUnaryMinusExpression *>(
          o_unary_minus_int->Copy());

  expression::OperatorUnaryMinusExpression *unary_minus_double =
      dynamic_cast<expression::OperatorUnaryMinusExpression *>(
          o_unary_minus_double->Copy());

  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "i", true);
  catalog::Column column2(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "f", true);
  columns.push_back(column1);
  columns.push_back(column2);
  catalog::Schema *schema(new catalog::Schema(columns));

  storage::Tuple *tuple(new storage::Tuple(schema, true));

  // Test with A = 1, should get -1
  tuple->SetValue(0, ValueFactory::GetIntegerValue(1), nullptr);
  Value result = unary_minus_int->Evaluate(tuple, nullptr, nullptr);
  EXPECT_EQ(ValuePeeker::PeekAsInteger(result), -1);

  // Test with A = 1.5, should get -1.5
  tuple->SetValue(1, ValueFactory::GetDoubleValue(1.5), nullptr);
  result = unary_minus_double->Evaluate(tuple, nullptr, nullptr);
  EXPECT_EQ(ValuePeeker::PeekDouble(result), -1.5);

  delete unary_minus_double;
  delete unary_minus_int;
  delete o_unary_minus_double;
  delete o_unary_minus_int;
  delete schema;
  delete tuple;
}

}  // End test namespace
}  // End peloton namespace
