#include <iostream>
#include <sstream>
#include <queue>

#include "gtest/gtest.h"
#include "harness.h"

#include "expression/abstract_expression.h"
#include "expression/expression.h"
#include "common/types.h"
#include "common/value_peeker.h"
#include "storage/tuple.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Expression Tests
//===--------------------------------------------------------------------===//

/*
   Description of test:

   1. This test defines a data structure for each expression type with
   unique fields.

   2. The test includes a helper to convert a std::queue of these structures
   into a tree of AbstractExpressions, using the expression factory via a
   json serialization.

   3. Using this utilities, the test defines several expressions (in
   std::queue) formats and asserts on the expected result.

   TODO: Unfortunately, the json_spirit serialization asserts when trying
   to read an int field from a value serialized as a string; need to figure
   out what's going on here .. and what the proper serialization method is.
 */


/*
 *  Abstract expression mock object
 */
class AE {
 public:
  AE(ExpressionType et, ValueType vt, int vs) :
    m_type(et), m_valueType(vt), m_valueSize(vs) , left(NULL), right(NULL) {}

  virtual ~AE() {
    delete left;
    delete right;
  }

  virtual json_spirit::Object SerializeValue() {
    json_spirit::Object json;
    Serialize(json);
    return json;
  }

  // this is how java serializes..
  // note derived class data follows the serialization of children
  virtual void Serialize(json_spirit::Object &json) {
    json.push_back(json_spirit::Pair("TYPE", json_spirit::Value(ExpressionToString(m_type))));
    json.push_back(json_spirit::Pair("VALUE_TYPE", json_spirit::Value(ValueToString(m_valueType))));
    json.push_back(json_spirit::Pair("VALUE_SIZE", json_spirit::Value(m_valueSize)));

    if (left)
      json.push_back(json_spirit::Pair("LEFT", left->SerializeValue()));
    if (right)
      json.push_back(json_spirit::Pair("RIGHT", right->SerializeValue()));
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
  CV(ExpressionType et, ValueType vt, int vs, int64_t v) :
    AE(et, vt, vs), m_jsontype (1), m_intValue(v)
 {
    m_stringValue = NULL;
    m_doubleValue = 0.0;
 }

  CV(ExpressionType et, ValueType vt, int vs, char * v) :
    AE(et, vt, vs), m_jsontype (1), m_stringValue(strdup(v))
  {
    m_intValue = 0;
    m_doubleValue = 0.0;
  }

  CV(ExpressionType et, ValueType vt, int vs, double v) :
    AE(et, vt, vs), m_jsontype (1), m_doubleValue(v)
  {
    m_stringValue = NULL;
    m_intValue = 0;
  }

  ~CV() {
  }

  virtual void Serialize(json_spirit::Object &json) {
    AE::Serialize(json);
    if (m_jsontype == 0)
      json.push_back(json_spirit::Pair("VALUE", json_spirit::Value(m_stringValue)));
    else if (m_jsontype == 1)
      json.push_back(json_spirit::Pair("VALUE", json_spirit::Value(m_intValue)));
    else if (m_jsontype == 2)
      json.push_back(json_spirit::Pair("VALUE", json_spirit::Value(m_doubleValue)));
  }

  int m_jsontype;  // 0 = string, 1 = int64_t, 2 = double

  int64_t  m_intValue;     // VALUE
  char    *m_stringValue;  // VALUE
  double   m_doubleValue;  // VALUE
};

/*
 * parameter value expression mock object
 */
class PV : public AE {
 public:
  PV(ExpressionType et, ValueType vt, int vs, int pi) :
    AE(et, vt, vs), m_paramIdx(pi) {}

  virtual void Serialize(json_spirit::Object &json) {
    AE::Serialize(json);
    json.push_back(json_spirit::Pair("PARAM_IDX", json_spirit::Value(m_paramIdx)));
  }

  int m_paramIdx;  // PARAM_IDX
};

/*
 * tuple value expression mock object
 */
class TV  : public AE {
 public:
  TV(ExpressionType et, ValueType vt, int vs, int ci,
     const char *tn, const char *cn, const char *ca) :
       AE(et, vt, vs), m_columnIdx(ci), m_tableName(strdup(tn)),
       m_colName(strdup(cn)), m_colAlias(strdup(ca)) {}

  ~TV() {
    delete m_tableName;
    delete m_colName;
    delete m_colAlias;
  }

  virtual void Serialize(json_spirit::Object &json) {
    AE::Serialize(json);
    json.push_back(json_spirit::Pair("COLUMN_IDX", json_spirit::Value(m_columnIdx)));
    json.push_back(json_spirit::Pair("TABLE_NAME", json_spirit::Value(m_tableName)));
    json.push_back(json_spirit::Pair("COLUMN_NAME", json_spirit::Value(m_colName)));
    json.push_back(json_spirit::Pair("COLUMN_ALIAS", json_spirit::Value(m_colAlias)));
  }

  int m_columnIdx;      // COLUMN_IDX
  char * m_tableName;   // TABLE_NAME
  char * m_colName;     // COLUMN_NAME
  char * m_colAlias;    // COLUMN_ALIAS
};

/*
   helpers to build trivial left-associative trees
   that is (a, *, b, +, c) returns (a * b) + c
   and (a, +, b, * c) returns (a + b) * c
 */

AE * Join(AE *op, AE *left, AE *right) {
  op->left = left;
  op->right = right;
  return op;
}

AE * MakeTree(AE *tree, std::queue<AE*> &q) {
  if (!q.empty()) {
    AE *left, *right, *op;
    if (tree) {
      left = tree;
    }
    else {
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

// boilerplate to turn the queue into a real AbstractExpression tree;
//   return the generated AE tree by reference to allow deletion (the queue
//   is emptied by the tree building process)
expression::AbstractExpression * convertToExpression(std::queue<AE*> &e) {
  AE *tree = MakeTree(NULL, e);
  json_spirit::Object json = tree->SerializeValue();
  expression::AbstractExpression * exp = expression::AbstractExpression::CreateExpressionTree(json);
  delete tree;
  return exp;
}


/*
 * Show that simple addition works with the framework
 */
TEST(ExpressionTest, SimpleAddition) {
  std::queue<AE*> e;
  storage::Tuple junk;

  // 1 + 4
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)1));
  e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, VALUE_TYPE_TINYINT, 1));
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)4));
  std::unique_ptr<expression::AbstractExpression> testexp(convertToExpression(e));

  Value result = testexp->Evaluate(&junk,NULL);
  std::cout << (*testexp);

  EXPECT_EQ(ValuePeeker::PeekAsBigInt(result), 5LL);
}

/*
 * Show that the associative property is as expected
 */
TEST(ExpressionTest, SimpleMultiplication) {
  std::queue<AE*> e;
  storage::Tuple junk;

  // (1 + 4) * 5
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)1));
  e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, VALUE_TYPE_TINYINT, 1));
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)4));
  e.push(new AE(EXPRESSION_TYPE_OPERATOR_MULTIPLY, VALUE_TYPE_TINYINT, 1));
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)5));

  std::unique_ptr<expression::AbstractExpression> e1(convertToExpression(e));
  Value r1 = e1->Evaluate(&junk, NULL);
  std::cout << (*e1);
  EXPECT_EQ(ValuePeeker::PeekAsBigInt(r1), 25LL);

  // (2 * 5) + 3
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)2));
  e.push(new AE(EXPRESSION_TYPE_OPERATOR_MULTIPLY, VALUE_TYPE_TINYINT, 1));
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)5));
  e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, VALUE_TYPE_TINYINT, 1));
  e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, VALUE_TYPE_TINYINT, 1, (int64_t)3));

  std::unique_ptr<expression::AbstractExpression> e2(convertToExpression(e));
  Value r2 = e2->Evaluate(&junk,NULL);
  std::cout << (*e2);
  EXPECT_EQ(ValuePeeker::PeekAsBigInt(r2), 13LL);
}

} // End test namespace
} // End nstore namespace

