#include <algorithm>
#include "common/harness.h"

#define private public
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "binder/bind_node_visitor.h"
#include "optimizer/operators.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/operator_expression.h"
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "util/string_util.h"

namespace peloton {

namespace test {

using namespace optimizer;

class OperatorTransformerTests : public PelotonTest {
 protected:
  virtual void SetUp() override {
    // Call parent virtual function first
    PelotonTest::SetUp();

    // Create test database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Create table
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");
    TestingSQLUtil::ExecuteSQLQuery(
        "CREATE TABLE test2(a2 INT PRIMARY KEY, b2 INT, c2 INT);");
  }
  std::shared_ptr<OperatorExpression> GetOpExpression(std::string query) {
    // Parse query
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto parsed_stmt = peloton_parser.BuildParseTree(query);
    auto select_stmt = parsed_stmt->GetStatement(0);

    // Bind query
    binder::BindNodeVisitor binder;
    binder.BindNameToNode(select_stmt);

    QueryToOperatorTransformer transformer;
    return std::move(transformer.ConvertToOpExpression(select_stmt));
  }

  void CheckJoinPredicate(expression::AbstractExpression* join_predicate,
                std::string table_names,
                std::string true_join_predicates) {
    // Parse true predicates
    auto peloton_parser = parser::PostgresParser::GetInstance();
    auto ref_query = StringUtil::Format(
        "SELECT %s FROM %s", true_join_predicates.c_str(), table_names.c_str());
    auto parsed_stmt = peloton_parser.BuildParseTree(ref_query);
    auto ref_stmt = parsed_stmt->GetStatement(0);
    binder::BindNodeVisitor binder;
    binder.BindNameToNode(ref_stmt);
    auto ref_expr = ((parser::SelectStatement*)ref_stmt)->select_list->at(0);
    EXPECT_TRUE(join_predicate->Equals(ref_expr));
  }

  virtual void TearDown() override {
    // Destroy test database
    auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Call parent virtual function
    PelotonTest::TearDown();
  }
};

TEST_F(OperatorTransformerTests, JoinTransformationTest){
  std::string tables = "test, test2";

  // Test table list
  auto op_expr = GetOpExpression("SELECT * FROM test, test2 WHERE test.a = test2.a2");
  auto op = op_expr->Op().As<LogicalInnerJoin>();
  CheckJoinPredicate(op->join_predicate.get(), tables, "test.a = test2.a2");

  // Test WHERE combined with JOIN ON
  op_expr = GetOpExpression(
      "SELECT * FROM test join test2 ON test.b = test2.b2 WHERE test.a = test2.a2");
  op = op_expr->Op().As<LogicalInnerJoin>();
  EXPECT_TRUE(op!= nullptr);
  CheckJoinPredicate(op->join_predicate.get(),
                     tables, "test.a = test2.a2 AND test.b = test2.b2");

  // Test multi-way JOIN with WHERE
  op_expr = GetOpExpression(
      "SELECT * FROM "
          "test as A "
          "JOIN "
          "test as B "
          "  ON A.b = B.b "
          "JOIN "
          "test as C "
          "  ON A.a = C.a "
          "WHERE B.c = C.c");
  op = op_expr->Op().As<LogicalInnerJoin>();
  EXPECT_TRUE(op!= nullptr);
  CheckJoinPredicate(op->join_predicate.get(),
                     "test as A, test as B, test as C",
                     "B.c = C.c AND A.a = C.a");
  auto childern = op_expr->Children();
  auto left_op = childern[0]->Op().As<LogicalInnerJoin>();
  EXPECT_TRUE(left_op!= nullptr);
  CheckJoinPredicate(left_op->join_predicate.get(),
                     "test as A, test as B, test as C",
                     "A.b = B.b");
}

} /* namespace test */
} /* namespace peloton */
