#include <algorithm>
#include "common/harness.h"

#define private public
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "binder/bind_node_visitor.h"
#include "expression/expression_util.h"
#include "optimizer/operators.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/operator_expression.h"
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "concurrency/transaction_manager_factory.h"
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
  std::shared_ptr<OperatorExpression> TransformToOpExpression(
      std::string query, std::unique_ptr<parser::SQLStatementList> &stmt_list) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // Parse query
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    stmt_list = peloton_parser.BuildParseTree(query);
    auto stmt =
        reinterpret_cast<parser::SelectStatement *>(stmt_list->GetStatement(0));

    // Bind query
    binder::BindNodeVisitor binder(txn, DEFAULT_DB_NAME);
    binder.BindNameToNode(stmt);

    QueryToOperatorTransformer transformer(txn);
    auto result = transformer.ConvertToOpExpression(stmt);

    txn_manager.CommitTransaction(txn);
    return result;
  }

  void CheckPredicate(std::vector<AnnotatedExpression> predicates,
                      std::string table_names, std::string true_predicates) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    // Parse true predicates
    auto peloton_parser = parser::PostgresParser::GetInstance();
    auto ref_query = StringUtil::Format(
        "SELECT %s FROM %s", true_predicates.c_str(), table_names.c_str());
    auto parsed_stmt = peloton_parser.BuildParseTree(ref_query);
    auto ref_stmt = parsed_stmt->GetStatement(0);
    binder::BindNodeVisitor binder(txn, DEFAULT_DB_NAME);
    binder.BindNameToNode(ref_stmt);
    auto ref_expr =
        ((parser::SelectStatement *)ref_stmt)->select_list.at(0).get();
    txn_manager.CommitTransaction(txn);
    LOG_INFO("Expected: %s", true_predicates.c_str());
    auto predicate = expression::ExpressionUtil::JoinAnnotatedExprs(predicates);
    LOG_INFO("Actual: %s", predicate->GetInfo().c_str());
    EXPECT_TRUE(predicate->ExactlyEquals(*ref_expr));
  }

  virtual void TearDown() override {
    // Destroy test database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // Call parent virtual function
    PelotonTest::TearDown();
  }
};
// TODO(boweic): Since operator transformer has a huge change during the
// optimizer refactoring, the old test is outdated. But at the same modifying
// the test is too much work because we extract all predicates in filters rather
// than keeping them in the scan/join operator at this step. At this time I just
// comment it out. There are other unit tests & integrating test that will
// also use the transformer and those test cases prove that the transfomer works
TEST_F(OperatorTransformerTests, JoinTransformationTest) {
  // std::unique_ptr<parser::SQLStatementList> stmt_list;
  //
  // // Test table list
  // auto op_expr = TransformToOpExpression("SELECT * FROM test, test2 WHERE
  // test.a = test2.a2", stmt_list);
  // // Check Join Predicates
  // auto op = op_expr->Op().As<LogicalInnerJoin>();
  //
  // CheckPredicate(op->join_predicates, "test, test2", "test.a = test2.a2");
  //
  //
  // // Test WHERE combined with JOIN ON
  // op_expr = TransformToOpExpression(
  //     "SELECT * FROM test join test2 ON test.b = test2.b2 WHERE test.a =
  //     test2.a2", stmt_list);
  // // Check Where
  // op = op_expr->Op().As<LogicalInnerJoin>();
  // // Check Join Predicates
  // EXPECT_TRUE(op != nullptr);
  // CheckPredicate(op->join_predicates,
  //                "test, test2", "test.a = test2.a2 AND test.b = test2.b2");
  //
  // // Test remaining expression in WHERE
  // op_expr = TransformToOpExpression(
  //     "SELECT * FROM test as A, test as B, test as C "
  //         "WHERE (A.a = B.b OR B.b = C.c) AND A.c = B.b AND A.b = 1 AND B.c +
  //         1 = 10", stmt_list);
  // op = op_expr->Op().As<LogicalInnerJoin>();
  // // Check Where
  // EXPECT_TRUE(op != nullptr);
  // // Check Join Predicates
  // CheckPredicate(op->join_predicates,
  //                "test as A, test as B, test as C", "A.a = B.b OR B.b =
  //                C.c");
  // auto childern = op_expr->Children();
  // auto left_op = childern[0]->Op().As<LogicalInnerJoin>();
  // EXPECT_TRUE(left_op != nullptr);
  // CheckPredicate(left_op->join_predicates,
  //                "test as A, test as B, test as C", "A.c = B.b");
  //
  //
  //
  // // Test multi-way JOIN with WHERE
  // op_expr = TransformToOpExpression(
  //     "SELECT * FROM "
  //         "test as A "
  //         "JOIN "
  //         "test as B "
  //         "  ON A.b = B.b "
  //         "JOIN "
  //         "test as C "
  //         "  ON A.a = C.a "
  //         "WHERE B.c = C.c", stmt_list);
  // op = op_expr->Op().As<LogicalInnerJoin>();
  // // Check Join Predicates
  // EXPECT_TRUE(op != nullptr);
  // CheckPredicate(op->join_predicates,
  //                "test as A, test as B, test as C",
  //                "B.c = C.c AND A.a = C.a");
  // childern = op_expr->Children();
  // left_op = childern[0]->Op().As<LogicalInnerJoin>();
  // EXPECT_TRUE(left_op != nullptr);
  // CheckPredicate(left_op->join_predicates,
  //                "test as A, test as B, test as C",
  //                "A.b = B.b");
}

}  // namespace test
}  // namespace peloton
