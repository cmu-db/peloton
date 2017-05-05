#include <algorithm>
#include "common/harness.h"

#define private public
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "optimizer/operators.h"
#include "optimizer/query_to_operator_transformer.h"
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"

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
  std::string query = "SELECT * FROM test, test2 where test.a = test2.a";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto parsed_stmt = peloton_parser.BuildParseTree(query);
  QueryToOperatorTransformer transformer;
  auto expr = transformer.ConvertToOpExpression(parsed_stmt->GetStatement(0));

}

} /* namespace test */
} /* namespace peloton */
