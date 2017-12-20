//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_sql_test.cpp
//
// Identification: test/sql/string_functions_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <math.h>
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {
class StringFunctionTest : public PelotonTest {};
/*
The following test inserts 32 tuples in the datatable.
Each tuple inserted is of the form (i, i * 'a' ), where i belongs to [0,32)
We then perform a sequential scan on the table and retrieve the id and length
of the second column.
*/
TEST_F(StringFunctionTest, LengthTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  catalog::Catalog::GetInstance()->Bootstrap();
  txn_manager.CommitTransaction(txn);
  // Create a t
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, name varchar(32));");
  int i;
  std::string s = "'", t = "'";
  for (i = 0; i < 32; i++) {
    s += "a";
    t = s + "'";
    std::string os =
        "insert into foo values(" + std::to_string(i) + ", " + t + ");";
    TestingSQLUtil::ExecuteSQLQuery(os);
  }
  EXPECT_EQ(i, 32);

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;

  int rows_affected;
  std::ostringstream os;
  txn_manager.CommitTransaction(txn);

  os << "select length(name) from foo;";
  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < 32; i++) {
    std::string resultStr(TestingSQLUtil::GetResultValueAsString(result, i));
    std::string expectedStr(std::to_string(i + 2));
    EXPECT_EQ(resultStr, expectedStr);
  }

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
