//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// Compression Testing Script
//===----------------------------------------------------------------------===//

#include <memory>
#include <math.h>
#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class CompressionTest : public PelotonTest {};

/*
The following test inserts 2500 tuples in the datatable. Since a 1000 tuples
are inserted in each tile_group, there will be 1 compressed tiles and 1
uncompressed tile. After insertion of all the tuples, we call the Compress
Table function.

Each tuple inserted is of the form (i/10, i*10), where i belongs to [0,2500)

We then perform a sequential scan on the table and retrieve the uncompressed
values. Each uncompressed value, should be equal to the origina value.
*/

TEST_F(CompressionTest, IntegerTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);
  // Create a t
  txn = txn_manager.BeginTransaction();

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, year integer);");
  int i;

  for (i = 0; i < 2500; i++) {
    std::ostringstream os;
    os << "insert into foo values(" << (i / 10) << ", " << i * 10 << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
  EXPECT_EQ(i, 2500);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;

  txn_manager.CommitTransaction(txn);

  storage::DataTable *foo_table =
      catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, "foo");
  foo_table->CompressTable();

  os << "select * from foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);

  for (i = 0; i < 2500; i++) {
    std::string resultStr(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)) + "\t" +
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    std::string expectedStr(std::to_string(i / 10) + "\t" +
                            std::to_string(i * 10));
    EXPECT_EQ(resultStr, expectedStr);
  }

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

/*
In the same way as the test above, we now insert decimals in the second column.

Each tuple inserted is of the form (i, (i+5)/10), where i belongs to [0,2500)
When inserting decimals, we ensure the precision is always 2 digits. This
prevents lossy compression of floats.

We then perform a sequential scan on the table and retrieve the uncompressed
values. Each uncompressed value, should be equal to the original value.

Note that as opposed to the previous test, we dont do a string comparison, but
convert the values from string to floats and then do a compare. This used
because in string compare : 80.000000 and 80 may fail even though they represent
the same value.
*/

TEST_F(CompressionTest, DecimalTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

  txn_manager.CommitTransaction(txn);
  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE foo(id integer, year decimal);");
  int i;

  for (i = 0; i < 2500; i++) {
    std::ostringstream os;
    float value = (float)i;
    float actual_year = value + (value + 5.0) / 10.0;
    std::stringstream stream;
    stream << std::fixed << std::setprecision(2) << actual_year;
    std::string inserted_value = stream.str();
    os << "insert into foo values(" << i << ", " << inserted_value << ");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }

  EXPECT_EQ(i, 2500);

  storage::DataTable *foo_table =
      catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, "foo");
  foo_table->CompressTable();

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;
  std::ostringstream os;

  os << "select * from foo;";

  TestingSQLUtil::ExecuteSQLQuery(os.str(), result, tuple_descriptor,
                                  rows_affected, error_message);
  for (i = 0; i < 2500; i++) {
    std::string result_id(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i)));
    std::string result_year(
        TestingSQLUtil::GetResultValueAsString(result, (2 * i) + 1));
    int id = std::stoi(result_id);
    float year = std::stof(result_year);
    float value = (float)i;
    float actual_year = value + (value + 5.0) / 10.0;
    EXPECT_EQ(id, i);
    EXPECT_EQ(year, actual_year);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}
}
