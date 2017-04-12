//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// Compression Testing Script
//===----------------------------------------------------------------------===//

#include <memory>

#include "sql/testing_sql_util.h"
#include "catalog/catalog.h"
#include "common/harness.h"
#include "executor/create_executor.h"
#include "optimizer/simple_optimizer.h"
#include "planner/create_plan.h"


namespace peloton {
namespace test {

class CompressionTest : public PelotonTest {};

TEST_F(CompressionTest, BasicInsertionTest) {
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);

  // Create a table first
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(id integer primary key, year integer);");
  int i;

  for (i = 0; i < 20; i++) {
    std::ostringstream os;
    os << "insert into foo values("<< i << ", " << i*100 <<");";
    TestingSQLUtil::ExecuteSQLQuery(os.str());
  }
    EXPECT_EQ(i, 20);
  }
}
}
