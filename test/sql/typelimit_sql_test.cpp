//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// typelimit_sql_test.cpp
//
// Identification: test/sql/typelimit_sql_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class TypeLimitSQLTests : public PelotonTest {};

const std::vector<type::TypeId> typeLimitSQLTestTypes = {
    type::TypeId::BOOLEAN,  type::TypeId::TINYINT,
    type::TypeId::SMALLINT, type::TypeId::INTEGER,
    // FIXME type::TypeId::BIGINT,
    // FIXME type::TypeId::DECIMAL,
    // FIXME type::TypeId::TIMESTAMP,
    // FIXME type::TypeId::DATE
};

void CreateAndLoadTable(std::string table_name, type::TypeId col_type) {
  std::string sql = StringUtil::Format("CREATE TABLE %s(id INT PRIMARY KEY, b %s);",
                                       table_name.c_str(),
                                       TypeIdToString(col_type).c_str());
  LOG_TRACE("SQL: %s", sql.c_str());
  TestingSQLUtil::ExecuteSQLQuery(sql);
}

/**
 * Check whether we can INSERT values that we have reserved for our NULL indicators
 * The DBMS should throw an error to prevent you from doing that
 */
TEST_F(TypeLimitSQLTests, InsertInvalidMinValue) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  for (auto col_type : typeLimitSQLTestTypes) {
    // CREATE TABLE that contains a column for each type
    std::string table_name = "tbl" + TypeIdToString(col_type);
    CreateAndLoadTable(table_name, col_type);

    // Then try to insert the min value
    std::ostringstream os;
    os << "INSERT INTO " << table_name << " VALUES (1, ";
    switch (col_type) {
      case type::TypeId::BOOLEAN:
        os << (int32_t)type::PELOTON_BOOLEAN_NULL;
        break;
      case type::TypeId::TINYINT:
        os << (int32_t)type::PELOTON_INT8_NULL;
        break;
      case type::TypeId::SMALLINT:
        os << type::PELOTON_INT16_NULL;
        break;
      case type::TypeId::INTEGER:
        os << type::PELOTON_INT32_NULL;
        break;
      case type::TypeId::BIGINT:
        os << type::PELOTON_INT64_NULL;
        break;
      case type::TypeId::DECIMAL:
        os << type::PELOTON_DECIMAL_NULL;
        break;
      case type::TypeId::TIMESTAMP:
        os << type::PELOTON_TIMESTAMP_NULL;
        break;
      case type::TypeId::DATE:
        os << type::PELOTON_DATE_NULL;
        break;
      default: {
        // Nothing to do!
      }
    } // SWITCH
    os << ");";
    // This should throw an error because the query
    // is trying to insert a value that is not in a valid range for the type
    auto result = TestingSQLUtil::ExecuteSQLQuery(os.str());
    LOG_DEBUG("%s => %s", TypeIdToString(col_type).c_str(), os.str().c_str());
    EXPECT_EQ(ResultType::FAILURE, result);
  }

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton