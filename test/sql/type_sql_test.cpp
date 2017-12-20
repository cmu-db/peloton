//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_sql_test.cpp
//
// Identification: test/sql/type_sql_test.cpp
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

class TypeSQLTests : public PelotonTest {
 protected:
  virtual void SetUp() override {
    PelotonTest::SetUp();

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    catalog::Catalog::GetInstance()->Bootstrap();
    txn_manager.CommitTransaction(txn);
  }

  virtual void TearDown() override {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    PelotonTest::TearDown();
  }
};

/**
 * Check whether we can INSERT values that we have reserved for our NULL
 * indicators The DBMS should throw an error to prevent you from doing that
 */
TEST_F(TypeSQLTests, TypeLimitSQLTest) {
  const std::vector<type::TypeId> typeLimitSQLTestTypes = {
      type::TypeId::BOOLEAN, type::TypeId::TINYINT,   type::TypeId::SMALLINT,
      type::TypeId::INTEGER, type::TypeId::TIMESTAMP,
      // FIXME type::TypeId::BIGINT,
      // FIXME type::TypeId::DECIMAL,
      // FIXME type::TypeId::DATE
  };

  for (auto col_type : typeLimitSQLTestTypes) {
    // CREATE TABLE that contains a column for each type
    std::string table_name = "tbl" + TypeIdToString(col_type);
    std::string sql = StringUtil::Format(
        "CREATE TABLE %s(id INT PRIMARY KEY, b %s);", table_name.c_str(),
        TypeIdToString(col_type).c_str());
    LOG_TRACE("SQL: %s", sql.c_str());
    TestingSQLUtil::ExecuteSQLQuery(sql);

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
    }  // SWITCH
    os << ");";
    // This should throw an error because the query
    // is trying to insert a value that is not in a valid range for the type
    auto result = TestingSQLUtil::ExecuteSQLQuery(os.str());
    LOG_TRACE("%s => %s", TypeIdToString(col_type).c_str(), os.str().c_str());
    EXPECT_EQ(ResultType::FAILURE, result);
  }
}

void CheckQueryResult(std::vector<ResultValue> result,
                      std::vector<std::string> expected,
                      size_t tuple_descriptor_size) {
  EXPECT_EQ(result.size(), expected.size());
  for (size_t i = 0; i < result.size(); i++) {
    for (size_t j = 0; j < tuple_descriptor_size; j++) {
      int idx = i * tuple_descriptor_size + j;
      std::string s =
          std::string(result[idx].begin(), result[idx].end());
      EXPECT_EQ(s, expected[i]);
    }
  }
}

TEST_F(TypeSQLTests, VarcharTest) {
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE foo(name varchar(250));");

  for (const std::string &name :
       {"Alice", "Peter", "Cathy", "Bob", "Alicia", "David"}) {
    std::string sql = "INSERT INTO foo VALUES ('" + name + "');";
    TestingSQLUtil::ExecuteSQLQuery(sql);
  }

  // NULL for good measure
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO foo VALUES (NULL);");

  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  std::string query;
  std::vector<std::string> expected{};

  query = "SELECT * FROM foo WHERE name = 'Alice';";
  expected = {"Alice"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name = 'david';";
  expected = {};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name = 'Ann';";
  expected = {};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name = 'Alice' OR name = 'Alicia';";
  expected = {"Alice", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name != 'Bob' AND name != 'David';";
  expected = {"Alice", "Peter", "Cathy", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name >= 'A' AND name <= 'D';";
  expected = {"Alice", "Cathy", "Bob", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name > 'David';";
  expected = {"Peter"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name <= 'Alicia';";
  expected = {"Alice", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name LIKE '%li%'";
  expected = {"Alice", "Alicia"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name LIKE '_____'";
  expected = {"Alice", "Peter", "Cathy", "David"};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());

  query = "SELECT * FROM foo WHERE name LIKE '%th'";
  expected = {};
  TestingSQLUtil::ExecuteSQLQuery(query.c_str(), result, tuple_descriptor,
                                  rows_changed, error_message);
  CheckQueryResult(result, expected, tuple_descriptor.size());
}

}  // namespace test
}  // namespace peloton
