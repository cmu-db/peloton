//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// functions_test.cpp
//
// Identification: test/function/functions_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"
#include "sql/testing_sql_util.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/catalog.h"
#include "catalog/proc_catalog.h"
#include "catalog/language_catalog.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace test {

class FunctionsTests : public PelotonTest {
 public:
  static type::Value TestFunc(UNUSED_ATTRIBUTE const std::vector<type::Value>& args) {
    return type::ValueFactory::GetIntegerValue(0);
  }
};

TEST_F(FunctionsTests, CatalogTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto pg_language = catalog::LanguageCatalog::GetInstance();

  // test "internal" language
  auto txn = txn_manager.BeginTransaction();
  auto internal_oid = pg_language->GetLanguageOid("internal", txn);
  EXPECT_NE(INVALID_OID, internal_oid);
  auto name = pg_language->GetLanguageName(internal_oid, txn);
  EXPECT_EQ("internal", name);

  // test add/del language
  type::EphemeralPool pool;
  std::string lanname = "foo_lang";
  pg_language->InsertLanguage(lanname, &pool, txn);
  auto oid = pg_language->GetLanguageOid(lanname, txn);
  EXPECT_NE(INVALID_OID, oid);
  name = pg_language->GetLanguageName(oid, txn);
  EXPECT_EQ(lanname, name);
  pg_language->DeleteLanguage(lanname, txn);
  oid = pg_language->GetLanguageOid(lanname, txn);
  EXPECT_EQ(INVALID_OID, oid);

  txn_manager.CommitTransaction(txn);

  auto pg_proc = catalog::ProcCatalog::GetInstance();

  // test pg_proc
  txn = txn_manager.BeginTransaction();
  std::string func_name = "test_func";
  std::vector<type::TypeId> arg_types{type::TypeId::VARCHAR, type::TypeId::INTEGER};

  catalog->AddFunction(func_name, arg_types, type::TypeId::INTEGER,
                       internal_oid, "TestFunc", TestFunc, nullptr, txn);

  oid_t prolang = pg_proc->GetProLang(func_name, arg_types, txn);
  EXPECT_EQ(internal_oid, prolang);
  type::TypeId ret_type = pg_proc->GetProRetType(func_name, arg_types, txn);
  EXPECT_EQ(type::TypeId::INTEGER, ret_type);
  std::string func = pg_proc->GetProSrc(func_name, arg_types, txn);
  EXPECT_EQ("TestFunc", func);

  txn_manager.CommitTransaction(txn);

  auto func_data = catalog->GetFunction(func_name, arg_types);
  EXPECT_EQ(ret_type, func_data.return_type_);
  EXPECT_EQ((int64_t)TestFunc, (int64_t)func_data.func_ptr_);
}

TEST_F(FunctionsTests, FuncCallTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a DECIMAL, s VARCHAR);");

  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (4.0, 'abc');");

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_affected;

  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT SQRT(a), SUBSTR(s,1,2) FROM test;", result,
      tuple_descriptor, rows_affected, error_message);
  EXPECT_EQ(1, result[0].second.size());
  EXPECT_EQ('2', result[0].second[0]);
  EXPECT_EQ(2, result[1].second.size());
  EXPECT_EQ(result[1].second[0], 'a');
  EXPECT_EQ(result[1].second[1], 'b');

  TestingSQLUtil::ExecuteSQLQuery(
      "SELECT ASCII(s) FROM test;", result,
      tuple_descriptor, rows_affected, error_message);
  EXPECT_EQ(2, result[0].second.size());
  EXPECT_EQ('9', result[0].second[0]);
  EXPECT_EQ('7', result[0].second[1]);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
