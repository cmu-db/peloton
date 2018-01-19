//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// functions_test.cpp
//
// Identification: test/function/functions_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/language_catalog.h"
#include "catalog/proc_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

class FunctionsTests : public PelotonTest {
 public:
  static type::Value TestFunc(
      UNUSED_ATTRIBUTE const std::vector<type::Value> &args) {
    return type::ValueFactory::GetIntegerValue(0);
  }

  virtual void SetUp() {
    PelotonTest::SetUp();
    auto catalog = catalog::Catalog::GetInstance();
    catalog->Bootstrap();
  }
};

TEST_F(FunctionsTests, CatalogTest) {
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto &pg_language = catalog::LanguageCatalog::GetInstance();

  // Test "internal" language
  auto txn = txn_manager.BeginTransaction();
  auto internal_lang = pg_language.GetLanguageByName("internal", txn);
  EXPECT_NE(nullptr, internal_lang);
  internal_lang = pg_language.GetLanguageByOid(internal_lang->GetOid(), txn);
  EXPECT_NE(nullptr, internal_lang);
  EXPECT_EQ("internal", internal_lang->GetName());

  // test add/del language
  type::EphemeralPool pool;
  std::string lanname = "foo_lang";
  pg_language.InsertLanguage(lanname, &pool, txn);
  auto inserted_lang = pg_language.GetLanguageByName(lanname, txn);
  EXPECT_NE(nullptr, inserted_lang);
  inserted_lang = pg_language.GetLanguageByOid(inserted_lang->GetOid(), txn);
  EXPECT_NE(nullptr, inserted_lang);
  EXPECT_EQ(lanname, inserted_lang->GetName());
  pg_language.DeleteLanguage(lanname, txn);
  inserted_lang = pg_language.GetLanguageByName(lanname, txn);
  EXPECT_EQ(nullptr, inserted_lang);

  txn_manager.CommitTransaction(txn);
  auto &pg_proc = catalog::ProcCatalog::GetInstance();

  // test pg_proc
  txn = txn_manager.BeginTransaction();
  std::string func_name = "test_func";
  std::vector<type::TypeId> arg_types{type::TypeId::VARCHAR,
                                      type::TypeId::INTEGER};

  catalog->AddBuiltinFunction(
      func_name, arg_types, type::TypeId::INTEGER, internal_lang->GetOid(),
      "TestFunc", function::BuiltInFuncType{OperatorId::Add, TestFunc}, txn);

  auto inserted_proc = pg_proc.GetProcByName(func_name, arg_types, txn);
  EXPECT_NE(nullptr, inserted_proc);
  EXPECT_EQ(internal_lang->GetOid(), inserted_proc->GetLangOid());
  type::TypeId ret_type = inserted_proc->GetRetType();
  EXPECT_EQ(type::TypeId::INTEGER, ret_type);
  std::string func = inserted_proc->GetSrc();
  EXPECT_EQ("TestFunc", func);

  txn_manager.CommitTransaction(txn);

  auto func_data = catalog->GetFunction(func_name, arg_types);
  EXPECT_EQ(ret_type, func_data.return_type_);
  EXPECT_EQ((int64_t)TestFunc, (int64_t)func_data.func_.impl);
}

TEST_F(FunctionsTests, FuncCallTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(a TINYINT, b SMALLINT, c INTEGER, d BIGINT, e "
      "DECIMAL, s VARCHAR);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO test VALUES (1.0, 4.0, 9.0, 16.0, 25.0, ' abc ');");

  std::vector<std::string> result = {"1|2|3|4|5"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT SQRT(a), SQRT(b), SQRT(c), SQRT(d), SQRT(e) FROM test;", result,
      false);

  result = {"32"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult("SELECT ASCII(s) FROM test;",
                                                result, false);
  

  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE OR REPLACE FUNCTION"
      " increment(e double) RETURNS double AS $$"
      " BEGIN RETURN e + 1; END; $$ LANGUAGE plpgsql;");

  result = {"26"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult("SELECT increment(e) FROM test;",
                                                result, false);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(FunctionsTests, SubstrFuncCallTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(a DECIMAL, s VARCHAR);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO test VALUES (4.0, '1234567');");

  std::vector<std::string> result = {"12345"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT SUBSTR(s,1,5) FROM test;", result, false);

  result = {"7"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "SELECT SUBSTR(s,7,1) FROM test;", result, false);
  EXPECT_EQ(1, result.size());
  EXPECT_EQ("7", std::string(result[0].begin(), result[0].begin() + 1));

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
