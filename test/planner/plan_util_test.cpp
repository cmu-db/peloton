//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util_test.cpp
//
// Identification: test/planner/plan_util_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/testing_executor_util.h"
#include "parser/postgresparser.h"
#include "storage/data_table.h"

#include "planner/plan_util.h"

namespace peloton {
namespace test {

#define TEST_DB_NAME "test_db"

class PlanUtilTests : public PelotonTest {};

TEST_F(PlanUtilTests, GetAffectedIndexesTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  catalog->CreateDatabase(TEST_DB_NAME, txn);
  auto db = catalog->GetDatabaseWithName(TEST_DB_NAME, txn);
  // Insert a table first
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  auto fname_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "first_name", false);
  auto lname_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "last_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, fname_column, lname_column}));
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(TEST_DB_NAME, "test_table", std::move(table_schema),
                       txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  auto source_table = db->GetTableWithName("test_table");
  oid_t col_id = source_table->GetSchema()->GetColumnID(id_column.column_name);
  std::vector<oid_t> source_col_ids;
  source_col_ids.push_back(col_id);

  // create index on 'id'
  catalog->CreateIndex(TEST_DB_NAME, "test_table", source_col_ids,
                       "test_id_idx", false, IndexType::BWTREE, txn);

  // create index on 'id' and 'first_name'
  col_id = source_table->GetSchema()->GetColumnID(fname_column.column_name);
  source_col_ids.push_back(col_id);

  catalog->CreateIndex(TEST_DB_NAME, "test_table", source_col_ids,
                       "test_fname_idx", false, IndexType::BWTREE, txn);
  txn_manager.CommitTransaction(txn);

  // dummy txn to get the catalog_cache object
  txn = txn_manager.BeginTransaction();

  // This is also required so that database objects are cached
  auto db_object = catalog->GetDatabaseObject(TEST_DB_NAME, txn);
  EXPECT_EQ(1, static_cast<int>(db_object->GetTableObjects().size()));

  // Till now, we have a table : id, first_name, last_name
  // And two indexes on following columns:
  // 1) id
  // 2) id and first_name
  auto table_object = db_object->GetTableObject("test_table");
  oid_t id_idx_oid = table_object->GetIndexObject("test_id_idx")->GetIndexOid();
  oid_t fname_idx_oid =
      table_object->GetIndexObject("test_fname_idx")->GetIndexOid();

  // An update query affecting both indexes
  std::string query_string = "UPDATE test_table SET id = 0;";
  std::unique_ptr<Statement> stmt(new Statement("UPDATE", query_string));
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::UpdateStatement *>(sql_stmt)->table->TryBindDatabaseName(
      TEST_DB_NAME);
  std::set<oid_t> affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);

  // id and first_name are affected
  EXPECT_EQ(2, static_cast<int>(affected_indexes.size()));
  std::set<oid_t> expected_oids{id_idx_oid, fname_idx_oid};
  EXPECT_EQ(expected_oids, affected_indexes);

  // Update query affecting only one index
  query_string = "UPDATE test_table SET first_name = '';";
  stmt.reset(new Statement("UPDATE", query_string));
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::UpdateStatement *>(sql_stmt)->table->TryBindDatabaseName(
      TEST_DB_NAME);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);

  // only first_name is affected
  EXPECT_EQ(1, static_cast<int>(affected_indexes.size()));
  expected_oids = std::set<oid_t>({fname_idx_oid});
  EXPECT_EQ(expected_oids, affected_indexes);

  // ====== DELETE statements check ===
  query_string = "DELETE FROM test_table;";
  stmt.reset(new Statement("DELETE", query_string));
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::DeleteStatement *>(sql_stmt)->TryBindDatabaseName(
      TEST_DB_NAME);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);

  // all indexes are affected
  EXPECT_EQ(2, static_cast<int>(affected_indexes.size()));
  expected_oids = std::set<oid_t>({id_idx_oid, fname_idx_oid});
  EXPECT_EQ(expected_oids, affected_indexes);

  // ========= INSERT statements check ==
  query_string = "INSERT INTO test_table VALUES (1, 'pel', 'ton');";
  stmt.reset(new Statement("INSERT", query_string));
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::InsertStatement *>(sql_stmt)->TryBindDatabaseName(
      TEST_DB_NAME);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);

  // all indexes are affected
  EXPECT_EQ(2, static_cast<int>(affected_indexes.size()));
  expected_oids = std::set<oid_t>({id_idx_oid, fname_idx_oid});
  EXPECT_EQ(expected_oids, affected_indexes);

  // ========= SELECT statement check ==
  query_string = "SELECT * FROM test_table;";
  stmt.reset(new Statement("SELECT", query_string));
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);

  // no indexes are affected
  EXPECT_EQ(0, static_cast<int>(affected_indexes.size()));
  txn_manager.CommitTransaction(txn);  
}

}  // namespace test
}  // namespace peloton
