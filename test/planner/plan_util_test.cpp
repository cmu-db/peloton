//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util_test.cpp
//
// Identification: test/planner/plan_util_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "binder/bind_node_visitor.h"
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
#define TEST_DB_COLUMNS "test_db_columns"

class PlanUtilTests : public PelotonTest {};

TEST_F(PlanUtilTests, GetAffectedIndexesTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

<<<<<<< HEAD
  catalog->CreateDatabase(TEST_DB_NAME, txn);
  auto db = catalog->GetDatabaseWithName(TEST_DB_NAME, txn);
=======
  catalog->CreateDatabase(txn, TEST_DB_NAME);
>>>>>>> upstream/master
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
  catalog->CreateTable(txn,
                       TEST_DB_NAME,
                       DEFAULT_SCHEMA_NAME,
                       std::move(table_schema),
                       "test_table",
                       false);
  auto source_table = catalog->GetTableWithName(txn,
                                                TEST_DB_NAME,
                                                DEFAULT_SCHEMA_NAME,
                                                "test_table");
  EXPECT_NE(source_table, nullptr);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
<<<<<<< HEAD
  oid_t db_oid = db->GetOid();
  oid_t table_oid = source_table->GetOid();
  oid_t col_id = source_table->GetSchema()->GetColumnID(id_column.column_name);
=======
  oid_t col_id = source_table->GetSchema()->GetColumnID(id_column.GetName());
>>>>>>> upstream/master
  std::vector<oid_t> source_col_ids;
  source_col_ids.push_back(col_id);

  // create index on 'id'
  catalog->CreateIndex(txn,
                       TEST_DB_NAME,
                       DEFAULT_SCHEMA_NAME,
                       "test_table",
                       "test_id_idx",
                       source_col_ids,
                       false,
                       IndexType::BWTREE);

  // create index on 'id' and 'first_name'
  col_id = source_table->GetSchema()->GetColumnID(fname_column.GetName());
  source_col_ids.push_back(col_id);

  catalog->CreateIndex(txn,
                       TEST_DB_NAME,
                       DEFAULT_SCHEMA_NAME,
                       "test_table",
                       "test_fname_idx",
                       source_col_ids,
                       false,
                       IndexType::BWTREE);
  txn_manager.CommitTransaction(txn);

  // dummy txn to get the catalog_cache object
  txn = txn_manager.BeginTransaction();

  // This is also required so that database objects are cached
  auto db_object = catalog->GetDatabaseCatalogEntry(txn, TEST_DB_NAME);

  // Till now, we have a table : id, first_name, last_name
  // And two indexes on following columns:
  // 1) id
  // 2) id and first_name
  auto table_object =
      db_object->GetTableCatalogEntry("test_table", DEFAULT_SCHEMA_NAME);
  EXPECT_NE(table_object, nullptr);
  oid_t id_idx_oid = table_object->GetIndexCatalogEntry("test_id_idx")->GetIndexOid();
  oid_t fname_idx_oid =
      table_object->GetIndexCatalogEntry("test_fname_idx")->GetIndexOid();

  // An update query affecting both indexes
  std::string query_string = "UPDATE test_table SET id = 0;";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::UpdateStatement *>(sql_stmt)
      ->table->TryBindDatabaseName(TEST_DB_NAME);
  std::vector<planner::col_triplet> affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);
  std::set<planner::col_triplet> affected_indexes_set(affected_indexes.begin(),
                                                      affected_indexes.end());

  // id and first_name are affected
  EXPECT_EQ(2, static_cast<int>(affected_indexes_set.size()));
  std::set<planner::col_triplet> expected_oids;
  expected_oids.emplace(db_oid, table_oid, id_idx_oid);
  expected_oids.emplace(db_oid, table_oid, fname_idx_oid);
  EXPECT_EQ(expected_oids, affected_indexes_set);

  // Update query affecting only one index
  query_string = "UPDATE test_table SET first_name = '';";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::UpdateStatement *>(sql_stmt)
      ->table->TryBindDatabaseName(TEST_DB_NAME);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);
  affected_indexes_set = std::set<planner::col_triplet>(
      affected_indexes.begin(), affected_indexes.end());

  // only first_name is affected
  EXPECT_EQ(1, static_cast<int>(affected_indexes_set.size()));
  expected_oids.clear();
  expected_oids.emplace(db_oid, table_oid, fname_idx_oid);
  EXPECT_EQ(expected_oids, affected_indexes_set);

  // ====== DELETE statements check ===
  query_string = "DELETE FROM test_table;";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::DeleteStatement *>(sql_stmt)
      ->TryBindDatabaseName(TEST_DB_NAME);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);
  affected_indexes_set = std::set<planner::col_triplet>(
      affected_indexes.begin(), affected_indexes.end());

  // all indexes are affected
  EXPECT_EQ(2, static_cast<int>(affected_indexes_set.size()));
  expected_oids.clear();
  expected_oids.emplace(db_oid, table_oid, fname_idx_oid);
  expected_oids.emplace(db_oid, table_oid, id_idx_oid);
  EXPECT_EQ(expected_oids, affected_indexes_set);

  // ========= INSERT statements check ==
  query_string = "INSERT INTO test_table VALUES (1, 'pel', 'ton');";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::InsertStatement *>(sql_stmt)
      ->TryBindDatabaseName(TEST_DB_NAME);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);
  affected_indexes_set = std::set<planner::col_triplet>(
      affected_indexes.begin(), affected_indexes.end());

  // all indexes are affected
  EXPECT_EQ(2, static_cast<int>(affected_indexes_set.size()));
  expected_oids.clear();
  expected_oids.emplace(db_oid, table_oid, fname_idx_oid);
  expected_oids.emplace(db_oid, table_oid, id_idx_oid);
  EXPECT_EQ(expected_oids, affected_indexes_set);

  // ========= SELECT statement check ==
  query_string = "SELECT * FROM test_table;";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);

  // no indexes are affected
  EXPECT_EQ(0, static_cast<int>(affected_indexes.size()));
  txn_manager.CommitTransaction(txn);
}

TEST_F(PlanUtilTests, GetIndexableColumnsTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();
<<<<<<< HEAD
  catalog->CreateDatabase(TEST_DB_COLUMNS, txn);
  auto db_object = catalog->GetDatabaseObject(TEST_DB_COLUMNS, txn);
  oid_t database_id = db_object->GetDatabaseOid();
  int table_count = db_object->GetTableObjects().size();
=======
  catalog->CreateDatabase(txn, TEST_DB_COLUMNS);
  auto db = catalog->GetDatabaseWithName(txn, TEST_DB_COLUMNS);
  oid_t database_id = db->GetOid();
  auto db_object = catalog->GetDatabaseCatalogEntry(txn, TEST_DB_COLUMNS);
  int table_count = db_object->GetTableCatalogEntries().size();
>>>>>>> upstream/master
  txn_manager.CommitTransaction(txn);

  // Insert a 'test_table' with 'id', 'first_name' and 'last_name'
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  auto fname_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "first_name", false);
  auto lname_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "last_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, fname_column, lname_column}));

  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(txn,
                       TEST_DB_COLUMNS,
                       DEFAULT_SCHEMA_NAME,
                       std::move(table_schema),
                       "test_table",
                       false);
  txn_manager.CommitTransaction(txn);

  // Obtain ids for the table and columns
  txn = txn_manager.BeginTransaction();
<<<<<<< HEAD

  auto source_table = catalog->GetTableWithName(
      TEST_DB_COLUMNS, DEFAULT_SCHEMA_NAME, "test_table", txn);
=======
  auto source_table = catalog->GetTableWithName(txn,
                                                TEST_DB_COLUMNS,
                                                DEFAULT_SCHEMA_NAME,
                                                "test_table");
>>>>>>> upstream/master
  txn_manager.CommitTransaction(txn);

  oid_t table_id = source_table->GetOid();
  oid_t id_col_oid =
      source_table->GetSchema()->GetColumnID(id_column.GetName());
  oid_t fname_col_oid =
      source_table->GetSchema()->GetColumnID(fname_column.GetName());
  oid_t lname_col_oid =
      source_table->GetSchema()->GetColumnID(lname_column.GetName());

  // Insert a 'test_table_job' with 'age', 'job' and 'pid'
  txn = txn_manager.BeginTransaction();
  auto age_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "age", true);
  auto job_column = catalog::Column(type::TypeId::VARCHAR, 32, "job", false);
  auto pid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "pid", true);

  std::unique_ptr<catalog::Schema> job_table_schema(
      new catalog::Schema({age_column, job_column, pid_column}));
  catalog->CreateTable(txn,
                       TEST_DB_COLUMNS,
                       DEFAULT_SCHEMA_NAME,
                       std::move(job_table_schema),
                       "test_table_job",
                       false);
  txn_manager.CommitTransaction(txn);

  // Obtain ids for the table and columns
  txn = txn_manager.BeginTransaction();
  auto source_table_job = catalog->GetTableWithName(txn,
                                                    TEST_DB_COLUMNS,
                                                    DEFAULT_SCHEMA_NAME,
                                                    "test_table_job");
  oid_t table_job_id = source_table_job->GetOid();
  oid_t age_col_oid =
      source_table_job->GetSchema()->GetColumnID(age_column.GetName());
  oid_t job_col_oid =
      source_table_job->GetSchema()->GetColumnID(job_column.GetName());
  oid_t pid_col_oid =
      source_table_job->GetSchema()->GetColumnID(pid_column.GetName());
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  // This is required so that database objects are cached
  db_object = catalog->GetDatabaseCatalogEntry(txn, TEST_DB_COLUMNS);
  EXPECT_EQ(
      2, static_cast<int>(db_object->GetTableCatalogEntries().size()) - table_count);

  // ====== UPDATE statements check ===
  // id and first_name in test_table are affected
  std::string query_string =
      "UPDATE test_table SET last_name = '' WHERE id = 0 AND first_name = '';";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  auto bind_node_visitor = binder::BindNodeVisitor(txn, TEST_DB_COLUMNS);
  bind_node_visitor.BindNameToNode(sql_stmt);
  std::vector<planner::col_triplet> affected_cols_vector =
      planner::PlanUtil::GetIndexableColumns(
          txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  std::set<planner::col_triplet> affected_cols(affected_cols_vector.begin(),
                                               affected_cols_vector.end());
  EXPECT_EQ(2, static_cast<int>(affected_cols.size()));
  std::set<planner::col_triplet> expected_oids;
  expected_oids.emplace(database_id, table_id, id_col_oid);
  expected_oids.emplace(database_id, table_id, fname_col_oid);
  EXPECT_EQ(expected_oids, affected_cols);

  // no column is affected
  query_string = "UPDATE test_table SET last_name = '';";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(0, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  EXPECT_EQ(expected_oids, affected_cols);

  // ====== DELETE statements check ===
  // no column is affected
  query_string = "DELETE FROM test_table;";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(0, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  EXPECT_EQ(expected_oids, affected_cols);

  // id and last_name in test_table are affected
  query_string = "DELETE FROM test_table WHERE id = 0 AND last_name = '';";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(2, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  expected_oids.emplace(database_id, table_id, id_col_oid);
  expected_oids.emplace(database_id, table_id, lname_col_oid);
  EXPECT_EQ(expected_oids, affected_cols);

  // ========= INSERT statements check ==
  // no columns is affected
  query_string = "INSERT INTO test_table VALUES (1, 'pel', 'ton');";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(0, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  EXPECT_EQ(expected_oids, affected_cols);

  // ========= SELECT statement check ==
  // first_name and last_name in test_table are affected
  query_string = "SELECT id FROM test_table WHERE first_name = last_name;";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(2, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  expected_oids.emplace(database_id, table_id, lname_col_oid);
  expected_oids.emplace(database_id, table_id, fname_col_oid);
  EXPECT_EQ(expected_oids, affected_cols);

  // age, job and pid in test_table_job are affected
  query_string =
      "SELECT pid FROM test_table_job WHERE age > 20 AND job = '' AND pid > 5;";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(3, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  expected_oids.emplace(database_id, table_job_id, job_col_oid);
  expected_oids.emplace(database_id, table_job_id, age_col_oid);
  expected_oids.emplace(database_id, table_job_id, pid_col_oid);
  EXPECT_EQ(expected_oids, affected_cols);

  // last_name in test_table and job in test_table_job are affected
  query_string =
      "SELECT test_table.first_name, test_table_job.pid, test_table_job.age "
      "FROM test_table JOIN test_table_job ON test_table.id = "
      "test_table_job.pid WHERE test_table_job.pid > 0 AND "
      "test_table.last_name = '';";
  sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  sql_stmt = sql_stmt_list->GetStatement(0);
  bind_node_visitor.BindNameToNode(sql_stmt);
  affected_cols_vector = planner::PlanUtil::GetIndexableColumns(
      txn->catalog_cache, std::move(sql_stmt_list), TEST_DB_COLUMNS);
  affected_cols = std::set<planner::col_triplet>(affected_cols_vector.begin(),
                                                 affected_cols_vector.end());
  EXPECT_EQ(2, static_cast<int>(affected_cols.size()));
  expected_oids.clear();
  expected_oids.emplace(database_id, table_id, lname_col_oid);
  expected_oids.emplace(database_id, table_job_id, pid_col_oid);
  EXPECT_EQ(expected_oids, affected_cols);

  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
