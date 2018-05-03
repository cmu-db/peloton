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

  catalog->CreateDatabase(TEST_DB_NAME, txn);
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
  catalog->CreateTable(TEST_DB_NAME, DEFUALT_SCHEMA_NAME, "test_table",
                       std::move(table_schema), txn);
  auto source_table = catalog->GetTableWithName(
      TEST_DB_NAME, DEFUALT_SCHEMA_NAME, "test_table", txn);
  EXPECT_NE(source_table, nullptr);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  oid_t col_id = source_table->GetSchema()->GetColumnID(id_column.column_name);
  std::vector<oid_t> source_col_ids;
  source_col_ids.push_back(col_id);

  // create index on 'id'
  catalog->CreateIndex(TEST_DB_NAME, DEFUALT_SCHEMA_NAME, "test_table",
                       source_col_ids, "test_id_idx", false, IndexType::BWTREE,
                       txn);

  // create index on 'id' and 'first_name'
  col_id = source_table->GetSchema()->GetColumnID(fname_column.column_name);
  source_col_ids.push_back(col_id);

  catalog->CreateIndex(TEST_DB_NAME, DEFUALT_SCHEMA_NAME, "test_table",
                       source_col_ids, "test_fname_idx", false,
                       IndexType::BWTREE, txn);
  txn_manager.CommitTransaction(txn);

  // dummy txn to get the catalog_cache object
  txn = txn_manager.BeginTransaction();

  // This is also required so that database objects are cached
  auto db_object = catalog->GetDatabaseObject(TEST_DB_NAME, txn);

  // Till now, we have a table : id, first_name, last_name
  // And two indexes on following columns:
  // 1) id
  // 2) id and first_name
  auto table_object =
      db_object->GetTableObject("test_table", DEFUALT_SCHEMA_NAME);
  EXPECT_NE(table_object, nullptr);
  oid_t id_idx_oid = table_object->GetIndexObject("test_id_idx")->GetIndexOid();
  oid_t fname_idx_oid =
      table_object->GetIndexObject("test_fname_idx")->GetIndexOid();

  // An update query affecting both indexes
  std::string query_string = "UPDATE test_table SET id = 0;";
  std::unique_ptr<Statement> stmt(new Statement("UPDATE", query_string));
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  static_cast<parser::UpdateStatement *>(sql_stmt)
      ->table->TryBindDatabaseName(TEST_DB_NAME);
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
  static_cast<parser::UpdateStatement *>(sql_stmt)
      ->table->TryBindDatabaseName(TEST_DB_NAME);
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
  static_cast<parser::DeleteStatement *>(sql_stmt)
      ->TryBindDatabaseName(TEST_DB_NAME);
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
  static_cast<parser::InsertStatement *>(sql_stmt)
      ->TryBindDatabaseName(TEST_DB_NAME);
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

TEST_F(PlanUtilTests, GetIndexableColumnsTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(TEST_DB_COLUMNS, txn);
  auto db = catalog->GetDatabaseWithName(TEST_DB_COLUMNS, txn);
  oid_t database_id = db->GetOid();
  auto db_object = catalog->GetDatabaseObject(TEST_DB_COLUMNS, txn);
  int table_count = db_object->GetTableObjects().size();
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
  catalog->CreateTable(TEST_DB_COLUMNS, DEFUALT_SCHEMA_NAME, "test_table",
                       std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // Obtain ids for the table and columns
  txn = txn_manager.BeginTransaction();
  auto source_table = catalog->GetTableWithName(
      TEST_DB_COLUMNS, DEFUALT_SCHEMA_NAME, "test_table", txn);
  txn_manager.CommitTransaction(txn);

  oid_t table_id = source_table->GetOid();
  oid_t id_col_oid =
      source_table->GetSchema()->GetColumnID(id_column.column_name);
  oid_t fname_col_oid =
      source_table->GetSchema()->GetColumnID(fname_column.column_name);
  oid_t lname_col_oid =
      source_table->GetSchema()->GetColumnID(lname_column.column_name);

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
  catalog->CreateTable(TEST_DB_COLUMNS, DEFUALT_SCHEMA_NAME, "test_table_job",
                       std::move(job_table_schema), txn);
  txn_manager.CommitTransaction(txn);

  // Obtain ids for the table and columns
  txn = txn_manager.BeginTransaction();
  auto source_table_job = catalog->GetTableWithName(
      TEST_DB_COLUMNS, DEFUALT_SCHEMA_NAME, "test_table_job", txn);
  oid_t table_job_id = source_table_job->GetOid();
  oid_t age_col_oid =
      source_table_job->GetSchema()->GetColumnID(age_column.column_name);
  oid_t job_col_oid =
      source_table_job->GetSchema()->GetColumnID(job_column.column_name);
  oid_t pid_col_oid =
      source_table_job->GetSchema()->GetColumnID(pid_column.column_name);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  // This is required so that database objects are cached
  db_object = catalog->GetDatabaseObject(TEST_DB_COLUMNS, txn);
  EXPECT_EQ(
      2, static_cast<int>(db_object->GetTableObjects().size()) - table_count);

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
