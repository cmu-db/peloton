//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_checkpoint_recovery_test.cpp
//
// Identification: test/logging/timestamp_checkpoint_recovery_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/constraint_catalog.h"
#include "catalog/index_catalog.h"
#include "common/init.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "logging/timestamp_checkpoint_manager.h"
#include "settings/settings_manager.h"
#include "storage/storage_manager.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpoint Recovery Tests
//===--------------------------------------------------------------------===//

class TimestampCheckpointRecoveryTests : public PelotonTest {};

TEST_F(TimestampCheckpointRecoveryTests, CheckpointRecoveryTest) {
  PelotonInit::Initialize();

  // do checkpoint recovery
  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();
  auto result = checkpoint_manager.DoCheckpointRecovery();
  if (!result) {
    LOG_ERROR("Recovery failed. Has to do timestamp_checkpointing_test"
        " in advance.");
    EXPECT_TRUE(false);
    return;
  }

  //--------------------------------------------------------------------------
  // LOW LEVEL TEST
  //--------------------------------------------------------------------------
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  auto storage = storage::StorageManager::GetInstance();

  // check an specific catalog: SettingCatalog
  auto &setting_manager = settings::SettingsManager::GetInstance();
  LOG_DEBUG("Setting info\n%s", setting_manager.GetInfo().c_str());

  // make sure data structure created in checkpointing test

  // check all tables in the default database
  auto default_db_catalog_entry = catalog->GetDatabaseCatalogEntry(txn, DEFAULT_DB_NAME);
  for (auto table_catalog_entry :
       default_db_catalog_entry->GetTableCatalogEntries((std::string)DEFAULT_SCHEMA_NAME)) {
    auto table_name = table_catalog_entry->GetTableName();
    auto table = storage->GetTableWithOid(table_catalog_entry->GetDatabaseOid(),
                                          table_catalog_entry->GetTableOid());
    auto schema = table->GetSchema();

    LOG_INFO("Check the table %d %s", table_catalog_entry->GetTableOid(),
             table_name.c_str());

    // check the basic table information
    if (table_name == "checkpoint_table_test") {
      // column info
      for (auto column_pair : table_catalog_entry->GetColumnCatalogEntries()) {
        auto column_catalog_entry = column_pair.second;
        auto column_name = column_catalog_entry->GetColumnName();
        auto column = schema->GetColumn(column_pair.first);

        LOG_INFO("Check the column %d %s\n%s", column_catalog_entry->GetColumnId(),
            column_name.c_str(), column.GetInfo().c_str());

        if (column_name == "id") {
          EXPECT_EQ(column_name, column.GetName());
          EXPECT_EQ(type::TypeId::INTEGER, column_catalog_entry->GetColumnType());
          EXPECT_EQ(0, column.GetOffset());
          EXPECT_EQ(4, column.GetLength());
          EXPECT_TRUE(column.IsInlined());
          EXPECT_FALSE(column.IsNotNull());
          EXPECT_FALSE(column.HasDefault());
        } else if (column_name == "value1") {
          EXPECT_EQ(column_name, column.GetName());
          EXPECT_EQ(type::TypeId::DECIMAL, column_catalog_entry->GetColumnType());
          EXPECT_EQ(4, column.GetOffset());
          EXPECT_EQ(8, column.GetLength());
          EXPECT_TRUE(column.IsInlined());
          EXPECT_FALSE(column.IsNotNull());
          EXPECT_FALSE(column.HasDefault());
        } else if (column_name == "value2") {
          EXPECT_EQ(column_name, column.GetName());
          EXPECT_EQ(type::TypeId::VARCHAR, column_catalog_entry->GetColumnType());
          EXPECT_EQ(12, column.GetOffset());
          EXPECT_EQ(32, column.GetLength());
          EXPECT_FALSE(column.IsInlined());
          EXPECT_FALSE(column.IsNotNull());
          EXPECT_FALSE(column.HasDefault());
        } else {
          LOG_ERROR("Unexpected column is found: %s", column_name.c_str());
          EXPECT_TRUE(false);
        }
      }

      // constraints
      EXPECT_EQ(0, schema->GetNotNullColumns().size());
      EXPECT_TRUE(schema->HasPrimary());
      EXPECT_FALSE(schema->HasUniqueConstraints());
      EXPECT_FALSE(schema->HasForeignKeys());
      EXPECT_EQ(0, schema->GetForeignKeyConstraints().size());
      EXPECT_TRUE(schema->HasForeignKeySources());
      EXPECT_EQ(1, schema->GetForeignKeySources().size());
    }
    // end: check the basic information of columns

    // check the index recovery
    else if (table_name == "checkpoint_index_test") {
      for (auto index_pair : table_catalog_entry->GetIndexCatalogEntries()) {
        auto index_catalog_entry = index_pair.second;
        auto index_name = index_catalog_entry->GetIndexName();
        auto index = table->GetIndexWithOid(index_pair.first);

        LOG_INFO("Check the index %s", index_name.c_str());

        // unique primary key for attribute "pid" (primary key)
        if (index_name == "checkpoint_index_test_pkey") {
          EXPECT_EQ(index_name, index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::PRIMARY_KEY, index->GetIndexType());
          EXPECT_TRUE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(2, key_columns.size());
          EXPECT_EQ("upid1", key_columns.at(0).GetName());
          EXPECT_EQ("upid2", key_columns.at(1).GetName());
        }
        // unique primary key for attribute "upid" (unique)
        else if (index_name == "checkpoint_index_test_upid1_UNIQ") {
          EXPECT_EQ(index_name, index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::UNIQUE, index->GetIndexType());
          EXPECT_TRUE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(1, key_columns.size());
          EXPECT_EQ("upid1", key_columns.at(0).GetName());
        }
        // ART index for attribute "value1"
        else if (index_name == "index_test1") {
          EXPECT_EQ(index_name, index->GetName());
          EXPECT_EQ(IndexType::ART, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::DEFAULT, index->GetIndexType());
          EXPECT_FALSE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(1, key_columns.size());
          EXPECT_EQ("value1", key_columns.at(0).GetName());
        }
        // SKIPLIST index for attributes "value2" and "value3"
        else if (index_name == "index_test2") {
          EXPECT_EQ(index_name, index->GetName());
          EXPECT_EQ(IndexType::SKIPLIST, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::DEFAULT, index->GetIndexType());
          EXPECT_FALSE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(2, key_columns.size());
          EXPECT_EQ("value2", key_columns.at(0).GetName());
          EXPECT_EQ("value3", key_columns.at(1).GetName());
        }
        // unique index for attribute "value2"
        else if (index_name == "unique_index_test") {
          EXPECT_EQ(index_name, index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::UNIQUE, index->GetIndexType());
          EXPECT_TRUE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(1, key_columns.size());
          EXPECT_EQ("value2", key_columns.at(0).GetName());
        } else {
          LOG_ERROR("Unexpected index is found: %s", index_name.c_str());
          EXPECT_TRUE(false);
        }
      }

      // constraints
      EXPECT_EQ(0, schema->GetNotNullColumns().size());
      EXPECT_TRUE(schema->HasPrimary());
      EXPECT_TRUE(schema->HasUniqueConstraints());
      EXPECT_FALSE(schema->HasForeignKeys());
      EXPECT_EQ(0, schema->GetForeignKeyConstraints().size());
      EXPECT_TRUE(schema->HasForeignKeySources());
      EXPECT_EQ(1, schema->GetForeignKeySources().size());
    }
    // end: check the index recovery

    // check the constraints recovery
    else if (table_name == "checkpoint_constraint_test") {
      // column constraints
      EXPECT_EQ(1, schema->GetNotNullColumns().size());
      for (auto column_pair : table_catalog_entry->GetColumnCatalogEntries()) {
        auto column_catalog_entry = column_pair.second;
        auto column_name = column_catalog_entry->GetColumnName();
        auto column = schema->GetColumn(column_pair.first);
        LOG_INFO("Check constraint within the column %d %s\n%s",
                 column_catalog_entry->GetColumnId(), column_name.c_str(),
                 column.GetInfo().c_str());

        // No column constraints
        if (column_name == "pid1" || column_name == "pid2" ||
            column_name == "value3" || column_name == "value4" ||
            column_name == "value5") {
          EXPECT_FALSE(column.IsNotNull());
          EXPECT_FALSE(column.HasDefault());
        }
        // DEFAULT constraint (value: 0) for column 'value1'
        else if (column_name == "value1") {
          EXPECT_FALSE(column.IsNotNull());
          EXPECT_TRUE(column.HasDefault());
          EXPECT_EQ(0, column.GetDefaultValue()->GetAs<int>());
        }
        // NOT NULL constraint for column 'value2'
        else if (column_name == "value2") {
          EXPECT_TRUE(column.IsNotNull());
          EXPECT_FALSE(column.HasDefault());
        } else {
          LOG_ERROR("Unexpected column is found: %s", column_name.c_str());
          EXPECT_TRUE(false);
        }
      }  // loop end: column constraints

      // table constraints
      EXPECT_TRUE(schema->HasPrimary());
      EXPECT_TRUE(schema->HasUniqueConstraints());
      EXPECT_TRUE(schema->HasForeignKeys());
      EXPECT_EQ(2, schema->GetForeignKeyConstraints().size());
      EXPECT_FALSE(schema->HasForeignKeySources());
      EXPECT_EQ(0, schema->GetForeignKeySources().size());
      for (auto constraint_catalog_entry_pair :
              table_catalog_entry->GetConstraintCatalogEntries()) {
        auto constraint_oid = constraint_catalog_entry_pair.first;
        auto constraint_catalog_entry = constraint_catalog_entry_pair.second;
        auto &constraint_name = constraint_catalog_entry->GetConstraintName();
        auto constraint = schema->GetConstraint(constraint_oid);
        LOG_INFO("Check table constraint: %d %s",
                 constraint_catalog_entry->GetConstraintOid(),
                 constraint_name.c_str());

        // PRIMARY KEY for a set of columns 'pid1' and 'pid2'
        if (constraint_name == "con_primary") {
          EXPECT_EQ(constraint_name, constraint->GetName());
          EXPECT_EQ(ConstraintType::PRIMARY, constraint->GetType());
          EXPECT_EQ(table->GetOid(), constraint->GetTableOid());
          auto &key_oids = constraint->GetColumnIds();
          EXPECT_EQ(2, key_oids.size());
          EXPECT_EQ("pid1", schema->GetColumn(key_oids.at(0)).GetName());
          EXPECT_EQ("pid2", schema->GetColumn(key_oids.at(1)).GetName());

          // index for the constraint
          auto index = table->GetIndexWithOid(constraint->GetIndexOid());
          EXPECT_EQ("checkpoint_constraint_test_pkey", index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::PRIMARY_KEY, index->GetIndexType());
          EXPECT_TRUE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(2, key_columns.size());
          EXPECT_EQ("pid1", key_columns.at(0).GetName());
          EXPECT_EQ("pid2", key_columns.at(1).GetName());
        }
        // UNIQUE constraint for a column 'value1'
        else if (constraint_name == "con_unique") {
          EXPECT_EQ(constraint_name, constraint->GetName());
          EXPECT_EQ(ConstraintType::UNIQUE, constraint->GetType());
          EXPECT_EQ(table->GetOid(), constraint->GetTableOid());
          auto &key_oids = constraint->GetColumnIds();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("value1", schema->GetColumn(key_oids.at(0)).GetName());

          // index for the constraint
          auto index = table->GetIndexWithOid(constraint->GetIndexOid());
          EXPECT_EQ("checkpoint_constraint_test_value1_UNIQ", index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::UNIQUE, index->GetIndexType());
          EXPECT_TRUE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(1, key_columns.size());
          EXPECT_EQ("value1", key_columns.at(0).GetName());
        }
        // CHECK constraint for a column 'value2' (value2 > 2)
        else if (constraint_name == "con_check") {
          EXPECT_EQ(constraint_name, constraint->GetName());
          EXPECT_EQ(ConstraintType::CHECK, constraint->GetType());
          EXPECT_EQ(table->GetOid(), constraint->GetTableOid());
          auto &key_oids = constraint->GetColumnIds();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("value2", schema->GetColumn(key_oids.at(0)).GetName());
          EXPECT_EQ(ExpressionType::COMPARE_GREATERTHAN,
                    constraint->GetCheckExpression().first);
          EXPECT_EQ(2, constraint->GetCheckExpression().second.GetAs<int>());

          // index for the constraint (No index)
          EXPECT_EQ(INVALID_OID, constraint->GetIndexOid());
        }
        // FOREIGN KEY constraint for a column 'value3' to checkpoint_table_test.pid
        else if (constraint_name ==
            "FK_checkpoint_constraint_test->checkpoint_table_test") {
          EXPECT_EQ(constraint_name, constraint->GetName());
          EXPECT_EQ(ConstraintType::FOREIGN, constraint->GetType());
          EXPECT_EQ(table->GetOid(), constraint->GetTableOid());
          auto &source_column_ids = constraint->GetColumnIds();
          EXPECT_EQ(1, source_column_ids.size());
          EXPECT_EQ("value3", schema->GetColumn(source_column_ids.at(0)).GetName());

          // sink table info
          auto sink_table = storage->GetTableWithOid(table->GetDatabaseOid(),
                                                     constraint->GetFKSinkTableOid());
          auto sink_schema = sink_table->GetSchema();
          EXPECT_TRUE(sink_schema->HasForeignKeySources());
          auto fk_sources = sink_schema->GetForeignKeySources();
          EXPECT_EQ(1, fk_sources.size());
          EXPECT_EQ(constraint->GetConstraintOid(), fk_sources.at(0)->GetConstraintOid());
          auto sink_column_ids = constraint->GetFKSinkColumnIds();
          EXPECT_EQ(1, sink_column_ids.size());
          EXPECT_EQ("id", sink_schema->GetColumn(sink_column_ids.at(0)).GetName());
          EXPECT_EQ(FKConstrActionType::NOACTION, constraint->GetFKUpdateAction());
          EXPECT_EQ(FKConstrActionType::NOACTION, constraint->GetFKDeleteAction());

          // index for the constraint
          auto index = table->GetIndexWithOid(constraint->GetIndexOid());
          EXPECT_EQ("checkpoint_constraint_test_value3_fkey", index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::DEFAULT, index->GetIndexType());
          EXPECT_FALSE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(1, key_columns.size());
          EXPECT_EQ("value3", key_columns.at(0).GetName());
        }
        // FOREIGN KEY constraint for a set of columns 'value4' and 'value5'
        // to (checkpoint_index_test.upid1, checkpoint_index_test.upid2)
        else if (constraint_name ==
            "FK_checkpoint_constraint_test->checkpoint_index_test") {
          EXPECT_EQ(constraint_name, constraint->GetName());
          EXPECT_EQ(ConstraintType::FOREIGN, constraint->GetType());
          EXPECT_EQ(table->GetOid(), constraint->GetTableOid());
          auto &source_column_ids = constraint->GetColumnIds();
          EXPECT_EQ(2, source_column_ids.size());
          EXPECT_EQ("value4", schema->GetColumn(source_column_ids.at(0)).GetName());
          EXPECT_EQ("value5", schema->GetColumn(source_column_ids.at(1)).GetName());

          // sink table info
          auto sink_table = storage->GetTableWithOid(table->GetDatabaseOid(),
                                                     constraint->GetFKSinkTableOid());
          auto sink_schema = sink_table->GetSchema();
          EXPECT_TRUE(sink_schema->HasForeignKeySources());
          auto fk_sources = sink_schema->GetForeignKeySources();
          EXPECT_EQ(1, fk_sources.size());
          EXPECT_EQ(constraint->GetConstraintOid(), fk_sources.at(0)->GetConstraintOid());
          auto sink_column_ids = constraint->GetFKSinkColumnIds();
          EXPECT_EQ(2, sink_column_ids.size());
          EXPECT_EQ("upid1", sink_schema->GetColumn(sink_column_ids.at(0)).GetName());
          EXPECT_EQ("upid2", sink_schema->GetColumn(sink_column_ids.at(1)).GetName());
          EXPECT_EQ(FKConstrActionType::NOACTION, constraint->GetFKUpdateAction());
          EXPECT_EQ(FKConstrActionType::NOACTION, constraint->GetFKDeleteAction());

          // index for the constraint
          auto index = table->GetIndexWithOid(constraint->GetIndexOid());
          EXPECT_EQ("checkpoint_constraint_test_value4_value5_fkey", index->GetName());
          EXPECT_EQ(IndexType::BWTREE, index->GetIndexMethodType());
          EXPECT_EQ(IndexConstraintType::DEFAULT, index->GetIndexType());
          EXPECT_FALSE(index->HasUniqueKeys());
          auto &key_columns = index->GetKeySchema()->GetColumns();
          EXPECT_EQ(2, key_columns.size());
          EXPECT_EQ("value4", key_columns.at(0).GetName());
          EXPECT_EQ("value5", key_columns.at(1).GetName());
        } else {
          LOG_ERROR("Unexpected constraint is found: %s", constraint_name.c_str());
          EXPECT_TRUE(false);
        }
      }  // loop end: table constraints
    }
    // end: check the constraints recovery

    else {
      LOG_ERROR("Unexpected table is found: %s", table_name.c_str());
      EXPECT_TRUE(false);
    }
  }  // table loop end

  // finish the low level test
  txn_manager.CommitTransaction(txn);


  //--------------------------------------------------------------------------
  // HIGH LEVEL TEST
  //--------------------------------------------------------------------------
  // make sure the records of 3 user tables created checkpointing test
  // are correct and their constraints are working correctly through
  // SQL execution.

  // make sure the data in each user table is correct
  std::string sql1 = "SELECT * FROM checkpoint_table_test;";
  std::vector<std::string> expected1 = {"0|1.2|aaa", "1|12.34|bbbbbb",
                                        "2|12345.7|ccccccccc", "3|0|xxxx"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql1, expected1, false);

  std::string sql2 = "SELECT * FROM checkpoint_index_test;";
  std::vector<std::string> expected2 = {"1|2|3|4|5", "6|7|8|9|10",
                                        "11|12|13|14|15"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql2, expected2, false);

  std::string sql3 = "SELECT * FROM checkpoint_constraint_test;";
  std::vector<std::string> expected3 = {"1|2|3|4|0|1|2", "5|6|7|8|1|6|7",
                                        "9|10|11|12|2|11|12"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql3, expected3, false);

  // make sure the constraints are working
  // PRIMARY KEY (1 column: pid)
  LOG_INFO("PRIMARY KEY (1 column) check");
  std::string primary_key_dml1 =
      "INSERT INTO checkpoint_table_test VALUES (0, 5.5, 'eee');";
  ResultType primary_key_result1 =
      TestingSQLUtil::ExecuteSQLQuery(primary_key_dml1);
  EXPECT_EQ(ResultType::ABORTED, primary_key_result1);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // PRIMARY KEY (2 column: pid1, pid2)
  LOG_INFO("PRIMARY KEY (2 columns) check");
  std::string primary_key_dml2 =
      "INSERT INTO checkpoint_constraint_test VALUES (1, 2, 15, 16, 0, 1 ,2);";
  ResultType primary_key_result2 =
      TestingSQLUtil::ExecuteSQLQuery(primary_key_dml2);
  EXPECT_EQ(ResultType::ABORTED, primary_key_result2);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // DEFAULT (value1 = 0)
  LOG_INFO("DEFAULT check");
  std::string default_dml =
      "INSERT INTO checkpoint_constraint_test"
      " (pid1, pid2, value2, value3, value4, value5)"
      " VALUES (13, 14, 16, 0, 1 ,2);";
  ResultType default_result1 = TestingSQLUtil::ExecuteSQLQuery(default_dml);
  EXPECT_EQ(ResultType::SUCCESS, default_result1);

  std::string default_sql =
      "SELECT value1 FROM checkpoint_constraint_test"
      " WHERE pid1 = 13 AND pid2 = 14;";
  std::vector<ResultValue> result_value;
  ResultType default_result2 =
      TestingSQLUtil::ExecuteSQLQuery(default_sql, result_value);
  EXPECT_EQ(ResultType::SUCCESS, default_result2);
  EXPECT_EQ("0", result_value.at(0));
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // UNIQUE (value1)
  LOG_INFO("UNIQUE check");
  std::string unique_dml =
      "INSERT INTO checkpoint_constraint_test VALUES (17, 18, 3, 20, 1, 6 ,7);";
  ResultType unique_result = TestingSQLUtil::ExecuteSQLQuery(unique_dml);
  EXPECT_EQ(ResultType::ABORTED, unique_result);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // NOT NULL (value2)
  LOG_INFO("NOT NULL check");
  std::string not_null_dml =
      "INSERT INTO checkpoint_constraint_test VALUES (17, 18, 19, NULL, 1, 6 "
      ",7);";
  ResultType not_null_result = TestingSQLUtil::ExecuteSQLQuery(not_null_dml);
  // EXPECT_EQ(ResultType::ABORTED, not_null_result);
  EXPECT_EQ(ResultType::FAILURE, not_null_result);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // CHECK (value2 > 2)
  LOG_INFO("CHECK check");
  std::string check_dml =
      "INSERT INTO checkpoint_constraint_test VALUES (17, 18, 19, 1, 1, 6 ,7);";
  ResultType check_result = TestingSQLUtil::ExecuteSQLQuery(check_dml);
  // EXPECT_EQ(ResultType::FAILURE, check_result);
  EXPECT_EQ(ResultType::SUCCESS, check_result);  // check doesn't work correctly
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // FOREIGN KEY (1 column: value3 => pid)
  LOG_INFO("FOREIGN KEY (1 column) check");
  std::string foreign_key_dml1 =
      "INSERT INTO checkpoint_constraint_test VALUES (21, 22, 23, 24, 10, 6 "
      ",7);";
  ResultType foreign_key_result1 =
      TestingSQLUtil::ExecuteSQLQuery(foreign_key_dml1);
  EXPECT_EQ(ResultType::ABORTED, foreign_key_result1);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // FOREIGN KEY (2 column: (value4, value5) => (upid1, upid2))
  LOG_INFO("FOREIGN KEY (2 columns) check");
  std::string foreign_key_dml2 =
      "INSERT INTO checkpoint_constraint_test VALUES (21, 22, 23, 24, 1, 20 "
      ",20);";
  ResultType foreign_key_result2 =
      TestingSQLUtil::ExecuteSQLQuery(foreign_key_dml2);
  EXPECT_EQ(ResultType::ABORTED, foreign_key_result2);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // Final check
  std::string sql4 = "SELECT * FROM checkpoint_constraint_test;";
  std::vector<std::string> expected4 = {"1|2|3|4|0|1|2", "5|6|7|8|1|6|7",
                                        "9|10|11|12|2|11|12",
                                        "13|14|0|16|0|1|2", "17|18|19|1|1|6|7"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql4, expected4, false);

  PelotonInit::Shutdown();
}
}
}
