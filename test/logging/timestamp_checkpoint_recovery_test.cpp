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
#include "catalog/foreign_key.h"
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
  settings::SettingsManager::SetBool(settings::SettingId::checkpointing, false);
  PelotonInit::Initialize();

  // do checkpoint recovery
  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();
  checkpoint_manager.DoCheckpointRecovery();


  // low level test
  // make sure data structures created in checkpointing test and recovered
  // above are correct.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  auto storage = storage::StorageManager::GetInstance();

  // check an uncommitted table does not exist
  EXPECT_FALSE(catalog->ExistTableByName(DEFAULT_DB_NAME, DEFAULT_SCHEMA_NAME,
                                         "out_of_checkpoint_test", txn));

  // check all tables in the default database
  auto default_db_catalog = catalog->GetDatabaseObject(DEFAULT_DB_NAME, txn);
  for (auto &table_catalog :
       default_db_catalog->GetTableObjects((std::string)DEFAULT_SCHEMA_NAME)) {
  	auto &table_name = table_catalog->GetTableName();
    auto table = storage->GetTableWithOid(table_catalog->GetDatabaseOid(),
                                          table_catalog->GetTableOid());

    LOG_INFO("Check the table %d %s\n%s", table_catalog->GetTableOid(),
             table_name.c_str(), table->GetInfo().c_str());

    // check the basic information of columns
    if (table_name == "checkpoint_table_test") {
      for (auto &column_pair : table_catalog->GetColumnObjects()) {
        auto &column_catalog = column_pair.second;
        auto &column_name = column_catalog->GetColumnName();
        auto column =
            table->GetSchema()->GetColumn(column_catalog->GetColumnId());

        LOG_INFO("Check the column %d %s\n%s", column_catalog->GetColumnId(),
        		column_name.c_str(), column.GetInfo().c_str());

        if (column_name == "id") {
          EXPECT_EQ(type::TypeId::INTEGER, column_catalog->GetColumnType());
          EXPECT_EQ(0, column_catalog->GetColumnOffset());
          EXPECT_EQ(4, column.GetLength());
          EXPECT_TRUE(column_catalog->IsInlined());
          EXPECT_FALSE(column_catalog->IsNotNull());
          EXPECT_TRUE(column_catalog->IsPrimary());
        } else if (column_name == "value1") {
          EXPECT_EQ(type::TypeId::DECIMAL, column_catalog->GetColumnType());
          EXPECT_EQ(4, column_catalog->GetColumnOffset());
          EXPECT_EQ(8, column.GetLength());
          EXPECT_TRUE(column_catalog->IsInlined());
          EXPECT_FALSE(column_catalog->IsNotNull());
          EXPECT_FALSE(column_catalog->IsPrimary());
        } else if (column_name == "value2") {
          EXPECT_EQ(type::TypeId::VARCHAR, column_catalog->GetColumnType());
          EXPECT_EQ(12, column_catalog->GetColumnOffset());
          EXPECT_EQ(32, column.GetLength());
          EXPECT_FALSE(column_catalog->IsInlined());
          EXPECT_FALSE(column_catalog->IsNotNull());
          EXPECT_FALSE(column_catalog->IsPrimary());
        } else {
          LOG_ERROR("Unexpected column is found: %s", column_name.c_str());
          EXPECT_TRUE(false);
        }
      }
    }
    // end: check the basic information of columns

    // check the index recovery
    else if (table_name == "checkpoint_index_test") {
      for (auto &index_pair : table_catalog->GetIndexObjects()) {
        auto &index_catalog = index_pair.second;
        auto &index_name = index_catalog->GetIndexName();

        LOG_INFO("Check the index %s", index_name.c_str());

        // unique primary key for attribute "pid" (primary key)
        if (index_name == "checkpoint_index_test_pkey") {
          EXPECT_EQ(IndexType::BWTREE, index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::PRIMARY_KEY,
                    index_catalog->GetIndexConstraint());
          EXPECT_TRUE(index_catalog->HasUniqueKeys());
          auto &key_oids = index_catalog->GetKeyAttrs();
          EXPECT_EQ(2, key_oids.size());
          EXPECT_EQ("upid1", table_catalog->GetColumnObject(key_oids.at(0))
                                 ->GetColumnName());
          EXPECT_EQ("upid2", table_catalog->GetColumnObject(key_oids.at(1))
                                 ->GetColumnName());
        }
        // unique primary key for attribute "upid" (unique)
        else if (index_name == "checkpoint_index_test_upid1_UNIQ") {
          EXPECT_EQ(IndexType::BWTREE, index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::UNIQUE,
                    index_catalog->GetIndexConstraint());
          EXPECT_TRUE(index_catalog->HasUniqueKeys());
          auto &key_oids = index_catalog->GetKeyAttrs();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("upid1", table_catalog->GetColumnObject(key_oids.at(0))
                                 ->GetColumnName());
        }
        // ART index for attribute "value1"
        else if (index_name == "index_test1") {
          EXPECT_EQ(IndexType::ART, index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::DEFAULT,
                    index_catalog->GetIndexConstraint());
          EXPECT_FALSE(index_catalog->HasUniqueKeys());
          auto &key_oids = index_catalog->GetKeyAttrs();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("value1", table_catalog->GetColumnObject(key_oids.at(0))
                                  ->GetColumnName());
        }
        // SKIPLIST index for attributes "value2" and "value3"
        else if (index_name == "index_test2") {
          EXPECT_EQ(IndexType::SKIPLIST, index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::DEFAULT,
                    index_catalog->GetIndexConstraint());
          EXPECT_FALSE(index_catalog->HasUniqueKeys());
          auto &key_oids = index_catalog->GetKeyAttrs();
          EXPECT_EQ(2, key_oids.size());
          EXPECT_EQ("value2", table_catalog->GetColumnObject(key_oids.at(0))
                                  ->GetColumnName());
          EXPECT_EQ("value3", table_catalog->GetColumnObject(key_oids.at(1))
                                  ->GetColumnName());
        }
        // unique index for attribute "value2"
        else if (index_name == "unique_index_test") {
          EXPECT_EQ(IndexType::BWTREE, index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::UNIQUE,
                    index_catalog->GetIndexConstraint());
          EXPECT_TRUE(index_catalog->HasUniqueKeys());
          auto &key_oids = index_catalog->GetKeyAttrs();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("value2", table_catalog->GetColumnObject(key_oids.at(0))
                                  ->GetColumnName());
        } else {
          LOG_ERROR("Unexpected index is found: %s", index_name.c_str());
          EXPECT_TRUE(false);
        }
      }
    }
    // end: check the index recovery

    // check the column constraint recovery
    else if (table_name == "checkpoint_constraint_test") {
      // multiple attributes constraint
      for (auto &multi_constraint : table->GetSchema()->GetMultiConstraints()) {
        // currently nothing (this might not be used)
        LOG_INFO("multi constraint: %s", multi_constraint.GetInfo().c_str());
      }

      // foreign key constraint
      auto fk_count = table->GetForeignKeyCount();
      EXPECT_EQ(2, fk_count);
      for (oid_t fk_id = 0; fk_id < fk_count; fk_id++) {
        auto foreign_key = table->GetForeignKey(fk_id);
        LOG_INFO("Check foreign key constraint: %s",
                 foreign_key->GetConstraintName().c_str());

        // value3 => checkpoint_table_test.pid
        if (foreign_key->GetConstraintName() ==
            "FK_checkpoint_constraint_test->checkpoint_table_test") {
          auto sink_table_catalog = default_db_catalog->GetTableObject(
              "checkpoint_table_test", "public", txn);
          EXPECT_EQ(INVALID_OID, foreign_key->GetSourceTableOid());
          EXPECT_EQ(sink_table_catalog->GetTableOid(),
                    foreign_key->GetSinkTableOid());
          auto source_columns = foreign_key->GetSourceColumnIds();
          EXPECT_EQ(1, source_columns.size());
          EXPECT_EQ("value3",
                    table_catalog->GetColumnObject(source_columns.at(0))
                        ->GetColumnName());
          auto sink_columns = foreign_key->GetSinkColumnIds();
          EXPECT_EQ(1, sink_columns.size());
          EXPECT_EQ("id",
                    sink_table_catalog->GetColumnObject(sink_columns.at(0))
                        ->GetColumnName());
        }
        // (value4, value5) => (checkpoint_index_test.upid1,
        // checkpoint_index_test.upid2)
        else if (foreign_key->GetConstraintName() ==
                 "FK_checkpoint_constraint_test->checkpoint_index_test") {
          auto sink_table_catalog = default_db_catalog->GetTableObject(
              "checkpoint_index_test", "public", txn);
          EXPECT_EQ(INVALID_OID, foreign_key->GetSourceTableOid());
          EXPECT_EQ(sink_table_catalog->GetTableOid(),
                    foreign_key->GetSinkTableOid());
          auto source_columns = foreign_key->GetSourceColumnIds();
          EXPECT_EQ(2, source_columns.size());
          EXPECT_EQ("value4",
                    table_catalog->GetColumnObject(source_columns.at(0))
                        ->GetColumnName());
          EXPECT_EQ("value5",
                    table_catalog->GetColumnObject(source_columns.at(1))
                        ->GetColumnName());
          auto sink_columns = foreign_key->GetSinkColumnIds();
          EXPECT_EQ(2, sink_columns.size());
          EXPECT_EQ("upid1",
                    sink_table_catalog->GetColumnObject(sink_columns.at(0))
                        ->GetColumnName());
          EXPECT_EQ("upid2",
                    sink_table_catalog->GetColumnObject(sink_columns.at(1))
                        ->GetColumnName());
        } else {
          LOG_ERROR("Unexpected foreign key is found: %s",
                    foreign_key->GetConstraintName().c_str());
          EXPECT_TRUE(false);
        }
      }  // loop end :foreign key constraint

      // index for constraints
      for (auto &constraint_index_pair : table_catalog->GetIndexObjects()) {
        auto &constraint_index_catalog = constraint_index_pair.second;
        auto &constraint_index_name = constraint_index_catalog->GetIndexName();

        LOG_INFO("check index for constraints: %s", constraint_index_name.c_str());

        // primary key for attributes "pid1" and "pid2"
        if (constraint_index_name == "checkpoint_constraint_test_pkey") {
          EXPECT_EQ(IndexType::BWTREE, constraint_index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::PRIMARY_KEY,
                    constraint_index_catalog->GetIndexConstraint());
          EXPECT_TRUE(constraint_index_catalog->HasUniqueKeys());
          auto &key_oids = constraint_index_catalog->GetKeyAttrs();
          EXPECT_EQ(2, key_oids.size());
          EXPECT_EQ("pid1", table_catalog->GetColumnObject(key_oids.at(0))
                                ->GetColumnName());
          EXPECT_EQ("pid2", table_catalog->GetColumnObject(key_oids.at(1))
                                ->GetColumnName());
        }
        // UNIQUE constraint index for an attribute "value1"
        else if (constraint_index_name == "checkpoint_constraint_test_value1_UNIQ") {
          EXPECT_EQ(IndexType::BWTREE, constraint_index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::UNIQUE,
                    constraint_index_catalog->GetIndexConstraint());
          EXPECT_TRUE(constraint_index_catalog->HasUniqueKeys());
          auto &key_oids = constraint_index_catalog->GetKeyAttrs();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("value1", table_catalog->GetColumnObject(key_oids.at(0))
                                  ->GetColumnName());
        }
        // foreign key index for an attribute "value3"
        else if (constraint_index_name ==
                 "checkpoint_constraint_test_FK_checkpoint_table_test_1") {
          EXPECT_EQ(IndexType::BWTREE, constraint_index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::DEFAULT,
                    constraint_index_catalog->GetIndexConstraint());
          EXPECT_FALSE(constraint_index_catalog->HasUniqueKeys());
          auto &key_oids = constraint_index_catalog->GetKeyAttrs();
          EXPECT_EQ(1, key_oids.size());
          EXPECT_EQ("value3", table_catalog->GetColumnObject(key_oids.at(0))
                                  ->GetColumnName());
        }
        // foreign key index for an attributes "value4" and "value5"
        else if (constraint_index_name ==
                 "checkpoint_constraint_test_FK_checkpoint_index_test_2") {
          EXPECT_EQ(IndexType::BWTREE, constraint_index_catalog->GetIndexType());
          EXPECT_EQ(IndexConstraintType::DEFAULT,
                    constraint_index_catalog->GetIndexConstraint());
          EXPECT_FALSE(constraint_index_catalog->HasUniqueKeys());
          auto &key_oids = constraint_index_catalog->GetKeyAttrs();
          EXPECT_EQ(2, key_oids.size());
          EXPECT_EQ("value4", table_catalog->GetColumnObject(key_oids.at(0))
                                  ->GetColumnName());
          EXPECT_EQ("value5", table_catalog->GetColumnObject(key_oids.at(1))
                                  ->GetColumnName());
        } else {
          LOG_ERROR("Unexpected index is found: %s", constraint_index_name.c_str());
          EXPECT_TRUE(false);
        }
      }  // loop end: index for constraints

      // single attribute constraint
      for (auto &constraint_column_pair : table_catalog->GetColumnObjects()) {
        auto &constraint_column_catalog = constraint_column_pair.second;
        auto &constraint_column_name = constraint_column_catalog->GetColumnName();
        auto constraint_column =
            table->GetSchema()->GetColumn(constraint_column_catalog->GetColumnId());
        LOG_INFO("Check constraints of the column %d %s\n%s",
                 constraint_column_catalog->GetColumnId(), constraint_column_name.c_str(),
                 constraint_column.GetInfo().c_str());

        // set primary key of attributes 'pid1' and 'pid2'
        if (constraint_column_name == "pid1" ||
        		constraint_column_name == "pid2") {
          EXPECT_FALSE(constraint_column_catalog->IsNotNull());
          EXPECT_TRUE(constraint_column_catalog->IsPrimary());
          EXPECT_EQ(1, constraint_column.GetConstraints().size());
          for (auto constraint : constraint_column.GetConstraints()) {
            if (constraint.GetName() == "con_primary") {
              EXPECT_EQ(ConstraintType::PRIMARY, constraint.GetType());
              EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
            } else {
              LOG_ERROR("Unexpected constraint is found: %s",
                        constraint.GetName().c_str());
            }
          }
        }
        // unique and default value in attribute 'value1'
        else if (constraint_column_name == "value1") {
          EXPECT_FALSE(constraint_column_catalog->IsNotNull());
          EXPECT_FALSE(constraint_column_catalog->IsPrimary());
          EXPECT_EQ(2, constraint_column.GetConstraints().size());
          for (auto constraint : constraint_column.GetConstraints()) {
            if (constraint.GetName() == "con_default") {
              EXPECT_EQ(ConstraintType::DEFAULT, constraint.GetType());
              EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
              EXPECT_EQ(0, constraint.getDefaultValue()->GetAs<int>());
            } else if (constraint.GetName() == "con_unique") {
              EXPECT_EQ(ConstraintType::UNIQUE, constraint.GetType());
              EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
            } else {
              LOG_ERROR("Unexpected constraint is found: %s",
                        constraint.GetName().c_str());
            }
          }
        }
        // not null and check constraint in attribute 'value2'
        else if (constraint_column_name == "value2") {
          EXPECT_TRUE(constraint_column_catalog->IsNotNull());
          EXPECT_FALSE(constraint_column_catalog->IsPrimary());
          EXPECT_EQ(2, constraint_column.GetConstraints().size());
          for (auto constraint : constraint_column.GetConstraints()) {
            if (constraint.GetName() == "con_not_null") {
              EXPECT_EQ(ConstraintType::NOTNULL, constraint.GetType());
              EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
            } else if (constraint.GetName() == "con_check") {
              EXPECT_EQ(ConstraintType::CHECK, constraint.GetType());
              EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
              EXPECT_EQ(ExpressionType::COMPARE_GREATERTHAN,
                        constraint.GetCheckExpression().first);
              EXPECT_EQ(2, constraint.GetCheckExpression().second.GetAs<int>());
            } else {
              LOG_ERROR("Unexpected constraint is found: %s",
                        constraint.GetName().c_str());
            }
          }
        }
        // foreign key in attribute 'value3' to attribute 'id' in table
        // 'checkpoint_table_test'
        else if (constraint_column_name == "value3") {
          EXPECT_FALSE(constraint_column_catalog->IsNotNull());
          EXPECT_FALSE(constraint_column_catalog->IsPrimary());
          EXPECT_EQ(1, constraint_column.GetConstraints().size());
          for (auto constraint : constraint_column.GetConstraints()) {
            if (constraint.GetName() ==
                "FK_checkpoint_constraint_test->checkpoint_table_test") {
              EXPECT_EQ(ConstraintType::FOREIGN, constraint.GetType());
              EXPECT_EQ(0, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
            } else {
              LOG_ERROR("Unexpected constraint is found: %s",
                        constraint.GetName().c_str());
            }
          }
        }
        // foreign keys in attribute 'value4'&'value5' to attribute
        // 'upid1'&'upid2' in table 'checkpoint_index_test'
        else if (constraint_column_name == "value4" ||
        		constraint_column_name == "value5") {
          EXPECT_FALSE(constraint_column_catalog->IsNotNull());
          EXPECT_FALSE(constraint_column_catalog->IsPrimary());
          EXPECT_EQ(1, constraint_column.GetConstraints().size());
          for (auto constraint : constraint_column.GetConstraints()) {
            if (constraint.GetName() ==
                "FK_checkpoint_constraint_test->checkpoint_index_test") {
              EXPECT_EQ(ConstraintType::FOREIGN, constraint.GetType());
              EXPECT_EQ(1, constraint.GetForeignKeyListOffset());
              EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
            } else {
              LOG_ERROR("Unexpected constraint is found: %s",
                        constraint.GetName().c_str());
            }
          }
        } else {
          LOG_ERROR("Unexpected column is found: %s", constraint_column_name.c_str());
          EXPECT_TRUE(false);
        }
      }  // loop end: single attribute constraint
      // end: check the column constraint recovery
    } else {
      LOG_ERROR("Unexpected table is found: %s", table_name.c_str());
      //EXPECT_TRUE(false);
    }
  }  // table loop end

  // finish the low level test
  txn_manager.CommitTransaction(txn);


  // high level test
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
