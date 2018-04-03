//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// new_checkpointing_test.cpp
//
// Identification: test/logging/new_checkpointing_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "common/init.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "logging/timestamp_checkpoint_manager.h"
#include "storage/storage_manager.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpointing Tests
//===--------------------------------------------------------------------===//

class TimestampCheckpointingTests : public PelotonTest {};

TEST_F(TimestampCheckpointingTests, CheckpointRecoveryTest) {
  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();
	settings::SettingsManager::SetBool(settings::SettingId::checkpointing, false);
	PelotonInit::Initialize();

  // do checkpoint recovery
  checkpoint_manager.DoCheckpointRecovery();

  // check schemas
  //LOG_DEBUG("%s", storage::StorageManager::GetInstance()->GetDatabaseWithOffset(1)->GetInfo().c_str());

  // check the data of 3 user tables as high level test
  std::string sql1 = "SELECT * FROM checkpoint_table_test";
  std::vector<std::string> expected1 = {"0|1.2|aaa", "1|12.34|bbbbbb", "2|12345.7|ccccccccc", "3|0|xxxx"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql1, expected1, false);

  std::string sql2 = "SELECT * FROM checkpoint_index_test";
  std::vector<std::string> expected2 = {"1|2|3|4", "5|6|7|8", "9|10|11|12"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql2, expected2, false);

  std::string sql3 = "SELECT * FROM checkpoint_constraint_test";
  std::vector<std::string> expected3 = {"1|2|3|4|1|3|4", "5|6|7|8|5|7|8", "9|10|11|12|9|11|12"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql3, expected3, false);

  // prepare for the low level check
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  auto storage = storage::StorageManager::GetInstance();

  // check the uncommitted table does not exist
  EXPECT_FALSE(catalog->ExistTableByName(DEFAULT_DB_NAME, "out_of_checkpoint", txn));

  auto default_db_catalog = catalog->GetDatabaseObject(DEFAULT_DB_NAME, txn);
  for (auto table_catalog_pair : default_db_catalog->GetTableObjects()) {
  	auto table_catalog = table_catalog_pair.second;
  	auto table = storage->GetTableWithOid(table_catalog->GetDatabaseOid(), table_catalog->GetTableOid());
		LOG_DEBUG("Check the table %s", table_catalog->GetTableName().c_str());

  	// check the basic information of columns
  	if(table_catalog->GetTableName() == "checkpoint_table_test") {
  		for (auto column_pair : table_catalog->GetColumnObjects()) {
  			auto column_catalog = column_pair.second;
  			auto column = table->GetSchema()->GetColumn(column_catalog->GetColumnId());
  			LOG_DEBUG("Check the column %s", column.GetInfo().c_str());

  			if (column_catalog->GetColumnName() == "id") {
  				EXPECT_EQ(type::TypeId::INTEGER, column_catalog->GetColumnType());
  				EXPECT_EQ(0, column_catalog->GetColumnOffset());
  				EXPECT_EQ(4, column.GetLength());
  				EXPECT_TRUE(column_catalog->IsInlined());
  				EXPECT_FALSE(column_catalog->IsNotNull());
  				EXPECT_FALSE(column_catalog->IsPrimary());
  			}
  			else if (column_catalog->GetColumnName() == "value1") {
  				EXPECT_EQ(type::TypeId::DECIMAL, column_catalog->GetColumnType());
  				EXPECT_EQ(4, column_catalog->GetColumnOffset());
  				EXPECT_EQ(8, column.GetLength());
  				EXPECT_TRUE(column_catalog->IsInlined());
  				EXPECT_FALSE(column_catalog->IsNotNull());
  				EXPECT_FALSE(column_catalog->IsPrimary());
  			}
  			else if (column_catalog->GetColumnName() == "value2") {
  				EXPECT_EQ(type::TypeId::VARCHAR, column_catalog->GetColumnType());
  				EXPECT_EQ(12, column_catalog->GetColumnOffset());
  				EXPECT_EQ(32, column.GetLength());
  				EXPECT_FALSE(column_catalog->IsInlined());
  				EXPECT_FALSE(column_catalog->IsNotNull());
  				EXPECT_FALSE(column_catalog->IsPrimary());
  			}
  			else {
  				LOG_ERROR("Unexpected column found: %s", column_catalog->GetColumnName().c_str());
  			}
  		}
  	}

  	// check the index recovery
  	else if (table_catalog->GetTableName() == "checkpoint_index_test") {
  		for (auto index_pair : table_catalog->GetIndexObjects()) {
  			auto index_catalog = index_pair.second;
  			LOG_DEBUG("Check the index %s", index_catalog->GetIndexName().c_str());
  			// unique primary key for attribute "pid" (primary key)
  			if (index_catalog->GetIndexName() == "checkpoint_index_test_pkey") {
  				EXPECT_EQ(IndexType::BWTREE, index_catalog->GetIndexType());
  				EXPECT_EQ(IndexConstraintType::PRIMARY_KEY, index_catalog->GetIndexConstraint());
  				EXPECT_TRUE(index_catalog->HasUniqueKeys());
  				auto key_attrs = index_catalog->GetKeyAttrs();
  				EXPECT_EQ(1, key_attrs.size());
  				EXPECT_EQ("pid", table_catalog->GetColumnObject(key_attrs[0])->GetColumnName());
  			}
  			// unique primary key for attribute "pid" (unique)
  			else if (index_catalog->GetIndexName() == "checkpoint_index_test_pid_UNIQ") {
  				EXPECT_EQ(IndexType::BWTREE, index_catalog->GetIndexType());
  				EXPECT_EQ(IndexConstraintType::UNIQUE, index_catalog->GetIndexConstraint());
  				EXPECT_TRUE(index_catalog->HasUniqueKeys());
  				auto key_attrs = index_catalog->GetKeyAttrs();
  				EXPECT_EQ(1, key_attrs.size());
  				EXPECT_EQ("pid", table_catalog->GetColumnObject(key_attrs[0])->GetColumnName());
  			}
  			// ART index for attribute "value1"
  			else if (index_catalog->GetIndexName() == "index_test1") {
  				EXPECT_EQ(IndexType::ART, index_catalog->GetIndexType());
  				EXPECT_EQ(IndexConstraintType::DEFAULT, index_catalog->GetIndexConstraint());
  				EXPECT_FALSE(index_catalog->HasUniqueKeys());
  				auto key_attrs = index_catalog->GetKeyAttrs();
  				EXPECT_EQ(1, key_attrs.size());
  				EXPECT_EQ("value1", table_catalog->GetColumnObject(key_attrs[0])->GetColumnName());
  			}
  			// SKIPLIST index for attributes "value2" and "value3"
  			else if (index_catalog->GetIndexName() == "index_test2") {
  				EXPECT_EQ(IndexType::SKIPLIST, index_catalog->GetIndexType());
  				EXPECT_EQ(IndexConstraintType::DEFAULT, index_catalog->GetIndexConstraint());
  				EXPECT_FALSE(index_catalog->HasUniqueKeys());
  				auto key_attrs = index_catalog->GetKeyAttrs();
  				EXPECT_EQ(2, key_attrs.size());
  				EXPECT_EQ("value2", table_catalog->GetColumnObject(key_attrs[0])->GetColumnName());
  				EXPECT_EQ("value3", table_catalog->GetColumnObject(key_attrs[1])->GetColumnName());
  			}
  			// unique index for attribute "value2"
  			else if (index_catalog->GetIndexName() == "unique_index_test") {
  				EXPECT_EQ(IndexType::BWTREE, index_catalog->GetIndexType());
  				EXPECT_EQ(IndexConstraintType::UNIQUE, index_catalog->GetIndexConstraint());
  				EXPECT_TRUE(index_catalog->HasUniqueKeys());
  				auto key_attrs = index_catalog->GetKeyAttrs();
  				EXPECT_EQ(1, key_attrs.size());
  				EXPECT_EQ("value2", table_catalog->GetColumnObject(key_attrs[0])->GetColumnName());
  			}
  			else {
  				LOG_ERROR("Unexpected index found: %s", index_catalog->GetIndexName().c_str());
  			}
  		}
  	}

  	// check the column constraint recovery
  	else if (table_catalog->GetTableName() == "checkpoint_constraint_test") {
  		// multiple attributes constraint
			for (auto multi_constraint : table->GetSchema()->GetMultiConstraints()) {
				// currently nothing
				LOG_DEBUG("multi constraint: %s", multi_constraint.GetInfo().c_str());
			}

			// single attribute constraint
  		for (auto column_pair : table_catalog->GetColumnObjects()) {
  			auto column_catalog = column_pair.second;
  			auto column = table->GetSchema()->GetColumn(column_catalog->GetColumnId());
  			LOG_DEBUG("Check constraints of the column %s", column.GetInfo().c_str());

  			// set primary key of attributes 'pid1' and 'pid2'
  			if (column_catalog->GetColumnName() == "pid1" || column_catalog->GetColumnName() == "pid2") {
  				EXPECT_FALSE(column_catalog->IsNotNull());
  				EXPECT_TRUE(column_catalog->IsPrimary());
  				for(auto constraint : column.GetConstraints()) {
  					if(constraint.GetName() == "con_primary") {
  						EXPECT_EQ(ConstraintType::PRIMARY, constraint.GetType());
  						EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
  						EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
  					} else {
  						LOG_ERROR("Unexpected constraint found: %s", constraint.GetName().c_str());
  					}
  				}
  			}
  			// not null and default value in attribute 'value1'
  			else if (column_catalog->GetColumnName() == "value1") {
  				EXPECT_TRUE(column_catalog->IsNotNull());
  				EXPECT_FALSE(column_catalog->IsPrimary());
  				for(auto constraint : column.GetConstraints()) {
  					if(constraint.GetName() == "con_default") {
  						EXPECT_EQ(ConstraintType::DEFAULT, constraint.GetType());
  						EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
  						EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
  						EXPECT_EQ(0, constraint.getDefaultValue()->GetAs<int>());
  					} else if(constraint.GetName() == "con_not_null") {
  						EXPECT_EQ(ConstraintType::NOTNULL, constraint.GetType());
  						EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
  						EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
  					} else {
  						LOG_ERROR("Unexpected constraint found: %s", constraint.GetName().c_str());
  					}
  				}
  			}
  			// check constraint in attribute 'value2'
  			else if (column_catalog->GetColumnName() == "value2") {
  				EXPECT_FALSE(column_catalog->IsNotNull());
  				EXPECT_FALSE(column_catalog->IsPrimary());
  				for(auto constraint : column.GetConstraints()) {
  					if(constraint.GetName() == "con_check") {
  						EXPECT_EQ(ConstraintType::CHECK, constraint.GetType());
  						EXPECT_EQ(INVALID_OID, constraint.GetForeignKeyListOffset());
  						EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
  						EXPECT_EQ(ExpressionType::COMPARE_GREATERTHAN, constraint.GetCheckExpression().first);
  						EXPECT_EQ(2, constraint.GetCheckExpression().second.GetAs<int>());
  					} else {
  						LOG_ERROR("Unexpected constraint found: %s", constraint.GetName().c_str());
  					}
  				}
  			}
  			// foreign key in attribute 'value3' to attribute 'value1' in table 'checkpoint_index_test'
  			else if (column_catalog->GetColumnName() == "value3") {
  				EXPECT_FALSE(column_catalog->IsNotNull());
  				EXPECT_FALSE(column_catalog->IsPrimary());
  				for(auto constraint : column.GetConstraints()) {
  					if(constraint.GetName() == "FK_checkpoint_constraint_test->checkpoint_index_test") {
  						EXPECT_EQ(ConstraintType::FOREIGN, constraint.GetType());
  						EXPECT_EQ(0, constraint.GetForeignKeyListOffset());
  						EXPECT_EQ(INVALID_OID, constraint.GetUniqueIndexOffset());
  					} else {
  						LOG_ERROR("Unexpected constraint found: %s", constraint.GetName().c_str());
  					}
  				}
  			}
    		else if (column_catalog->GetColumnName() == "value4") {
    			// currently no constraint
    		}
    		else if (column_catalog->GetColumnName() == "value5") {
    			// currently no constraint
    		}
  			else {
  				LOG_ERROR("Unexpected column found: %s", column_catalog->GetColumnName().c_str());
  			}
  		}

  	}
  	else {
  		LOG_ERROR("Unexpected table found: %s", table_catalog->GetTableName().c_str());
  	}
  }

  // check the catalog data
  auto catalog_db_catalog = catalog->GetDatabaseObject(CATALOG_DATABASE_OID, txn);
  for (auto table_catalog_pair : catalog_db_catalog->GetTableObjects()) {
  	auto table_catalog = table_catalog_pair.second;
  	// auto table = storage->GetTableWithOid(table_catalog->GetDatabaseOid(), table_catalog->GetTableOid());
		LOG_DEBUG("Check catalog table %s", table_catalog->GetTableName().c_str());
		// currently do nothing
  }

  // finish the low level check
  txn_manager.CommitTransaction(txn);


  /*
  auto sm = storage::StorageManager::GetInstance();
  auto db = sm->GetDatabaseWithOffset(1);
  for (oid_t t = 0; t < db->GetTableCount(); t++) {
  	auto table = db->GetTable(t);
  	for (oid_t tg = 0; tg < table->GetTileGroupCount(); tg++) {
  		LOG_DEBUG("%s", table->GetTileGroup(tg)->GetInfo().c_str());
  	}
  }
  */

  PelotonInit::Shutdown();
}


}
}
