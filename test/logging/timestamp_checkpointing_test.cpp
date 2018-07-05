//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_checkpointing_test.cpp
//
// Identification: test/logging/timestamp_checkpointing_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/layout_catalog.h"
#include "catalog/system_catalogs.h"
#include "common/init.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "logging/timestamp_checkpoint_manager.h"
#include "logging/logging_util.h"
#include "storage/storage_manager.h"
#include "storage/tile_group_factory.h"
#include "sql/testing_sql_util.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpointing Tests
//===--------------------------------------------------------------------===//

class TimestampCheckpointingTests : public PelotonTest {};

bool RecoverTileGroupFromFile(
    std::vector<std::shared_ptr<storage::TileGroup>> &tile_groups,
    storage::DataTable *table, FileHandle table_file, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  size_t table_size = logging::LoggingUtil::GetFileSize(table_file);
  if (table_size == 0) return false;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (!logging::LoggingUtil::ReadNBytesFromFile(table_file, data.get(), table_size)) {
    LOG_ERROR("Checkpoint table file read error: table %d", table->GetOid());
    return false;
  }
  CopySerializeInput input_buffer(data.get(), table_size);

  auto schema = table->GetSchema();
  auto default_layout = table->GetDefaultLayout();
  auto column_count = schema->GetColumnCount();
  oid_t tile_group_count = input_buffer.ReadLong();
  for (oid_t tg_idx = START_OID; tg_idx < tile_group_count; tg_idx++) {
    // recover layout for this tile group
    oid_t layout_oid = input_buffer.ReadInt();
    std::shared_ptr<const storage::Layout> layout;
    if (default_layout->GetOid() != layout_oid) {
      layout = catalog::Catalog::GetInstance()
            ->GetTableCatalogEntry(txn, table->GetDatabaseOid(), table->GetOid())
            ->GetLayout(layout_oid);
    } else {
      layout = default_layout;
    }
    // recover tile group
    oid_t tile_group_id =
        storage::StorageManager::GetInstance()->GetNextTileGroupId();
    oid_t allocated_tuple_count = input_buffer.ReadInt();
    auto layout_schemas = layout->GetLayoutSchemas(schema);
    std::shared_ptr<storage::TileGroup> tile_group(
        storage::TileGroupFactory::GetTileGroup(
                table->GetDatabaseOid(), table->GetOid(), tile_group_id, table,
                layout_schemas, layout, allocated_tuple_count));

    LOG_TRACE("Deserialized tile group %u in %s \n%s", tile_group->GetTileGroupId(),
        table->GetName().c_str(), tile_group->GetLayout().GetInfo().c_str());

    // recover tuples located in the tile group
    while (input_buffer.ReadBool()) {
      // recover values on each column
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
      for (oid_t column_id = 0; column_id < column_count; column_id++) {
        auto value = type::Value::DeserializeFrom(
            input_buffer, schema->GetType(column_id), pool);
        tuple->SetValue(column_id, value, pool);
      }

      // insert the tuple into the tile group
      oid_t tuple_slot = tile_group->InsertTuple(tuple.get());
      EXPECT_NE(tuple_slot, INVALID_OID);
      if (tuple_slot == INVALID_OID) {
        LOG_ERROR("Tuple insert error for tile group %d of table %d",
                  tile_group->GetTileGroupId(), table->GetOid());
        return false;
      }
    }

    tile_groups.push_back(tile_group);
  }

  return true;
}


TEST_F(TimestampCheckpointingTests, CheckpointingTest) {
  PelotonInit::Initialize();

  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();

  // checkpoint_manager.SetCheckpointBaseDirectory("/var/tmp/peloton/checkpoints")

  // generate table and data taken into storage.
  // basic table test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE checkpoint_table_test (id INTEGER PRIMARY KEY, value1 "
      "REAL, value2 VARCHAR(32));");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (0, 1.2, 'aaa');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (1, 12.34, 'bbbbbb');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (2, 12345.678912345, "
      "'ccccccccc');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // primary key and index test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE checkpoint_index_test ("
      "upid1 INTEGER UNIQUE PRIMARY KEY, "
      "upid2 INTEGER PRIMARY KEY, "
      "value1 INTEGER, value2 INTEGER, value3 INTEGER);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE INDEX index_test1 ON checkpoint_index_test USING art (value1);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE INDEX index_test2 ON checkpoint_index_test USING skiplist "
      "(value2, value3);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE UNIQUE INDEX unique_index_test ON checkpoint_index_test "
      "(value2);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_index_test VALUES (1, 2, 3, 4, 5);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_index_test VALUES (6, 7, 8, 9, 10);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_index_test VALUES (11, 12, 13, 14, 15);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // column constraint test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  std::string constraint_test_sql =
      "CREATE TABLE checkpoint_constraint_test ("
      "pid1 INTEGER, pid2 INTEGER, "
      "value1 INTEGER DEFAULT 0 UNIQUE, "
      "value2 INTEGER NOT NULL CHECK (value2 > 2), "  // check doesn't work
                                                      // correctly
      "value3 INTEGER REFERENCES checkpoint_table_test (id), "
      "value4 INTEGER, value5 INTEGER, "
      "FOREIGN KEY (value4, value5) REFERENCES checkpoint_index_test (upid1, "
      "upid2), "
      // not supported yet "UNIQUE (value4, value5), "
      "PRIMARY KEY (pid1, pid2));";
  TestingSQLUtil::ExecuteSQLQuery(constraint_test_sql);
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_constraint_test VALUES (1, 2, 3, 4, 0, 1, 2);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_constraint_test VALUES (5, 6, 7, 8, 1, 6, 7);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_constraint_test VALUES (9, 10, 11, 12, 2, 11, "
      "12);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // insert test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (3, 0.0, 'xxxx');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // generate table and data that will be out of checkpointing.
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (4, -1.0, 'out of the "
      "checkpoint');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (5, -2.0, 'out of the "
      "checkpoint');");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE out_of_checkpoint_test (pid INTEGER PRIMARY KEY);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO out_of_checkpoint_test VALUES (1);");
  // TestingSQLUtil::ExecuteSQLQuery("CREATE DATABASE out_of_checkpoint;");

  // do checkpointing
  checkpoint_manager.StartCheckpointing();

  EXPECT_TRUE(checkpoint_manager.GetStatus());

  std::this_thread::sleep_for(std::chrono::seconds(3));
  checkpoint_manager.StopCheckpointing();

  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  EXPECT_FALSE(checkpoint_manager.GetStatus());

  // test files created by this checkpointing
  // prepare file check
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  auto storage = storage::StorageManager::GetInstance();
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());

  // check the created directory of the checkpoint
  eid_t checkpointed_epoch = checkpoint_manager.GetRecoveryCheckpointEpoch();
  std::string checkpoint_dir = "./data/checkpoints/" + std::to_string(checkpointed_epoch);
  EXPECT_TRUE(logging::LoggingUtil::CheckDirectoryExistence(checkpoint_dir.c_str()));

  // check user table file
  auto default_db_catalog_entry = catalog->GetDatabaseCatalogEntry(txn, DEFAULT_DB_NAME);
  for (auto table_catalog_entry :
      default_db_catalog_entry->GetTableCatalogEntries((std::string)DEFAULT_SCHEMA_NAME)) {
    auto table = storage->GetTableWithOid(table_catalog_entry->GetDatabaseOid(),
                                          table_catalog_entry->GetTableOid());
    FileHandle table_file;
    std::string file = checkpoint_dir + "/" + "checkpoint_" +
        default_db_catalog_entry->GetDatabaseName() + "_" +
        table_catalog_entry->GetSchemaName() + "_" + table_catalog_entry->GetTableName();

    LOG_INFO("Check the user table %s.%s\n%s", table_catalog_entry->GetSchemaName().c_str(),
        table_catalog_entry->GetTableName().c_str(), table->GetInfo().c_str());

    // open table file
    // table 'out_of_checkpoint_test' is not targeted for the checkpoint
    if (table_catalog_entry->GetTableName() == "out_of_checkpoint_test") {
      EXPECT_FALSE(logging::LoggingUtil::OpenFile(file.c_str(), "rb", table_file));
      continue;
    } else {
      bool open_file_result = logging::LoggingUtil::OpenFile(file.c_str(), "rb", table_file);
      EXPECT_TRUE(open_file_result);
      if(open_file_result == false) {
        LOG_ERROR("Unexpected table is found: %s", table_catalog_entry->GetTableName().c_str());
        continue;
      }
    }

    // read data (tile groups and records)
    std::vector<std::shared_ptr<storage::TileGroup>> tile_groups;
    RecoverTileGroupFromFile(tile_groups, table, table_file, pool.get(), txn);

    logging::LoggingUtil::CloseFile(table_file);

    // check the tile group
    auto schema = table->GetSchema();
    auto column_count = schema->GetColumnCount();
    for (auto tile_group : tile_groups) {
      // check the layout of columns in the tile group
      EXPECT_EQ(column_count, tile_group->GetLayout().GetColumnCount());

      // check the records
      oid_t max_tuple_count = tile_group->GetNextTupleSlot();
      for (oid_t tuple_id = START_OID; tuple_id < max_tuple_count; tuple_id++) {
        for (oid_t column_id = START_OID; column_id < column_count; column_id++) {
          type::Value value = tile_group->GetValue(tuple_id, column_id);
          // for checkpoint_table_test
          if (table_catalog_entry->GetTableName() == "checkpoint_table_test") {
            switch (column_id) {
            case 0:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(0)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(1)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(2)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(3)) == CmpBool::CmpTrue);
              break;
            case 1:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetDecimalValue(1.2)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetDecimalValue(12.34)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetDecimalValue(12345.678912345)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetDecimalValue(0.0)) == CmpBool::CmpTrue);
              break;
            case 2:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetVarcharValue(std::string("aaa"), pool.get())) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetVarcharValue(std::string("bbbbbb"), pool.get())) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetVarcharValue(std::string("ccccccccc"), pool.get())) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetVarcharValue(std::string("xxxx"), pool.get())) == CmpBool::CmpTrue);
              break;
            default:
              LOG_ERROR("Unexpected column is found in %s: %d", table_catalog_entry->GetTableName().c_str(), column_id);
              EXPECT_TRUE(false);
            }
          }
          // for checkpoint_index_test
          else if (table_catalog_entry->GetTableName() == "checkpoint_index_test") {
            switch (column_id) {
            case 0:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(1)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(6)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(11)) == CmpBool::CmpTrue);
              break;
            case 1:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(2)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(7)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(12)) == CmpBool::CmpTrue);
              break;
            case 2:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(3)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(8)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(13)) == CmpBool::CmpTrue);
              break;
            case 3:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(4)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(9)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(14)) == CmpBool::CmpTrue);
              break;
            case 4:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(5)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(10)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(15)) == CmpBool::CmpTrue);
              break;
            default:
              LOG_ERROR("Unexpected column is found in %s: %d", table_catalog_entry->GetTableName().c_str(), column_id);
              EXPECT_TRUE(false);
            }
          }
          // for checkpoint_index_test
          else if (table_catalog_entry->GetTableName() == "checkpoint_constraint_test") {
            switch (column_id) {
            case 0:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(1)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(5)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(9)) == CmpBool::CmpTrue);
              break;
            case 1:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(2)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(6)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(10)) == CmpBool::CmpTrue);
              break;
            case 2:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(3)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(7)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(11)) == CmpBool::CmpTrue);
              break;
            case 3:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(4)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(8)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(12)) == CmpBool::CmpTrue);
              break;
            case 4:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(0)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(1)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(2)) == CmpBool::CmpTrue);
              break;
            case 5:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(1)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(6)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(11)) == CmpBool::CmpTrue);
              break;
            case 6:
              EXPECT_TRUE(value.CompareEquals(type::ValueFactory::GetIntegerValue(2)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(7)) == CmpBool::CmpTrue ||
                  value.CompareEquals(type::ValueFactory::GetIntegerValue(12)) == CmpBool::CmpTrue);
              break;
            default:
              LOG_ERROR("Unexpected column is found in %s: %d", table_catalog_entry->GetTableName().c_str(), column_id);
              EXPECT_TRUE(false);
            }
          }
          else {
            LOG_ERROR("Unexpected table is found: %s", table_catalog_entry->GetTableName().c_str());
            PELOTON_ASSERT(false);
          }
        }
      }
    }
  }

  txn_manager.CommitTransaction(txn);

  PelotonInit::Shutdown();
}
}
}
