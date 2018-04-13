//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer_test.cpp
//
// Identification: test/logging/log_buffer_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/tile_group_factory.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "logging/log_buffer.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Tests
//===--------------------------------------------------------------------===//

class LogBufferTests : public PelotonTest {};

TEST_F(LogBufferTests, LogBufferTest) {
  oid_t block = 1;
  ItemPointer old_location(block, 8);
  ItemPointer location(block, 16);
  ItemPointer new_location(block, 24);
  eid_t epoch_id = 3;
  txn_id_t txn_id = 99;
  cid_t commit_id = 98;
  oid_t database_id = 10;
  oid_t table_id = 20;
  oid_t tile_group_id = 30;

  logging::LogBuffer *log_buffer;
  log_buffer = new logging::LogBuffer(logging::LogManager::GetInstance().GetTransactionBufferSize());

  // TILES
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string>> column_names;

  tile_column_names.push_back("INTEGER COL");
  column_names.push_back(tile_column_names);

  std::vector<catalog::Schema> schemas;
  std::vector<catalog::Column> columns;

  // SCHEMA
  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  columns.push_back(column1);

  std::unique_ptr<catalog::Schema> schema1(new catalog::Schema(columns));
  schemas.push_back(*schema1);

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map;
  column_map[0] = std::make_pair(0, 0);

  std::shared_ptr<storage::TileGroup> tile_group(storage::TileGroupFactory::GetTileGroup(
          database_id, table_id, tile_group_id, nullptr, schemas, column_map, 3));
  catalog::Manager::GetInstance().AddTileGroup(block, tile_group);

  logging::LogRecord insert_record = logging::LogRecordFactory::CreateTupleRecord(
          LogRecordType::TUPLE_INSERT, location, epoch_id, txn_id, commit_id);
  logging::LogRecord update_record = logging::LogRecordFactory::CreateTupleRecord(
          LogRecordType::TUPLE_UPDATE, location, new_location, epoch_id, txn_id,
          commit_id);
  logging::LogRecord delete_record = logging::LogRecordFactory::CreateTupleRecord(
          LogRecordType::TUPLE_DELETE, old_location, epoch_id, txn_id, commit_id);

  EXPECT_EQ(log_buffer->GetThreshold(), logging::LogManager::GetInstance().GetTransactionBufferSize());
  log_buffer->WriteRecord(insert_record);

//  std::cout << log_buffer->GetSize() << std::endl;
//  std::cout << log_buffer->GetData() << std::endl;

//  log_buffer->WriteRecord(update_record);
//  std::cout << log_buffer->GetSize() << std::endl;
//  std::cout << log_buffer->GetData() << std::endl;

//  log_buffer->WriteRecord(delete_record);
//  std::cout << log_buffer->GetSize() << std::endl;
//  std::cout << log_buffer->GetData() << std::endl;
}
}
}