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
  oid_t block = 1, offset = 16, old_block = 1, old_offset = 8, new_block = 1,
        new_offset = 24;
  ItemPointer old_location(old_block, old_offset);
  ItemPointer location(block, offset);
  ItemPointer new_location(new_block, new_offset);
  eid_t epoch_id = 3;
  txn_id_t txn_id = 99;
  cid_t commit_id = 98;
  oid_t database_id = 10;
  oid_t table_id = 20;
  oid_t tile_group_id = 30;
  double value1 = 9.1, value2 = 9.2, value3 = 9.3;
  oid_t target_oid1 = 2, target_oid2 = 4, target_oid3 = 5;

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

  type::Value val1 = type::ValueFactory::GetDecimalValue(value1);
  type::Value val2 = type::ValueFactory::GetDecimalValue(value2);
  type::Value val3 = type::ValueFactory::GetDecimalValue(value3);
  std::vector<type::Value> values;
  values.push_back(val1);
  values.push_back(val2);
  values.push_back(val3);
  insert_record.SetValuesArray(reinterpret_cast<char *>(values.data()), static_cast<uint32_t>(values.size()));
  log_buffer->WriteRecord(insert_record);
  size_t buffer_size1 = sizeof(int32_t) + sizeof(int8_t) + sizeof(int64_t) * 3 + sizeof(int64_t) * 4 +
                       sizeof(double) * 3;
  EXPECT_EQ(buffer_size1, log_buffer->GetSize());

  expression::AbstractExpression *expr = nullptr;
  planner::DerivedAttribute tmp_att(expr);
  Target target1 = std::make_pair(target_oid1, tmp_att);
  Target target2 = std::make_pair(target_oid2, tmp_att);
  Target target3 = std::make_pair(target_oid3, tmp_att);
  TargetList *target_list = new TargetList;
  target_list->push_back(target1);
  target_list->push_back(target2);
  target_list->push_back(target3);
  update_record.SetOffsetsArray(target_list);
  update_record.SetValuesArray(reinterpret_cast<char *>(values.data()), static_cast<uint32_t>(values.size()));
  log_buffer->WriteRecord(update_record);
  size_t buffer_size2 = sizeof(int32_t) + sizeof(int8_t) + sizeof(int64_t) * 3 + sizeof(int64_t) * 7 +
                         (sizeof(int32_t) + sizeof(double)) * 3;
  EXPECT_EQ(buffer_size2, log_buffer->GetSize() - buffer_size1);

  ReferenceSerializeInput log_buffer_input(log_buffer->GetData(), log_buffer->GetSize());
  // Insert record
  EXPECT_EQ(buffer_size1 - sizeof(int32_t), log_buffer_input.ReadInt());
  EXPECT_EQ(static_cast<int>(LogRecordType::TUPLE_INSERT), log_buffer_input.ReadEnumInSingleByte());
  EXPECT_EQ(epoch_id, log_buffer_input.ReadLong());
  EXPECT_EQ(txn_id, log_buffer_input.ReadLong());
  EXPECT_EQ(commit_id, log_buffer_input.ReadLong());
  EXPECT_EQ(database_id, log_buffer_input.ReadLong());
  EXPECT_EQ(table_id, log_buffer_input.ReadLong());
  EXPECT_EQ(block, log_buffer_input.ReadLong());
  EXPECT_EQ(offset, log_buffer_input.ReadLong());
  EXPECT_EQ(value1, log_buffer_input.ReadDouble());
  EXPECT_EQ(value2, log_buffer_input.ReadDouble());
  EXPECT_EQ(value3, log_buffer_input.ReadDouble());

  // Update Record
  EXPECT_EQ(buffer_size2 - sizeof(int32_t), log_buffer_input.ReadInt());
  EXPECT_EQ(static_cast<int>(LogRecordType::TUPLE_UPDATE), log_buffer_input.ReadEnumInSingleByte());
  EXPECT_EQ(epoch_id, log_buffer_input.ReadLong());
  EXPECT_EQ(txn_id, log_buffer_input.ReadLong());
  EXPECT_EQ(commit_id, log_buffer_input.ReadLong());
  EXPECT_EQ(database_id, log_buffer_input.ReadLong());
  EXPECT_EQ(table_id, log_buffer_input.ReadLong());
  EXPECT_EQ(block, log_buffer_input.ReadLong());
  EXPECT_EQ(offset, log_buffer_input.ReadLong());
  EXPECT_EQ(new_block, log_buffer_input.ReadLong());
  EXPECT_EQ(new_offset, log_buffer_input.ReadLong());
  EXPECT_EQ(update_record.GetNumValues(), log_buffer_input.ReadLong());
  EXPECT_EQ(target_oid1, log_buffer_input.ReadInt());
  EXPECT_EQ(value1, log_buffer_input.ReadDouble());
  EXPECT_EQ(target_oid2, log_buffer_input.ReadInt());
  EXPECT_EQ(value2, log_buffer_input.ReadDouble());
  EXPECT_EQ(target_oid3, log_buffer_input.ReadInt());
  EXPECT_EQ(value3, log_buffer_input.ReadDouble());

//  log_buffer->WriteRecord(update_record);
//  std::cout << log_buffer->GetSize() << std::endl;
//  std::cout << log_buffer->GetData() << std::endl;

//  log_buffer->WriteRecord(delete_record);
//  std::cout << log_buffer->GetSize() << std::endl;
//  std::cout << log_buffer->GetData() << std::endl;
}
}
}