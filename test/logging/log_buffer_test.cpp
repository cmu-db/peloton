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

#include "logging/log_buffer.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Tests
//===--------------------------------------------------------------------===//

class LogBufferTests : public PelotonTest {};

TEST_F(LogBufferTests, LogBufferTest) {
  ItemPointer old_location(1, 8);
  ItemPointer location(1, 16);
  ItemPointer new_location(1, 24);
  eid_t epoch_id = 3;
  txn_id_t txn_id = 99;
  cid_t commit_id = 98;
  logging::LogBuffer log_buffer(3);

  logging::LogRecord insert_record = logging::LogRecordFactory::CreateTupleRecord(
          LogRecordType::TUPLE_INSERT, location, epoch_id, txn_id, commit_id);
  logging::LogRecord update_record = logging::LogRecordFactory::CreateTupleRecord(
          LogRecordType::TUPLE_UPDATE, location, new_location, epoch_id, txn_id,
          commit_id);
  logging::LogRecord delete_record = logging::LogRecordFactory::CreateTupleRecord(
          LogRecordType::TUPLE_DELETE, old_location, epoch_id, txn_id, commit_id);

  EXPECT_EQ(log_buffer.GetThreshold(), 3);
//  log_buffer.WriteRecord(insert_record);
//  std::cout << log_buffer.GetSize() << std::endl;
//  std::cout << log_buffer.GetData() << std::endl;
//
//  log_buffer.WriteRecord(update_record);
//  std::cout << log_buffer.GetSize() << std::endl;
//  std::cout << log_buffer.GetData() << std::endl;
//
//  log_buffer.WriteRecord(delete_record);
//  std::cout << log_buffer.GetSize() << std::endl;
//  std::cout << log_buffer.GetData() << std::endl;
}
}
}