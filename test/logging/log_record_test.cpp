//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record_test.cpp
//
// Identification: test/logging/log_record_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/log_record.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Log Buffer Tests
//===--------------------------------------------------------------------===//

class LogRecordTests : public PelotonTest {};

TEST_F(LogRecordTests, LogRecordTest) {
  
  std::vector<LogRecordType> tuple_type_list = {
    LogRecordType::TUPLE_INSERT,
    LogRecordType::TUPLE_DELETE,
    LogRecordType::TUPLE_UPDATE
  };

  for (auto type : tuple_type_list) {
    logging::LogRecord tuple_record = 
        logging::LogRecordFactory::CreateTupleRecord(type, ItemPointer(0, 0), 5, 1);

    EXPECT_EQ(tuple_record.GetType(), type);
  }
  
  std::vector<LogRecordType> txn_type_list = {
    LogRecordType::TRANSACTION_BEGIN,
    LogRecordType::TRANSACTION_COMMIT
  };

  for (auto type : txn_type_list) {
    logging::LogRecord txn_record = 
        logging::LogRecordFactory::CreateTxnRecord(type, 50);

    EXPECT_EQ(txn_record.GetType(), type);
  }

  std::vector<LogRecordType> epoch_type_list = {
    LogRecordType::EPOCH_BEGIN, 
    LogRecordType::EPOCH_END
  };

  for (auto type : epoch_type_list) {
    logging::LogRecord epoch_record = 
        logging::LogRecordFactory::CreateEpochRecord(type, 100);

    EXPECT_EQ(epoch_record.GetType(), type);
  }
}

}
}
