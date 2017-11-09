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
  std::vector<LogRecordType> tuple_type_list = {LogRecordType::TUPLE_INSERT,
                                                LogRecordType::TUPLE_DELETE,
                                                LogRecordType::TUPLE_UPDATE};

  for (auto type : tuple_type_list) {
    logging::LogRecord tuple_record =
        logging::LogRecordFactory::CreateTupleRecord(type, ItemPointer(0, 0), 5,
                                                     1);

    EXPECT_EQ(tuple_record.GetType(), type);
  }

}
}
}
