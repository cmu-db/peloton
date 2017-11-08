//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// new_logging_test.cpp
//
// Identification: test/logging/wal_logger_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "logging/wal_logger.h"
#include "catalog/catalog.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class WalLoggerTests : public PelotonTest {};

TEST_F(WalLoggerTests, LogWrittenTest) {
  logging::WalLogger* logger = new logging::WalLogger(1, "/tmp");
  catalog::Catalog::GetInstance();

  std::vector<logging::LogRecord> rs;
  logging::LogRecord r = logging::LogRecordFactory::CreateTupleRecord(
      LogRecordType::TUPLE_INSERT, ItemPointer(2, 5), 1, 1);
  rs.push_back(r);
  logger->WriteTransaction(rs);
  delete logger;
  EXPECT_TRUE(FileUtil::Exists("/tmp/log_1_0"));
}
}
}
