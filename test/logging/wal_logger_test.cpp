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
#include "logging/logging_util.h"

namespace peloton {
namespace logging {
class LoggingUtil;

}
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class WalLoggerTests : public PelotonTest {};

TEST_F(WalLoggerTests, LogInsertTest) {
  logging::WalLogger* logger = new logging::WalLogger(1, "/tmp");
  catalog::Catalog::GetInstance();
  std::vector<logging::LogRecord> rs;
  logging::LogRecord r = logging::LogRecordFactory::CreateTupleRecord(
      LogRecordType::TUPLE_INSERT, ItemPointer(2, 5), 1, 3);
  rs.push_back(r);
  logger->WriteTransaction(rs);
  delete logger;
  EXPECT_TRUE(FileUtil::Exists("/tmp/logfile_1"));
  FileHandle f;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];
  logging::LoggingUtil::OpenFile("/tmp/logfile_1", "rb", f);
  logging::LoggingUtil::ReadNBytesFromFile(f,(void *)length_buf, 4);
  CopySerializeInput length_decode((const void *)&length_buf, 4);
  int length = length_decode.ReadInt();
  logging::LoggingUtil::ReadNBytesFromFile(f, (void *)buffer.get(),
                                          length);
  CopySerializeInput record_decode((const void *)buffer.get(), length);
    LogRecordType log_record_type = (LogRecordType)record_decode.ReadEnumInSingleByte();
  EXPECT_EQ(LogRecordType::TUPLE_INSERT, log_record_type);
  EXPECT_EQ(3, record_decode.ReadLong());
  EXPECT_EQ(1, record_decode.ReadLong());
  EXPECT_EQ(CATALOG_DATABASE_OID, record_decode.ReadLong());
  EXPECT_EQ(COLUMN_CATALOG_OID, record_decode.ReadLong());
  EXPECT_EQ(2, record_decode.ReadLong());
  EXPECT_EQ(5, record_decode.ReadLong());


}

TEST_F(WalLoggerTests, LogDeleteTest) {
  logging::WalLogger* logger = new logging::WalLogger(2, "/tmp");
  catalog::Catalog::GetInstance();
  std::vector<logging::LogRecord> rs;
  logging::LogRecord r = logging::LogRecordFactory::CreateTupleRecord(
      LogRecordType::TUPLE_DELETE, ItemPointer(2, 5), 1, 3);
  rs.push_back(r);
  logger->WriteTransaction(rs);
  delete logger;
  EXPECT_TRUE(FileUtil::Exists("/tmp/logfile_2"));
  FileHandle f;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];
  logging::LoggingUtil::OpenFile("/tmp/logfile_2", "rb", f);
  logging::LoggingUtil::ReadNBytesFromFile(f,(void *)length_buf, 4);
  CopySerializeInput length_decode((const void *)&length_buf, 4);
  int length = length_decode.ReadInt();
  logging::LoggingUtil::ReadNBytesFromFile(f, (void *)buffer.get(),
                                          length);
  CopySerializeInput record_decode((const void *)buffer.get(), length);
    LogRecordType log_record_type = (LogRecordType)record_decode.ReadEnumInSingleByte();
  EXPECT_EQ(LogRecordType::TUPLE_DELETE, log_record_type);
  EXPECT_EQ(3, record_decode.ReadLong());
  EXPECT_EQ(1, record_decode.ReadLong());
  EXPECT_EQ(CATALOG_DATABASE_OID, record_decode.ReadLong());
  EXPECT_EQ(COLUMN_CATALOG_OID, record_decode.ReadLong());
  EXPECT_EQ(2, record_decode.ReadLong());
  EXPECT_EQ(5, record_decode.ReadLong());


}

TEST_F(WalLoggerTests, LogUpdateTest) {
  logging::WalLogger* logger = new logging::WalLogger(3, "/tmp");
  catalog::Catalog::GetInstance();
  std::vector<logging::LogRecord> rs;
  logging::LogRecord r = logging::LogRecordFactory::CreateTupleRecord(
      LogRecordType::TUPLE_UPDATE, ItemPointer(2, 5), 1, 3);
  r.SetOldItemPointer(ItemPointer(2,4));
  rs.push_back(r);
  logger->WriteTransaction(rs);
  delete logger;
  EXPECT_TRUE(FileUtil::Exists("/tmp/logfile_3"));
  FileHandle f;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];
  logging::LoggingUtil::OpenFile("/tmp/logfile_3", "rb", f);
  logging::LoggingUtil::ReadNBytesFromFile(f,(void *)length_buf, 4);
  CopySerializeInput length_decode((const void *)&length_buf, 4);
  int length = length_decode.ReadInt();
  logging::LoggingUtil::ReadNBytesFromFile(f, (void *)buffer.get(),
                                          length);
  CopySerializeInput record_decode((const void *)buffer.get(), length);
    LogRecordType log_record_type = (LogRecordType)record_decode.ReadEnumInSingleByte();
  EXPECT_EQ(LogRecordType::TUPLE_UPDATE, log_record_type);
  EXPECT_EQ(3, record_decode.ReadLong());
  EXPECT_EQ(1, record_decode.ReadLong());
  EXPECT_EQ(CATALOG_DATABASE_OID, record_decode.ReadLong());
  EXPECT_EQ(COLUMN_CATALOG_OID, record_decode.ReadLong());
  EXPECT_EQ(2, record_decode.ReadLong());
  EXPECT_EQ(4, record_decode.ReadLong());
  EXPECT_EQ(2, record_decode.ReadLong());
  EXPECT_EQ(5, record_decode.ReadLong());


}
}
}
