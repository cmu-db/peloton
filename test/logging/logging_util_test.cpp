// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // logging_util_test.cpp
// //
// // Identification: test/logging/logging_util_test.cpp
// //
// // Copyright (c) 2015-17, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "logging/logging_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//
class LoggingUtilTests : public PelotonTest {};

TEST_F(LoggingUtilTests, LoggingUtilCreateDirectoryTest) {
  auto status = logging::LoggingUtil::CreateDirectory("test_dir", 0700);
  EXPECT_EQ(status, true);
}
TEST_F(LoggingUtilTests, LoggingUtilCheckDirectoryTest) {
  auto status = logging::LoggingUtil::CheckDirectoryExistence("test_dir");
  EXPECT_EQ(status, true);
}
TEST_F(LoggingUtilTests, LoggingUtilRemoveDirectoryTest) {
  auto status = logging::LoggingUtil::RemoveDirectory("test_dir", true);
  EXPECT_EQ(status, true);
}
TEST_F(LoggingUtilTests, LoggingUtilManipulateFileTest) {
  FileHandle f;
  char buf[4];
  auto status = logging::LoggingUtil::OpenFile("test", "wb+", f);
  EXPECT_EQ(status, true);
  fwrite((const void *)"abc", 3, 1, f.file);
  logging::LoggingUtil::FFlushFsync(f);
  EXPECT_EQ(logging::LoggingUtil::GetFileSize(f), 3);
  status = logging::LoggingUtil::CloseFile(f);
  EXPECT_EQ(status, true);
  FileHandle f2;
  status = logging::LoggingUtil::OpenFile("test", "rb", f2);
  EXPECT_EQ(status, true);
  status = logging::LoggingUtil::ReadNBytesFromFile(f2, buf, 3);
  EXPECT_EQ(status, true);
  buf[(sizeof buf) - 1] = 0;
  std::string str(buf);
  EXPECT_EQ(str, "abc");
  status = logging::LoggingUtil::CloseFile(f2);
  EXPECT_EQ(status, true);
}

}  // namespace test
}  // namespace peloton
