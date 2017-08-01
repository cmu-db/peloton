//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value_test.cpp
//
// Identification: test/common/boolean_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "common/logger.h"

#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "util/file_util.h"
#include "util/string_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// FileUtil Test
//===--------------------------------------------------------------------===//

class FileUtilTests : public PelotonTest {
 public:
  void TearDown() override {
    for (auto path : tempFiles) {
      if (FileUtil::Exists(path)) {
        LOG_TRACE("Deleting temp file '%s'", path.c_str());
        std::remove(path.c_str());
      }
    }  // FOR
    PelotonTest::TearDown();
  }

  std::vector<std::string> tempFiles;
};

TEST_F(FileUtilTests, WriteTempTest) {
  std::string contents =
      "All along it was the Geto, nothing but the Geto"
      "Taking short steps one foot at a time and keep my head low\n";
  std::string prefix = "peloton-";
  std::string suffix = "tmpfile";

  std::string path = FileUtil::WriteTempFile(contents, prefix, suffix);
  tempFiles.push_back(path);
  LOG_TRACE("Temp: %s", path.c_str());
  EXPECT_FALSE(path.empty());
  EXPECT_TRUE(FileUtil::Exists(path));
  EXPECT_TRUE(StringUtil::Contains(path, prefix));
  EXPECT_TRUE(StringUtil::EndsWith(path, suffix));

  // Read the file back in and make sure the contains match
  std::string result = FileUtil::GetFile(path);
  EXPECT_EQ(contents, result);
}

TEST_F(FileUtilTests, ExistsTest) {
  // This probably won't work on non-Unix systems...
  EXPECT_TRUE(FileUtil::Exists("/home"));
  EXPECT_FALSE(FileUtil::Exists("/thereisnowaythatyoucouldhavethisfilename"));
}

}  // namespace test
}  // namespace peloton
