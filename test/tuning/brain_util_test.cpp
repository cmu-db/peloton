//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// brain_util_test.cpp
//
// Identification: test/tuning/brain_util_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <boost/filesystem.hpp>
#include <chrono>
#include <cstdio>
#include <random>

#include "common/harness.h"

#include "tuning/brain_util.h"

#include "executor/testing_executor_util.h"
#include "tuning/sample.h"
#include "common/generator.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// BrainUtil Tests
//===--------------------------------------------------------------------===//

class BrainUtilTests : public PelotonTest {
 public:
  void TearDown() override {
    for (auto path : tempFiles) {
      if (FileUtil::Exists(path)) {
        std::remove(path.c_str());
      }
    }  // FOR
    PelotonTest::TearDown();
  }
  std::vector<std::string> tempFiles;
};

TEST_F(BrainUtilTests, LoadIndexStatisticsFileTest) {
  // Create some Table samples
  std::vector<double> cols0 = {0, 1, 2};
  std::vector<double> cols1 = {9, 8, 7, 6};

  std::map<std::string, tuning::Sample> expected;
  expected.insert(std::map<std::string, tuning::Sample>::value_type(
      "table_x", tuning::Sample(cols0, 888, tuning::SampleType::ACCESS)));
  expected.insert(std::map<std::string, tuning::Sample>::value_type(
      "table_y", tuning::Sample(cols1, 999, tuning::SampleType::ACCESS)));
  EXPECT_FALSE(expected.empty());

  // Serialize them to a string and write them out to a temp file
  std::ostringstream os;
  for (auto x : expected) {
    os << x.first << " " << x.second.ToString() << std::endl;
  }  // FOR
  std::string path = FileUtil::WriteTempFile(os.str(), "index-");
  tempFiles.push_back(path);
  EXPECT_TRUE(FileUtil::Exists(path));
  LOG_TRACE("IndexStats File: %s\n%s", path.c_str(),
            FileUtil::GetFile(path).c_str());

  // Load that mofo back in and make sure our objects match
  std::unordered_map<std::string, std::vector<tuning::Sample>> result =
      tuning::BrainUtil::LoadSamplesFile(path);
  EXPECT_EQ(expected.size(), result.size());
  for (auto s0 : result) {
    auto s1 = expected.find(s0.first);
    // EXPECT_TRUE(s1 != nullptr);
    EXPECT_TRUE(s1->second == s0.second[0]);
  }
}

}  // namespace test
}  // namespace peloton
