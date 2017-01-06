//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// brain_util_test.cpp
//
// Identification: test/brain/brain_util_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <boost/filesystem.hpp>
#include <chrono>
#include <cstdio>
#include <random>

#include "common/harness.h"

#include "brain/brain_util.h"
#include "brain/sample.h"
#include "common/generator.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// BrainUtil Tests
//===--------------------------------------------------------------------===//

class BrainUtilTests : public PelotonTest {};

TEST_F(BrainUtilTests, LoadIndexStatisticsFileTest) {
  // Create some Table samples
  std::vector<double> cols0 = {0, 1, 2};
  std::vector<double> cols1 = {9, 8, 7, 6};
  std::map<std::string, brain::Sample> expected;
  //  expected["TABLE_X"] =
  //      brain::Sample(cols0, 888, brain::SAMPLE_TYPE_ACCESS, 1.234);
  //
  //  expected["TABLE_Y"] =
  //      brain::Sample(cols1, 999, brain::SAMPLE_TYPE_ACCESS, 5.678);

  // Serialize them to a string and write them out to a temp file
  std::ostringstream os;
  for (auto x : expected) {
    os << x.first << " " << x.second.ToString() << std::endl;
  }  // FOR
  std::string path = FileUtil::WriteTempFile(os.str(), "index-");
  LOG_INFO("IndexStats File: %s", path.c_str());
  EXPECT_TRUE(FileUtil::Exists(path));
}

}  // End test namespace
}  // End peloton namespace
