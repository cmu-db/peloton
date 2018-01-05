//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_test.cpp
//
// Identification: test/codegen/sorter_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>

#include "common/harness.h"
#include "common/timer.h"
#include "codegen/util/sorter.h"

namespace peloton {
namespace test {

// What we will be sorting
struct TestTuple {
  uint32_t col_a;
  uint32_t col_b;
  uint32_t col_c;
  uint32_t col_d;
};

// The comparison function for TestTuples. We sort on column B.
static int CompareTuplesForAscending(const char *a, const char *b) {
  const auto *at = reinterpret_cast<const TestTuple *>(a);
  const auto *bt = reinterpret_cast<const TestTuple *>(b);
  return at->col_b - bt->col_b;
}

class SorterTest : public PelotonTest {
 public:
  SorterTest() {
    // Init
    sorter.Init(CompareTuplesForAscending, sizeof(TestTuple));
  }

  ~SorterTest() {
    // Clean up
    sorter.Destroy();
  }

  void TestSort(uint64_t num_tuples_to_insert = 10) {
    // Time this stuff
    Timer<std::ratio<1, 1000>> timer;
    timer.Start();

    // Insert TestTuples
    for (uint32_t i = 0; i < num_tuples_to_insert; i++) {
      TestTuple *tuple =
          reinterpret_cast<TestTuple *>(sorter.StoreInputTuple());
      tuple->col_a = rand() % 100;
      tuple->col_b = rand() % 1000;
      tuple->col_c = rand() % 10000;
      tuple->col_d = rand() % 100000;
    }

    timer.Stop();
    LOG_INFO("Loading %llu tuples into sort took %.2f ms",
             (unsigned long long)num_tuples_to_insert, timer.GetDuration());
    timer.Reset();
    timer.Start();

    // Sort
    sorter.Sort();

    timer.Stop();
    LOG_INFO("Sorting %llu tuples took %.2f ms",
             (unsigned long long)num_tuples_to_insert, timer.GetDuration());

    // Check sorted results
    uint64_t res_tuples = 0;
    uint32_t last_col_b = std::numeric_limits<uint32_t>::max();
    for (auto iter : sorter) {
      const auto *tt = reinterpret_cast<const TestTuple *>(iter);
      if (last_col_b != std::numeric_limits<uint32_t>::max()) {
        EXPECT_LE(last_col_b, tt->col_b);
      }
      last_col_b = tt->col_b;
      res_tuples++;
    }

    EXPECT_EQ(num_tuples_to_insert, res_tuples);
  }

  // The sorter instance
  codegen::util::Sorter sorter;
};

TEST_F(SorterTest, CanSortTuples) {
  // Test sorting 10
  TestSort(10);
}

TEST_F(SorterTest, BenchmarkSorter) {
  // Test sorting 5 million input tuples
  TestSort(5000000);
}

}  // namespace test
}  // namespace peloton