//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_test.cpp
//
// Identification: test/codegen/sorter_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <random>

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
  static void LoadSorter(codegen::util::Sorter &sorter, uint64_t num_inserts) {
    std::random_device r;
    std::default_random_engine e(r());
    std::uniform_int_distribution<uint32_t> gen;

    for (uint32_t i = 0; i < num_inserts; i++) {
      auto *tuple = reinterpret_cast<TestTuple *>(sorter.StoreInputTuple());
      tuple->col_a = gen(e) % 100;
      tuple->col_b = gen(e) % 1000;
      tuple->col_c = gen(e) % 10000;
      tuple->col_d = gen(e) % 100000;
    }
  }

  static void CheckSorted(codegen::util::Sorter &sorter, bool ascending) {
    uint32_t last_col_b = std::numeric_limits<uint32_t>::max();
    for (auto iter : sorter) {
      const auto *tt = reinterpret_cast<const TestTuple *>(iter);
      if (last_col_b != std::numeric_limits<uint32_t>::max()) {
        if (ascending) {
          EXPECT_LE(last_col_b, tt->col_b);
        } else {
          EXPECT_GE(last_col_b, tt->col_b);
        }
      }
      last_col_b = tt->col_b;
    }
  }

  void TestSort(uint64_t num_tuples_to_insert = 100) {
    ::peloton::type::EphemeralPool pool;

    {
      codegen::util::Sorter sorter{pool, CompareTuplesForAscending,
                                   sizeof(TestTuple)};

      // Time this stuff
      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      // Load the sorter
      LoadSorter(sorter, num_tuples_to_insert);

      timer.Stop();
      LOG_INFO("Loading %" PRId64 " tuples into sort took %.2f ms",
               num_tuples_to_insert, timer.GetDuration());
      timer.Reset();
      timer.Start();

      // Sort
      sorter.Sort();

      timer.Stop();
      LOG_INFO("Sorting %" PRId64 " tuples took %.2f ms", num_tuples_to_insert,
               timer.GetDuration());

      // Check sorted results
      CheckSorted(sorter, true);

      EXPECT_EQ(num_tuples_to_insert, sorter.NumTuples());
    }
  }
};

TEST_F(SorterTest, CanSortTuples) {
  // Test sorting 10
  TestSort(100);
}

TEST_F(SorterTest, BenchmarkSorter) {
  // Test sorting 5 million input tuples
  TestSort(5000000);
}

TEST_F(SorterTest, ParallelSortTest) {
  // A fake executor context associated to no transaction
  executor::ExecutorContext ctx(nullptr);

  uint32_t num_threads = 4;

  // Allocate sorters for fake threads
  auto &thread_states = ctx.GetThreadStates();
  thread_states.Reset(sizeof(codegen::util::Sorter));
  thread_states.Allocate(num_threads);

  // Load each sorter
  uint32_t num_tuples = 5000000;
  uint32_t ntuples_per_sorter = num_tuples / num_threads;

  // Load each sorter
  for (uint32_t i = 0; i < num_threads; i++) {
    auto *sorter = reinterpret_cast<codegen::util::Sorter *>(
        thread_states.AccessThreadState(i));
    codegen::util::Sorter::Init(*sorter, ctx, CompareTuplesForAscending,
                                sizeof(TestTuple));
    LoadSorter(*sorter, ntuples_per_sorter);
  }

  {
    codegen::util::Sorter main_sorter{*ctx.GetPool(), CompareTuplesForAscending,
                                      sizeof(TestTuple)};
    Timer<std::milli> timer;
    timer.Start();

    // Sort parallel
    main_sorter.SortParallel(thread_states, 0);

    timer.Stop();
    LOG_INFO("Parallel sort took: %.2lf ms", timer.GetDuration());

    // Check main sorter is sorted
    CheckSorted(main_sorter, true);

    // Check result size
    EXPECT_EQ(num_tuples, main_sorter.NumTuples());

    // Clean up
    for (uint32_t i = 0; i < num_threads; i++) {
      auto *sorter = reinterpret_cast<codegen::util::Sorter *>(
          thread_states.AccessThreadState(i));
      codegen::util::Sorter::Destroy(*sorter);
    }
  }
}

}  // namespace test
}  // namespace peloton