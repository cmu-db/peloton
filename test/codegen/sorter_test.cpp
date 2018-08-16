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
#include "util/string_util.h"

namespace peloton {
namespace test {

// What we will be sorting
struct TestTuple {
  uint32_t col_a;
  uint32_t col_b;
  uint32_t col_c;
  uint32_t col_d;

  std::string ToString() const {
    return StringUtil::Format("TT[%u,%u,%u,%u]", col_a, col_b, col_c, col_d);
  }
};

// The comparison function for TestTuples. We sort on column B.
static int CompareTuplesForAscending(const char *a, const char *b) {
  const auto *at = reinterpret_cast<const TestTuple *>(a);
  const auto *bt = reinterpret_cast<const TestTuple *>(b);
  return at->col_b - bt->col_b;
}

class SorterTest : public PelotonTest {
 public:
  std::unique_ptr<executor::ExecutorContext> ctx;

  SorterTest() : ctx(new executor::ExecutorContext(nullptr)) {}

  executor::ExecutorContext &ExecCtx() { return *ctx; }
  ::peloton::type::AbstractPool &Pool() { return *ctx->GetPool(); }

  static std::vector<TestTuple> GenerateRandomData(uint64_t num_rows) {
    std::random_device r;
    std::default_random_engine e(r());
    std::uniform_int_distribution<uint32_t> gen;

    std::vector<TestTuple> tuples;
    for (uint32_t i = 0; i < num_rows; i++) {
      TestTuple tuple = {.col_a = gen(e) % 100,
                         .col_b = gen(e) % 100,
                         .col_c = gen(e) % 10000,
                         .col_d = gen(e) % 100000};
      tuples.emplace_back(tuple);
    }
    return tuples;
  }

  static void LoadSorter(codegen::util::Sorter &sorter, uint64_t num_inserts) {
    auto test_data = GenerateRandomData(num_inserts);
    sorter.TypedInsertAll(test_data);
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
    codegen::util::Sorter sorter{Pool(), CompareTuplesForAscending,
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
  uint32_t num_threads = 4;

  // Allocate sorters for fake threads
  auto &thread_states = ExecCtx().GetThreadStates();
  thread_states.Reset(sizeof(codegen::util::Sorter));
  thread_states.Allocate(num_threads);

  // Load each sorter
  uint32_t num_tuples = 5000000;
  uint32_t ntuples_per_sorter = num_tuples / num_threads;

  // Load each sorter
  for (uint32_t i = 0; i < num_threads; i++) {
    auto *sorter = reinterpret_cast<codegen::util::Sorter *>(
        thread_states.AccessThreadState(i));
    codegen::util::Sorter::Init(*sorter, ExecCtx(), CompareTuplesForAscending,
                                sizeof(TestTuple));
    LoadSorter(*sorter, ntuples_per_sorter);
  }

  {
    codegen::util::Sorter main_sorter{Pool(), CompareTuplesForAscending,
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

TEST_F(SorterTest, SortForTopK) {
  auto test = [this](uint64_t num_inserts, uint64_t top_k) {
    // The sorter
    codegen::util::Sorter sorter{Pool(), CompareTuplesForAscending,
                                 sizeof(TestTuple)};

    // Generate test data, create a copy and sort the WHOLE thing
    auto test_data = GenerateRandomData(num_inserts);

    // Insert and sort
    sorter.TypedInsertAllForTopK(test_data, top_k);
    sorter.Sort();

    auto cmp = [](const TestTuple &a, const TestTuple &b) {
      return a.col_b < b.col_b;
    };
    std::sort(test_data.begin(), test_data.end(), cmp);

    // Check
    EXPECT_EQ(std::min(top_k, num_inserts), sorter.NumTuples());

    auto top_k_iter = sorter.begin();
    auto top_k_end = sorter.end();
    for (auto sorted_iter = test_data.begin(); top_k_iter != top_k_end;
         ++top_k_iter, ++sorted_iter) {
      auto *top_k_tuple = reinterpret_cast<TestTuple *>(*top_k_iter);
      EXPECT_EQ(sorted_iter->col_b, top_k_tuple->col_b)
          << sorted_iter->ToString() << " != " << top_k_tuple->ToString();
    }
  };

  //////////////////////////////////////////////////////////
  /// Three tests:
  ///
  /// 1. limit = 1
  /// 2. limit < num_inserts
  /// 3. limit > num_inserts
  ///
  /// Results should always be sorted. The only difference
  /// will be the number of returned results.
  //////////////////////////////////////////////////////////
  test(100, 1);
  test(100, 10);
  test(100, 200);
}

}  // namespace test
}  // namespace peloton