//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sampler_test.cpp
//
// Identification: test/optimizer/tuple_sampler_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "optimizer/stats/tuple_sampler.h"

#include "storage/data_table.h"

#include "executor/testing_executor_util.h"
#include "storage/tile_group.h"

#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class TupleSamplerTests : public PelotonTest {};

TEST_F(TupleSamplerTests, SampleCountTest) {
  const int tuple_count = 100;
  const int tuple_per_tilegroup = 100;

  // Create a table
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_per_tilegroup, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  TupleSampler sampler(data_table.get());

  size_t sampled_count = sampler.AcquireSampleTuples(10);
  EXPECT_EQ(sampled_count, 10);

  std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples =
      sampler.GetSampledTuples();
  EXPECT_EQ(sampled_tuples.size(), 10);
}

}  // namespace test
}  // namespace peloton
