//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sampler_test.cpp
//
// Identification: test/optimizer/tuple_sampler.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "optimizer/tuple_sampler.h"

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

  // Create a table and wrap it in logical tiles
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), tuple_count, false, false,
                                     true, txn);
  txn_manager.CommitTransaction(txn);

  TupleSampler sampler(data_table.get());

  size_t total_tuple_count;
  size_t sampled_count = sampler.AcquireSampleTuples(10, &total_tuple_count);
  EXPECT_EQ(10, sampled_count);
}



} /* namespace test */
} /* namespace peloton */
