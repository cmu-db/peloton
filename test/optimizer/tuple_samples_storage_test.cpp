//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_samples_storage_test.cpp
//
// Identification: test/optimizer/tuple_samples_storage_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#define private public
#define protected public

#include "optimizer/stats/tuple_samples_storage.h"
#include "optimizer/stats/tuple_sampler.h"

#include "storage/data_table.h"
#include "storage/database.h"
#include "catalog/catalog.h"
#include "executor/testing_executor_util.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class TupleSamplesStorageTests : public PelotonTest {};

TEST_F(TupleSamplesStorageTests, SamplesDBTest) {
  catalog::Catalog *catalog = catalog::Catalog::GetInstance();
  TupleSamplesStorage *tuple_samples_storage =
      TupleSamplesStorage::GetInstance();
  (void)tuple_samples_storage;
  storage::Database *samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  EXPECT_TRUE(samples_db != nullptr);
  EXPECT_EQ(samples_db->GetDBName(), SAMPLES_DB_NAME);
  EXPECT_EQ(samples_db->GetTableCount(), 0);
}

TEST_F(TupleSamplesStorageTests, AddSamplesTableTest) {
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

  // Acquire samples
  TupleSampler sampler(data_table.get());
  size_t sampled_count = sampler.AcquireSampleTuples(10);
  std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples =
      sampler.GetSampledTuples();

  // Add samples into database
  catalog::Catalog *catalog = catalog::Catalog::GetInstance();
  TupleSamplesStorage *tuple_samples_storage =
      TupleSamplesStorage::GetInstance();
  tuple_samples_storage->AddSamplesTable(data_table.get(), sampled_tuples);

  // Check the sampled tuples
  std::string samples_table_name =
      tuple_samples_storage->GenerateSamplesTableName(
          data_table->GetDatabaseOid(), data_table->GetOid());
  storage::Database *samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  storage::DataTable *samples_table =
      samples_db->GetTableWithName(samples_table_name);
  EXPECT_TRUE(samples_table != nullptr);
  EXPECT_EQ(samples_table->GetTupleCount(), sampled_count);
}

} /* namespace test */
} /* namespace peloton */
