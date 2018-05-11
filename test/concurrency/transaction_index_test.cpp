//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_index_test.cpp
//
// Identification: test/concurrency/transaction_index_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "concurrency/testing_transaction_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TransactionContext Index Tests
//===--------------------------------------------------------------------===//

class TransactionIndexTests : public PelotonTest {};

static std::vector<ProtocolType> PROTOCOL_TYPES = {
    ProtocolType::TIMESTAMP_ORDERING};

static IsolationLevelType ISOLATION_LEVEL_TYPE =
    IsolationLevelType::SERIALIZABLE;

TEST_F(TransactionIndexTests, BasicIndexTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(protocol_type,
                                                      ISOLATION_LEVEL_TYPE);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    storage::DataTable *table =
        TestingTransactionUtil::CreateTableWithoutIndex();
    // basic create and drop index test
    {
      TransactionScheduler scheduler(2, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).DropIndex();
      scheduler.Txn(1).Commit();
      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[1].drop_index_results[0]);
    }

    // test with concurrent transactions
    {
      TransactionScheduler scheduler(3, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(1).CreateIndex();
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Commit();
      scheduler.Txn(2).DropIndex();
      scheduler.Txn(2).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      // create two index with same name, can success
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      // TODO: maybe should return 0?
      EXPECT_EQ(1, scheduler.schedules[1].create_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].drop_index_results[0]);
    }

    // create index and insert new record
    {
      TransactionScheduler scheduler(3, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(1).Insert(100, 0);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Commit();
      scheduler.Txn(2).Read(100);
      scheduler.Txn(2).DropIndex();
      scheduler.Txn(2).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].drop_index_results[0]);
    }

    // create index and update record
    {
      TransactionScheduler scheduler(3, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(1).Insert(200, 0);
      scheduler.Txn(1).Update(200, 1);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Commit();
      scheduler.Txn(2).Read(200);
      scheduler.Txn(2).DropIndex();
      scheduler.Txn(2).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].drop_index_results[0]);
    }

    // create index and delete record
    {
      TransactionScheduler scheduler(3, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(1).Delete(1);
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).Commit();
      scheduler.Txn(2).Read(1);
      scheduler.Txn(2).DropIndex();
      scheduler.Txn(2).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      EXPECT_EQ(-1, scheduler.schedules[2].results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].drop_index_results[0]);
    }

    // transaction abort test
    {
      TransactionScheduler scheduler(3, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(0).Abort();
      scheduler.Txn(1).CreateIndex();
      scheduler.Txn(1).Commit();
      scheduler.Txn(2).DropIndex();
      scheduler.Txn(2).Commit();

      scheduler.Run();

      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[1].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[1].create_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].drop_index_results[0]);
    }

    {
      TransactionScheduler scheduler(3, table, &txn_manager);
      scheduler.Txn(0).CreateIndex();
      scheduler.Txn(0).Commit();
      scheduler.Txn(1).DropIndex();
      scheduler.Txn(1).Abort();
      scheduler.Txn(2).DropIndex();
      scheduler.Txn(2).Commit();

      scheduler.Run();
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[0].txn_result);
      EXPECT_EQ(ResultType::ABORTED, scheduler.schedules[1].txn_result);
      EXPECT_EQ(ResultType::SUCCESS, scheduler.schedules[2].txn_result);

      EXPECT_EQ(1, scheduler.schedules[0].create_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[1].drop_index_results[0]);
      EXPECT_EQ(1, scheduler.schedules[2].drop_index_results[0]);
    }
  }
}

}  // namespace test
}  // namespace peloton

