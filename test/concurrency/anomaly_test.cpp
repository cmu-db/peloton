//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// anomaly_test.cpp
//
// Identification: test/concurrency/anomaly_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/testing_transaction_util.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Anomaly Tests
//===--------------------------------------------------------------------===//
// These test cases are based on the paper: 
// -- "A Critique of ANSI SQL Isolation Levels"

class AnomalyTests : public PelotonTest {};

static std::vector<ProtocolType> PROTOCOL_TYPES = {
    ProtocolType::TIMESTAMP_ORDERING
};

static std::vector<IsolationLevelType> ISOLATION_LEVEL_TYPES = {
  IsolationLevelType::SERIALIZABLE, 
  IsolationLevelType::SNAPSHOT, 
  IsolationLevelType::REPEATABLE_READS, 
  IsolationLevelType::READ_COMMITTED
};

static std::vector<ConflictAvoidanceType> CONFLICT_AVOIDANCE_TYPES = {
  // ConflictAvoidanceType::WAIT,
  ConflictAvoidanceType::ABORT
};


// Dirty write:
// TransactionContext T1 modifies a data item. Another transaction T2 then further
// modifies that data item before T1 performs a COMMIT or ROLLBACK.
// If T1 or T2 then performs a ROLLBACK, it is unclear what the correct 
// data value should be.

// for all isolation levels, dirty write must never happen.
void DirtyWriteTest(const ProtocolType protocol UNUSED_ATTRIBUTE, 
                    const IsolationLevelType isolation, 
                    const ConflictAvoidanceType conflict) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 updates (0, ?) to (0, 2)
    // T0 commits
    // T1 commits
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

      if (isolation == IsolationLevelType::SNAPSHOT) {
        EXPECT_EQ(0, scheduler.schedules[2].results[0]);
      } else {
        EXPECT_EQ(2, scheduler.schedules[2].results[0]);        
      }
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);      
      
      if (isolation == IsolationLevelType::SNAPSHOT) {
        EXPECT_EQ(0, scheduler.schedules[2].results[0]);
      } else {
        EXPECT_EQ(1, scheduler.schedules[2].results[0]);        
      }
    }

    schedules.clear();
  }

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 updates (0, ?) to (0, 2)
    // T1 commits
    // T0 commits
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

      if (isolation == IsolationLevelType::SNAPSHOT) {
        EXPECT_EQ(0, scheduler.schedules[2].results[0]); 
      } else {
        EXPECT_EQ(1, scheduler.schedules[2].results[0]); 
      }
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);      
      
      if (isolation == IsolationLevelType::SNAPSHOT) {
        EXPECT_EQ(0, scheduler.schedules[2].results[0]);
      } else {
        EXPECT_EQ(1, scheduler.schedules[2].results[0]);        
      }
    }

    schedules.clear();
  }

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 updates (0, ?) to (0, 2)
    // T0 aborts
    // T1 commits
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(0).Abort();
    scheduler.Txn(1).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

      EXPECT_EQ(2, scheduler.schedules[2].results[0]);
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);      
      
      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    }

    schedules.clear();
  }

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 updates (0, ?) to (0, 2)
    // T1 commits
    // T0 aborts
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Abort();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

      EXPECT_EQ(2, scheduler.schedules[2].results[0]);
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);      
      
      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    }

    schedules.clear();
  }


  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 updates (0, ?) to (0, 2)
    // T0 aborts
    // T1 aborts
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(0).Abort();
    scheduler.Txn(1).Abort();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);      
      
      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    }

    schedules.clear();
  }


  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 updates (0, ?) to (0, 2)
    // T1 aborts
    // T0 aborts
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Update(0, 2);
    scheduler.Txn(1).Abort();
    scheduler.Txn(0).Abort();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);      
      
      EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    }

    schedules.clear();
  }
}


// Dirty read:
// TransactionContext T1 modifies a data item. Another transaction T2 then reads
// that data item before T1 performs a COMMIT or ROLLBACK.
// If T1 then performs a ROLLBACK, T2 has read a data item that was never
// committed and so never really existed.

// for all isolation levels except READ_UNCOMMITTED, 
// dirty read must never happen.
void DirtyReadTest(const ProtocolType protocol UNUSED_ATTRIBUTE, 
                   const IsolationLevelType isolation, 
                   const ConflictAvoidanceType conflict) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
        TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 reads (0, ?)
    // T0 commits
    // T1 commits
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

      EXPECT_EQ(1, scheduler.schedules[2].results[0]);
    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);

      
      if (isolation == IsolationLevelType::SNAPSHOT) {
        EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);
        EXPECT_EQ(0, scheduler.schedules[1].results[0]);
        EXPECT_EQ(0, scheduler.schedules[2].results[0]);
      } else {
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);
        EXPECT_EQ(1, scheduler.schedules[2].results[0]);        
      }
    }

    schedules.clear();
  }

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
        TestingTransactionUtil::CreateTable();
    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 updates (0, ?) to (0, 1)
    // T1 reads (0, ?)
    // T0 aborts
    // T1 commits
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Abort();
    scheduler.Txn(1).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (conflict == ConflictAvoidanceType::WAIT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
      EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

      EXPECT_EQ(0, scheduler.schedules[2].results[0]);

    }

    if (conflict == ConflictAvoidanceType::ABORT) {
      EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);

      if (isolation == IsolationLevelType::SNAPSHOT) {
        EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);
        EXPECT_EQ(0, scheduler.schedules[1].results[0]); 
        EXPECT_EQ(0, scheduler.schedules[2].results[0]); 
      } else {
        EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);
        EXPECT_EQ(0, scheduler.schedules[2].results[0]); 
      }
    }

    schedules.clear();
  }

}


// Fuzzy read:
// TransactionContext T1 reads a data item. Another transaction T2 then modifies
// or deletes that data item and commits. If T1 then attempts to reread
// the data item, it receives a modified value or discovers that the data
// item has been deleted.

// for all isolation levels except READ_UNCOMMITTED and READ_COMMITTED, 
// dirty read must never happen.
void FuzzyReadTest(const ProtocolType protocol, 
                   const IsolationLevelType isolation UNUSED_ATTRIBUTE, 
                   const ConflictAvoidanceType conflict) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
        TestingTransactionUtil::CreateTable();

    TransactionScheduler scheduler(3, table, &txn_manager);
    // T0 obtains a smaller timestamp.
    // T0 reads (0, ?)
    // T1 updates (0, ?) to (0, 1)
    // T1 commits
    // T0 reads (0, ?)
    // T0 commits
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(0);
    scheduler.Txn(0).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;


    if (protocol == ProtocolType::TIMESTAMP_ORDERING) {
      if (conflict == ConflictAvoidanceType::ABORT) {
        if (isolation != IsolationLevelType::READ_COMMITTED && 
            isolation != IsolationLevelType::SNAPSHOT) {
        
          EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
          EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

          EXPECT_EQ(0, scheduler.schedules[0].results[0]);
          EXPECT_EQ(0, scheduler.schedules[0].results[1]);

          EXPECT_EQ(1, scheduler.schedules[2].results[0]);
        }
      }
    }

    schedules.clear();
  }


  {
    concurrency::EpochManagerFactory::GetInstance().Reset();
    storage::DataTable *table = 
        TestingTransactionUtil::CreateTable();

    TransactionScheduler scheduler(3, table, &txn_manager);
    // T1 obtains a smaller timestamp.
    // T1 reads (0, ?)
    // T0 reads (0, ?)
    // T1 updates (0, ?) to (0, 1)
    // T1 commits
    // T0 reads (0, ?)
    // T0 commits
    scheduler.Txn(1).Read(0);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(0);
    scheduler.Txn(0).Commit();

    // observer
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Commit();

    scheduler.Run();
    auto &schedules = scheduler.schedules;

    if (protocol == ProtocolType::TIMESTAMP_ORDERING) {
      if (conflict == ConflictAvoidanceType::ABORT) {
        if (isolation == IsolationLevelType::SERIALIZABLE || 
            isolation == IsolationLevelType::REPEATABLE_READS) {

          EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
          EXPECT_TRUE(schedules[1].txn_result == ResultType::ABORTED);

          EXPECT_EQ(0, scheduler.schedules[0].results[0]);
          EXPECT_EQ(0, scheduler.schedules[0].results[1]);
          EXPECT_EQ(0, scheduler.schedules[1].results[0]);

          EXPECT_EQ(0, scheduler.schedules[2].results[0]);
        }
        else if (isolation == IsolationLevelType::SNAPSHOT) {

          EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
          EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

          EXPECT_EQ(0, scheduler.schedules[0].results[0]);
          // TODO: double check!
          EXPECT_EQ(0, scheduler.schedules[0].results[1]);
          EXPECT_EQ(0, scheduler.schedules[1].results[0]);

          // TODO: double check!
          EXPECT_EQ(0, scheduler.schedules[2].results[0]);          
        }
        else if (isolation == IsolationLevelType::READ_COMMITTED) {

          EXPECT_TRUE(schedules[0].txn_result == ResultType::SUCCESS);
          EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

          EXPECT_EQ(0, scheduler.schedules[0].results[0]);
          EXPECT_EQ(1, scheduler.schedules[0].results[1]);
          EXPECT_EQ(0, scheduler.schedules[1].results[0]);

          EXPECT_EQ(1, scheduler.schedules[2].results[0]);          
        }
      }
    }

    schedules.clear();
  }

  // {
  //   storage::DataTable *table = 
  //       TestingTransactionUtil::CreateTable();

  //   TransactionScheduler scheduler(2, table, &txn_manager);
  //   scheduler.Txn(0).Read(0);
  //   scheduler.Txn(1).Delete(0);
  //   scheduler.Txn(1).Delete(1);
  //   scheduler.Txn(1).Commit();
  //   scheduler.Txn(0).Read(1);
  //   scheduler.Txn(0).Commit();

  //   scheduler.Run();
  //   auto &schedules = scheduler.schedules;

  //   if (conflict == ConflictAvoidanceType::WAIT) {
  //     EXPECT_TRUE(schedules[0].txn_result == ResultType::ABORTED);
  //     EXPECT_TRUE(schedules[1].txn_result == ResultType::SUCCESS);

  //     EXPECT_EQ(0, scheduler.schedules[2].results[0]);

  //   }

  //   schedules.clear();
  // }
}

void PhantomTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Insert(5, 0);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();
    size_t original_tuple_count = 10;
    if (scheduler.schedules[0].txn_result == ResultType::SUCCESS &&
        scheduler.schedules[1].txn_result == ResultType::SUCCESS) {
      // Should scan no more tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() ==
                  original_tuple_count * 2);
    }
  }

  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Delete(4);
    scheduler.Txn(0).Scan(0);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();

    size_t original_tuple_count = 11;
    if (scheduler.schedules[0].txn_result == ResultType::SUCCESS &&
        scheduler.schedules[1].txn_result == ResultType::SUCCESS) {
      // Should scan no less tuples
      EXPECT_TRUE(scheduler.schedules[0].results.size() ==
                  original_tuple_count * 2);
    }
  }
}

// Can't pass this test!
void WriteSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();

  {
    // Prepare
    TransactionScheduler scheduler(1, table, &txn_manager);
    scheduler.Txn(0).Update(1, 1);
    scheduler.Txn(0).Commit();
    scheduler.Run();
    EXPECT_EQ(ResultType::SUCCESS,
              scheduler.schedules[0].txn_result);
  }
  {
    // the database has tuple (0, 0), (1, 1)
    // T0 will set all 1 to 0
    // T1 will set all 0 to 1
    // The results are either (0, 0), (1, 0) or (0, 1), (1, 1) in serializable
    // transactions.
    TransactionScheduler scheduler(3, table, &txn_manager);

    scheduler.Txn(0).UpdateByValue(1,
                                   0);  // txn 0 see (1, 1), update it to (1, 0)
    scheduler.Txn(1).UpdateByValue(0,
                                   1);  // txn 1 see (0, 0), update it to (0, 1)
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();
    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Read(1);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_EQ(ResultType::SUCCESS,
              scheduler.schedules[2].txn_result);
    // Can't all success
    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_TRUE(scheduler.schedules[2].results[0] ==
                  scheduler.schedules[2].results[1]);
    }
  }
}

void ReadSkewTest() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
  {
    TransactionScheduler scheduler(2, table, &txn_manager);
    scheduler.Txn(0).Read(0);
    scheduler.Txn(1).Update(0, 1);
    scheduler.Txn(1).Update(1, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Read(1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[0] ==
                  scheduler.schedules[0].results[1]);
    }
  }
}

// Look at the SSI paper (http://drkp.net/papers/ssi-vldb12.pdf).
// This is an anomaly involving three transactions (one of them is a read-only
// transaction).
void SIAnomalyTest1() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::DataTable *table = 
      TestingTransactionUtil::CreateTable();
  int current_batch_key = 10000;
  {
    TransactionScheduler scheduler(1, table, &txn_manager);
    // Prepare
    scheduler.Txn(0).Insert(current_batch_key, 100);
    scheduler.Txn(0).Update(100, 1);
    scheduler.Txn(0).Commit();
    scheduler.Run();
    EXPECT_EQ(ResultType::SUCCESS,
              scheduler.schedules[0].txn_result);
  }
  {
    TransactionScheduler scheduler(4, table, &txn_manager);
    // Test against anomaly
    scheduler.Txn(1).ReadStore(current_batch_key, 0);
    scheduler.Txn(2).Update(current_batch_key, 100 + 1);
    scheduler.Txn(2).Commit();
    scheduler.Txn(0).ReadStore(current_batch_key, -1);
    scheduler.Txn(0).Read(TXN_STORED_VALUE);
    scheduler.Txn(1).Update(TXN_STORED_VALUE, 2);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Txn(3).ReadStore(current_batch_key, -1);
    scheduler.Txn(3).Read(TXN_STORED_VALUE);
    scheduler.Txn(3).Commit();
    scheduler.Run();

    if (ResultType::SUCCESS == scheduler.schedules[0].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[1].txn_result &&
        ResultType::SUCCESS == scheduler.schedules[2].txn_result) {
      EXPECT_TRUE(scheduler.schedules[0].results[1] ==
                  scheduler.schedules[3].results[1]);
    }
  }
}

TEST_F(AnomalyTests, SerializableTest) {
  for (auto protocol_type : PROTOCOL_TYPES) {
    for (auto isolation_level_type: ISOLATION_LEVEL_TYPES) {
      for (auto conflict_avoidance_type : CONFLICT_AVOIDANCE_TYPES) {
        concurrency::TransactionManagerFactory::Configure(
            protocol_type, isolation_level_type, conflict_avoidance_type);

        DirtyWriteTest(protocol_type, isolation_level_type, conflict_avoidance_type);
        DirtyReadTest(protocol_type, isolation_level_type, conflict_avoidance_type);
        FuzzyReadTest(protocol_type, isolation_level_type, conflict_avoidance_type);
        // // WriteSkewTest();
        // ReadSkewTest(isolation_level_type, conflict_avoidance_type);
        // PhantomTest(isolation_level_type, conflict_avoidance_type);
        // SIAnomalyTest1(isolation_level_type, conflict_avoidance_type);

      }
    }
  }
}

TEST_F(AnomalyTests, StressTest) {
  const int num_txn = 2;  // 16
  const int scale = 1;    // 20
  const int num_key = 2;  // 256
  srand(15721);
  for (auto protocol_type : PROTOCOL_TYPES) {
    concurrency::TransactionManagerFactory::Configure(
        protocol_type, 
        IsolationLevelType::SERIALIZABLE, 
        ConflictAvoidanceType::ABORT);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    
    EXPECT_EQ(IsolationLevelType::SERIALIZABLE, txn_manager.GetIsolationLevel());

    storage::DataTable *table = 
        TestingTransactionUtil::CreateTable(num_key);

    TransactionScheduler scheduler(num_txn, table, &txn_manager);
    scheduler.SetConcurrent(true);
    for (int i = 0; i < num_txn; i++) {
      for (int j = 0; j < scale; j++) {
        // randomly select two unique keys
        int key1 = rand() % num_key;
        int key2 = rand() % num_key;
        int delta = rand() % 1000;
        // Store subtracted value
        scheduler.Txn(i).ReadStore(key1, -delta);
        scheduler.Txn(i).Update(key1, TXN_STORED_VALUE);
        LOG_INFO("Txn %d deducts %d from %d", i, delta, key1);
        // Store increased value
        scheduler.Txn(i).ReadStore(key2, delta);
        scheduler.Txn(i).Update(key2, TXN_STORED_VALUE);
        LOG_INFO("Txn %d adds %d to %d", i, delta, key2);
      }
      scheduler.Txn(i).Commit();
    }
    scheduler.Run();

    // Read all values
    TransactionScheduler scheduler2(1, table, &txn_manager);
    for (int i = 0; i < num_key; i++) {
      scheduler2.Txn(0).Read(i);
    }
    scheduler2.Txn(0).Commit();
    scheduler2.Run();

    EXPECT_EQ(ResultType::SUCCESS,
              scheduler2.schedules[0].txn_result);
    // The sum should be zero
    int sum = 0;
    for (auto result : scheduler2.schedules[0].results) {
      LOG_INFO("Table has tuple value: %d", result);
      sum += result;
    }

    EXPECT_EQ(0, sum);

    // stats
    int nabort = 0;
    for (auto &schedule : scheduler.schedules) {
      if (schedule.txn_result == ResultType::ABORTED) nabort += 1;
    }
    LOG_INFO("Abort: %d out of %d", nabort, num_txn);
  }
}

}  // namespace test
}  // namespace peloton
