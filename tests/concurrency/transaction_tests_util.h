//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.h
//
// Identification: tests/concurrency/transaction_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * How to use the transaction test utilities
 *
 * These utilities are used to construct test cases for transaction and
 * concurrency control related tests. It makes you be able to describe the
 * schedule of each transaction (when to do what), i.e. you can describe the
 * serilized orders of each operation among the transactions.
 *
 * To schedule a txn tests, you need a TransactionScheduler (scheduler). Then
 * write the schedule in the following way: scheduler.Txn(n).ACTION(args)
 * scheduler.Txn(0).Insert(0, 1);
 * scheduler.Txn(0).Read(0);
 * scheduler.Commit();
 *  => Notice that this order will be the serial order to excute the operaions
 *
 * There's a CreateTable() method, it will create a table with two columns:
 * key and value, and a primiary index on the key column. The table is pre-
 * populated with the following tuples:
 * (0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0),(9, 0)
 *
 * ACTION supported:
 * * Insert(key, value): Insert (key, value) into DB, key must be unique
 * * Read(key): Read value from DB, if the key does not exist, will read a value
 * *            of -1
 * * Update(key, value): Update the value of *key* to *value*
 * * Delete(key): delete tuple with key *key*
 * * Scan(key): Scan the table for key >= *key*, if nothing satisfies key >=
 * *            *key*, will scan a value of -1
 * * ReadStore(key, modify): Read value with key *key* from DB, and store the
 * *                         (result+*modify*) temporarily, the stored value
 * *                         can be further refered as TXN_STORED_VALUE in
 * *                         any above operations.
 * * Commit(): Commit the txn
 * * Abort(): Abort the txn
 *
 * Then, run the schedules by scheduler.Run(), it will schedule the txns to
 * execute corresponding opersions.
 * The results of executing Run() can be fetched from
 * scheduler.schedules[TXN_ID].results[]. It will store the results from Read()
 * and Scan(), in the order they executed. The txn result (SUCCESS, FAILURE)
 * can be retrieved from scheduler.schedules[TXN_ID].txn_result.
 *
 * See isolation_level_test.cpp for examples.
 */

#include "harness.h"
#include "backend/catalog/schema.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group_header.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"
#include "backend/common/types.h"
#include "backend/expression/comparison_expression.h"

#pragma once

namespace peloton {

namespace storage {
class Backend;
class TileGroup;
class TileGroupFactory;
class TileGroupHeader;
class DataTable;
class TableFactory;
class Tuple;
}

namespace catalog {
class Column;
class Manager;
}

namespace index {
class IndexMetadata;
class IndexFactory;
}

namespace planner {
class ProjectInfo;
}

namespace test {
enum txn_op_t {
  TXN_OP_READ,
  TXN_OP_INSERT,
  TXN_OP_UPDATE,
  TXN_OP_DELETE,
  TXN_OP_SCAN,
  TXN_OP_ABORT,
  TXN_OP_COMMIT,
  TXN_OP_READ_STORE,
  TXN_OP_UPDATE_BY_VALUE
};

#define TXN_STORED_VALUE      -10000

class TransactionTestsUtil {
 public:
  // Create a simple table with two columns: the id column and the value column
  // Further add a unique index on the id column. The table has one tuple (0, 0)
  // when created
  static storage::DataTable *CreateTable(int num_key = 10,
                                         std::string table_name = "TEST_TABLE",
                                         oid_t database_id = INVALID_OID,
                                         oid_t relation_id = INVALID_OID,
                                         oid_t index_oid = 1234,
                                         bool need_primary_index = false);

  // Create the same table as CreateTable with primary key constrainst on id and
  // unique key constraints on value
  static storage::DataTable *CreatePrimaryKeyUniqueKeyTable();

  // Create the same table with combined primary key constrainst on (id, value)
  static storage::DataTable *CreateCombinedPrimaryKeyTable();

  static bool ExecuteInsert(concurrency::Transaction *txn,
                            storage::DataTable *table, int id, int value);
  static bool ExecuteRead(concurrency::Transaction *txn,
                          storage::DataTable *table, int id, int &result);
  static bool ExecuteDelete(concurrency::Transaction *txn,
                            storage::DataTable *table, int id);
  static bool ExecuteUpdate(concurrency::Transaction *txn,
                            storage::DataTable *table, int id, int value);
  static bool ExecuteUpdateByValue(concurrency::Transaction *txn,
                                   storage::DataTable *table, int old_value, int new_value);
  static bool ExecuteScan(concurrency::Transaction *txn,
                          std::vector<int> &results, storage::DataTable *table,
                          int id);

 private:
  static std::unique_ptr<const planner::ProjectInfo> MakeProjectInfoFromTuple(
      const storage::Tuple *tuple);
  static expression::ComparisonExpression<expression::CmpEq> *MakePredicate(
      int id);
};

struct TransactionOperation {
  // Operation of the txn
  txn_op_t op;
  // id of the row to be manipulated
  int id;
  // value of the row, used by INSERT and DELETE operation
  int value;
  TransactionOperation(txn_op_t op_, int id_, int value_)
      : op(op_), id(id_), value(value_){};
};

// The schedule for transaction execution
struct TransactionSchedule {
  Result txn_result;
  std::vector<TransactionOperation> operations;
  std::vector<int> results;
  int stored_value;
  int schedule_id;
  TransactionSchedule(int schedule_id_) : txn_result(RESULT_FAILURE), stored_value(0), schedule_id(schedule_id_) {}
};

// A thread wrapper that runs a transaction
class TransactionThread {
 public:
  TransactionThread(TransactionSchedule *sched, storage::DataTable *table_,
                    concurrency::TransactionManager *txn_manager_)
      : schedule(sched),
        txn_manager(txn_manager_),
        table(table_),
        cur_seq(0),
        go(false) {
    LOG_TRACE("Thread has %d ops", (int)sched->operations.size());
  }

  void RunLoop() {
    while (true) {
      while (!go) {
        std::chrono::milliseconds sleep_time(1);
        std::this_thread::sleep_for(sleep_time);
      };
      ExecuteNext();
      if (cur_seq == (int)schedule->operations.size()) {
        go = false;
        return;
      }
      go = false;
    }
  }

  void RunNoWait() {
    while (true) {
      ExecuteNext();
      if (cur_seq == (int)schedule->operations.size()) {
        break;
      }
    }
  }

  std::thread Run(bool no_wait = false) {
    if (!no_wait)
      return std::thread(&TransactionThread::RunLoop, this);
    else
      return std::thread(&TransactionThread::RunNoWait, this);
  }

  void ExecuteNext() {
    // Prepare data for operation
    txn_op_t op = schedule->operations[cur_seq].op;
    int id = schedule->operations[cur_seq].id;
    int value = schedule->operations[cur_seq].value;

    if (id == TXN_STORED_VALUE)
      id = schedule->stored_value;
    if (value == TXN_STORED_VALUE)
      value = schedule->stored_value;

    if (cur_seq == 0) txn = txn_manager->BeginTransaction();
    if (schedule->txn_result == RESULT_ABORTED) {
      cur_seq++;
      return;
    }

    cur_seq++;
    bool execute_result = true;

    // Execute the operation
    switch (op) {
      case TXN_OP_INSERT: {
        LOG_TRACE("Execute Insert");
        execute_result =
            TransactionTestsUtil::ExecuteInsert(txn, table, id, value);
        break;
      }
      case TXN_OP_READ: {
        LOG_TRACE("Execute Read");
        int result;
        execute_result =
            TransactionTestsUtil::ExecuteRead(txn, table, id, result);
        schedule->results.push_back(result);
        break;
      }
      case TXN_OP_DELETE: {
        LOG_TRACE("Execute Delete");
        execute_result = TransactionTestsUtil::ExecuteDelete(txn, table, id);
        break;
      }
      case TXN_OP_UPDATE: {
        execute_result =
            TransactionTestsUtil::ExecuteUpdate(txn, table, id, value);
        LOG_INFO("Txn %d Update %d's value to %d, %d", schedule->schedule_id, id, value, execute_result);
        break;
      }
      case TXN_OP_SCAN: {
        LOG_TRACE("Execute Scan");
        execute_result = TransactionTestsUtil::ExecuteScan(
            txn, schedule->results, table, id);
        break;
      }
      case TXN_OP_UPDATE_BY_VALUE: {
        int old_value = id;
        int new_value = value;
        execute_result = TransactionTestsUtil::ExecuteUpdateByValue(
            txn, table, old_value, new_value);
        break;
      }
      case TXN_OP_ABORT: {
        LOG_INFO("Txn %d Abort", schedule->schedule_id);
        // Assert last operation
        assert(cur_seq == (int)schedule->operations.size());
        schedule->txn_result = txn_manager->AbortTransaction();
        txn = NULL;
        break;
      }
      case TXN_OP_COMMIT: {
        schedule->txn_result = txn_manager->CommitTransaction();
        LOG_INFO("Txn %d commits: %s", schedule->schedule_id, schedule->txn_result == RESULT_SUCCESS ? "Success" : "Fail");
        txn = NULL;
        break;
      }
      case TXN_OP_READ_STORE: {
        int result;
        execute_result =
            TransactionTestsUtil::ExecuteRead(txn, table, id, result);
        schedule->results.push_back(result);
        LOG_INFO("Txn %d READ_STORE, key: %d, read: %d, modify and stored as: %d", schedule->schedule_id, id, result, result+value);
        schedule->stored_value = result + value;
        break;
      }
    }

    if (txn != NULL && txn->GetResult() == RESULT_FAILURE) {
      txn_manager->AbortTransaction();
      txn = NULL;
      LOG_TRACE("ABORT NOW");
      if (execute_result == false) LOG_TRACE("Executor returns false");
      schedule->txn_result = RESULT_ABORTED;
    }
  }

  TransactionSchedule *schedule;
  concurrency::TransactionManager *txn_manager;
  storage::DataTable *table;
  int cur_seq;
  bool go;
  concurrency::Transaction *txn;
};

// Transaction scheduler, to make life easier writting txn test
class TransactionScheduler {
 public:
  TransactionScheduler(size_t num_txn, storage::DataTable *datatable_,
                       concurrency::TransactionManager *txn_manager_)
      : txn_manager(txn_manager_),
        table(datatable_),
        time(0),
        concurrent(false) {
          for (size_t i = 0; i < num_txn; i++) {
            schedules.emplace_back(i);
          } 
        }

  void Run() {
    // Run the txns according to the schedule
    for (int i = 0; i < (int)schedules.size(); i++) {
      tthreads.emplace_back(&schedules[i], table, txn_manager);
    }
    if (!concurrent) {
      for (int i = 0; i < (int)schedules.size(); i++) {
        std::thread t = tthreads[i].Run();
        t.detach();
      }
      for (auto itr = sequence.begin(); itr != sequence.end(); itr++) {
        LOG_TRACE("Execute %d", (int)itr->second);
        tthreads[itr->second].go = true;
        while (tthreads[itr->second].go) {
          std::chrono::milliseconds sleep_time(1);
          std::this_thread::sleep_for(sleep_time);
        }
        LOG_TRACE("Done %d", (int)itr->second);
      }
    } else {
      // Run the txns concurrently
      std::vector<std::thread> threads(schedules.size());
      for (int i = 0; i < (int)schedules.size(); i++) {
        threads[i] = tthreads[i].Run(true);
      }
      for (auto &thread : threads) {
        thread.join();
      }
      LOG_TRACE("Done conccurent transaction schedule");
    }
  }

  TransactionScheduler &Txn(int txn_id) {
    assert(txn_id < (int)schedules.size());
    cur_txn_id = txn_id;
    return *this;
  }

  void Insert(int id, int value) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_INSERT, id, value);
    sequence[time++] = cur_txn_id;
  }
  void Read(int id) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_READ, id, 0);
    sequence[time++] = cur_txn_id;
  }
  void Delete(int id) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_DELETE, id, 0);
    sequence[time++] = cur_txn_id;
  }
  void Update(int id, int value) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_UPDATE, id, value);
    sequence[time++] = cur_txn_id;
  }
  void Scan(int id) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_SCAN, id, 0);
    sequence[time++] = cur_txn_id;
  }
  void Abort() {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_ABORT, 0, 0);
    sequence[time++] = cur_txn_id;
  }
  void Commit() {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_COMMIT, 0, 0);
    sequence[time++] = cur_txn_id;
  }
  void UpdateByValue(int old_value, int new_value) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_UPDATE_BY_VALUE, old_value, new_value);
    sequence[time++] = cur_txn_id;
  }
  // ReadStore will store the (result of read + modify) to the schedule, the
  // schedule may refer it by using TXN_STORED_VALUE in adding a new operation
  // to a schedule. See usage in isolation_level_test SIAnomalyTest.
  void ReadStore(int id, int modify) {
    schedules[cur_txn_id].operations.emplace_back(TXN_OP_READ_STORE, id, modify);
    sequence[time++] = cur_txn_id;
  }

  void SetConcurrent(bool flag) {
    concurrent = flag;
  }

  concurrency::TransactionManager *txn_manager;
  storage::DataTable *table;
  int time;
  std::vector<TransactionSchedule> schedules;
  std::vector<TransactionThread> tthreads;
  std::map<int, int> sequence;
  int cur_txn_id;
  bool concurrent;
};
}
}
