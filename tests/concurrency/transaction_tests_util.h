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
  TXN_OP_COMMIT
};

class TransactionTestsUtil {
 public:
  // Create a simple table with two columns: the id column and the value column
  // Further add a unique index on the id column. The table has one tuple (0, 0)
  // when created
  static storage::DataTable *CreateTable();
  static bool ExecuteInsert(concurrency::Transaction *txn,
                            storage::DataTable *table, int id, int value);
  static bool ExecuteRead(concurrency::Transaction *txn,
                          storage::DataTable *table, int id, int &result);
  static bool ExecuteDelete(concurrency::Transaction *txn,
                            storage::DataTable *table, int id);
  static bool ExecuteUpdate(concurrency::Transaction *txn,
                            storage::DataTable *table, int id, int value);
  static bool ExecuteScan(concurrency::Transaction *txn,
                          std::vector<int> &results, storage::DataTable *table,
                          int id);

 private:
  static planner::ProjectInfo *MakeProjectInfoFromTuple(
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
  TransactionSchedule() : txn_result(RESULT_FAILURE) {}
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

  void Run() {
    std::thread thread(&TransactionThread::RunLoop, this);
    thread.detach();
  }

  void ExecuteNext() {
    // Prepare data for operation
    txn_op_t op = schedule->operations[cur_seq].op;
    int id = schedule->operations[cur_seq].id;
    int value = schedule->operations[cur_seq].value;

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
        LOG_TRACE("Execute Update");
        execute_result =
            TransactionTestsUtil::ExecuteUpdate(txn, table, id, value);
        break;
      }
      case TXN_OP_SCAN: {
        LOG_TRACE("Execute Scan");
        execute_result = TransactionTestsUtil::ExecuteScan(
            txn, schedule->results, table, id);
        break;
      }
      case TXN_OP_ABORT: {
        LOG_TRACE("Abort");
        // Assert last operation
        assert(cur_seq == (int)schedule->operations.size());
        schedule->txn_result = txn_manager->AbortTransaction();
        txn = NULL;
        break;
      }
      case TXN_OP_COMMIT: {
        schedule->txn_result = txn_manager->CommitTransaction();
        txn = NULL;
        break;
      }
    }

    if (txn != NULL && txn->GetResult() == RESULT_FAILURE) {
      txn_manager->AbortTransaction();
      txn = NULL;
      if (execute_result == true) {
        LOG_TRACE("ABORT NOW, Executor returns %s",
                  execute_result ? "true" : "false");
      } else {
        LOG_TRACE("ABORT NOW, Executor returns %s",
                  execute_result ? "true" : "false");
      }
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
        schedules(num_txn) {}

  void Run() {
    for (int i = 0; i < (int)schedules.size(); i++) {
      tthreads.emplace_back(&schedules[i], table, txn_manager);
    }
    for (int i = 0; i < (int)schedules.size(); i++) {
      tthreads[i].Run();
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
  }

  void AddInsert(int txn_id, int id, int value) {
    schedules[txn_id].operations.emplace_back(TXN_OP_INSERT, id, value);
    sequence[time++] = txn_id;
  }
  void AddRead(int txn_id, int id) {
    schedules[txn_id].operations.emplace_back(TXN_OP_READ, id, 0);
    sequence[time++] = txn_id;
  }
  void AddDelete(int txn_id, int id) {
    schedules[txn_id].operations.emplace_back(TXN_OP_DELETE, id, 0);
    sequence[time++] = txn_id;
  }
  void AddUpdate(int txn_id, int id, int value) {
    schedules[txn_id].operations.emplace_back(TXN_OP_UPDATE, id, value);
    sequence[time++] = txn_id;
  }
  void AddScan(int txn_id, int id) {
    schedules[txn_id].operations.emplace_back(TXN_OP_SCAN, id, 0);
    sequence[time++] = txn_id;
  }
  void AddAbort(int txn_id) {
    schedules[txn_id].operations.emplace_back(TXN_OP_ABORT, 0, 0);
    sequence[time++] = txn_id;
  }

  void AddCommit(int txn_id) {
    schedules[txn_id].operations.emplace_back(TXN_OP_COMMIT, 0, 0);
    sequence[time++] = txn_id;
  }

  void clear() { schedules.clear(); }

  concurrency::TransactionManager *txn_manager;
  storage::DataTable *table;
  int time;
  std::vector<TransactionSchedule> schedules;
  std::vector<TransactionThread> tthreads;
  std::map<int, int> sequence;
};
}
}