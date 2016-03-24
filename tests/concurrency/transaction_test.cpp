//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_test.cpp
//
// Identification: tests/concurrency/transaction_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
#include "backend/concurrency/transaction.h"
#include "backend/common/types.h"

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

namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class TransactionTests : public PelotonTest {};

enum txn_op_t {
  TXN_OP_READ,
  TXN_OP_INSERT,
  TXN_OP_UPDATE,
  TXN_OP_DELETE,
  TXN_OP_NOTHING
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
  std::vector<TransactionOperation> operations;
  std::vector<int> times;
  std::vector<int> results;
  void AddInsert(int id, int value, int time) {
    operations.emplace_back(TXN_OP_INSERT, id, value);
    times.push_back(time);
  }
  void AddRead(int id, int time) {
    operations.emplace_back(TXN_OP_READ, id, 0);
    times.push_back(time);
  }
  void AddDelete(int id, int time) {
    operations.emplace_back(TXN_OP_DELETE, id, 0);
    times.push_back(time);
  }
  void AddUpdate(int id, int value, int time) {
    operations.emplace_back(TXN_OP_UPDATE, id, value);
    times.push_back(time);
  }
  void AddDoNothing(int time) {
    operations.emplace_back(TXN_OP_NOTHING, 0, 0);
    times.push_back(time);
  }
};

// Create a simple table with two columns: the id column and the value column
// Further add a unique index on the id column
storage::DataTable *CreateTable() {
  auto id_column = catalog::Column(VALUE_TYPE_INTEGER,
                                   GetTypeSize(VALUE_TYPE_INTEGER), "id", true);
  auto value_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value", true);

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});
  auto table_name = "TEST_TABLE";
  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create index on the id column
  std::vector<oid_t> key_attrs = {0};
  auto tuple_schema = table->GetSchema();
  bool unique = true;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_metadata = new index::IndexMetadata(
      "primary_btree_index", 1234, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(pkey_index);

  return table;
}

// Execute a schedule, the result for any read will be recorded in
// schedule->results
void ExecuteSchedule(concurrency::TransactionManager *txn_manager,
                     storage::DataTable *table, TransactionSchedule *schedule) {
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  // Sleep for the first operation, this gives the opportunity to schedule
  // the start time of a txn
  int last_time = schedule->times[0];
  std::this_thread::sleep_for(std::chrono::microseconds(last_time));

  auto transaction = txn_manager->BeginTransaction();
  for (int i = 0; i < (int)schedule->operations.size(); i++) {
    // Sleep milliseconds
    std::this_thread::sleep_for(
        std::chrono::microseconds(schedule->times[i] - last_time));
    last_time = schedule->times[i];

    // Prepare data for operation
    txn_op_t op = schedule->operations[i].op;
    int id = schedule->operations[i].id;
    int value = schedule->operations[i].value;
    std::unique_ptr<storage::Tuple> tuple;
    std::unique_ptr<storage::Tuple> key;

    if (op == TXN_OP_DELETE || op == TXN_OP_UPDATE || op == TXN_OP_READ) {
      key = std::unique_ptr<storage::Tuple>(
          new storage::Tuple(table->GetIndex(0)->GetKeySchema(), true));
      key->SetValue(0, ValueFactory::GetIntegerValue(id), testing_pool);
    }

    if (op == TXN_OP_INSERT || op == TXN_OP_UPDATE) {
      tuple = std::unique_ptr<storage::Tuple>(
          new storage::Tuple(table->GetSchema(), true));
      tuple->SetValue(0, ValueFactory::GetIntegerValue(id), testing_pool);
      tuple->SetValue(1, ValueFactory::GetIntegerValue(value), testing_pool);
    }

    // Execute the operation
    switch (op) {
      case TXN_OP_INSERT: {
        LOG_TRACE("Execute Insert");
        table->InsertTuple(transaction, tuple.get());
        break;
      }
      case TXN_OP_READ: {
        auto index = table->GetIndex(0);
        auto location = index->ScanKey(key.get())[0];

        // Check visibility
        auto tile_group = table->GetTileGroupById(location.block);
        auto tile_group_header = tile_group->GetHeader();
        txn_id_t tuple_txn_id =
            tile_group_header->GetTransactionId(location.offset);
        cid_t tuple_begin_cid =
            tile_group_header->GetBeginCommitId(location.offset);
        cid_t tuple_end_cid =
            tile_group_header->GetEndCommitId(location.offset);

        if (txn_manager->IsVisible(tuple_txn_id, tuple_begin_cid,
                                   tuple_end_cid)) {
          auto result = table->GetTileGroupById(location.block)
                            ->GetValue(location.offset, 1)
                            .GetIntegerForTestsOnly();
          schedule->results.push_back(result);
        } else {
          schedule->results.push_back(-1);
        }

        break;
      }
      case TXN_OP_DELETE: {
        auto index = table->GetIndex(0);
        auto location = index->ScanKey(key.get())[0];
        table->DeleteTuple(transaction, location);
        break;
      }
      case TXN_OP_UPDATE: {
        // Update is a delete + insert
        auto index = table->GetIndex(0);
        auto location = index->ScanKey(key.get())[0];
        table->DeleteTuple(transaction, location);
        table->InsertTuple(transaction, tuple.get());
        break;
      }
      case TXN_OP_NOTHING: {
        // Do nothing
        break;
      }
    }
  }
  txn_manager->CommitTransaction();
}

void ThreadExecuteSchedule(concurrency::TransactionManager *txn_manager,
                           storage::DataTable *table,
                           std::vector<TransactionSchedule *> schedules) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();
  auto schedule = schedules[thread_id];
  ExecuteSchedule(txn_manager, table, schedule);
}

void TransactionTest(concurrency::TransactionManager *txn_manager) {
  uint64_t thread_id = TestingHarness::GetInstance().GetThreadId();

  for (oid_t txn_itr = 1; txn_itr <= 100; txn_itr++) {
    txn_manager->BeginTransaction();
    if (thread_id % 2 == 0) {
      std::chrono::microseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    }

    if (txn_itr % 50 != 0) {
      txn_manager->CommitTransaction();
    } else {
      txn_manager->AbortTransaction();
    }
  }
}

TEST_F(TransactionTests, TransactionTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  LaunchParallelTest(8, TransactionTest, &txn_manager);

  std::cout << "next Commit Id :: " << txn_manager.GetNextCommitId() << "\n";
}

TEST_F(TransactionTests, SnapshotIsolationTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<storage::DataTable> table(CreateTable());
  std::vector<TransactionSchedule> schedules;
  TransactionSchedule schedule1;
  //  TransactionSchedule schedule2;
  //  TransactionSchedule schedule3;

  schedule1.AddInsert(0, 1, 0);
  schedule1.AddRead(0, 0);
  ExecuteSchedule(&txn_manager, table.get(), &schedule1);
  EXPECT_EQ(1, schedule1.results[0]);
}

}  // End test namespace
}  // End peloton namespace
