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

  class TransactionTestsUtil {
  public:
    // Create a simple table with two columns: the id column and the value column
    // Further add a unique index on the id column. The table has one tuple (0, 0)
    // when created
    static storage::DataTable *CreateTable();
    static bool ExecuteInsert(concurrency::Transaction *transaction, storage::DataTable *table, int id, int value);
    static int ExecuteRead(concurrency::Transaction *transaction, storage::DataTable *table, int id);
    static bool ExecuteDelete(concurrency::Transaction *transaction, storage::DataTable *table, int id);
    static bool ExecuteUpdate(concurrency::Transaction *transaction, storage::DataTable *table, int id, int value);

  private:
    static planner::ProjectInfo *MakeProjectInfoFromTuple(const storage::Tuple *tuple);
    static expression::ComparisonExpression<expression::CmpEq> *MakePredicate(int key);
  };
}
}