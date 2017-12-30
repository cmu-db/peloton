//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_samples_storage.h
//
// Identification: src/include/optimizer/stats/tuple_samples_storage.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>

#include "common/macros.h"
#include "common/internal_types.h"
#include "type/value_factory.h"

#define SAMPLE_COUNT_PER_TABLE 100

namespace peloton {

namespace storage {
class DataTable;
class Tuple;
}

namespace concurrency {
class TransactionContext;
}

namespace executor {
class LogicalTile;
}

namespace optimizer {

#define SAMPLES_DB_NAME "samples_db"

class TupleSamplesStorage {
 public:
  // Global Singleton
  static TupleSamplesStorage *GetInstance();

  TupleSamplesStorage();

  /* Functions for managing tuple samples */

  void CreateSamplesDatabase();

  void AddSamplesTable(
      storage::DataTable *data_table,
      std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples);

  ResultType DeleteSamplesTable(oid_t database_id, oid_t table_id,
                                concurrency::TransactionContext *txn = nullptr);

  bool InsertSampleTuple(storage::DataTable *samples_table,
                         std::unique_ptr<storage::Tuple> tuple,
                         concurrency::TransactionContext *txn);

  ResultType CollectSamplesForTable(storage::DataTable *data_table,
                                    concurrency::TransactionContext *txn = nullptr);

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetTuplesWithSeqScan(storage::DataTable *data_table,
                       std::vector<oid_t> column_offsets,
                       concurrency::TransactionContext *txn);

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetTupleSamples(oid_t database_id, oid_t table_id);

  void GetColumnSamples(oid_t database_id, oid_t table_id, oid_t column_id,
                        std::vector<type::Value> &column_samples);

  std::string GenerateSamplesTableName(oid_t db_id, oid_t table_id) {
    return std::to_string(db_id) + "_" + std::to_string(table_id);
  }

 private:
  std::unique_ptr<type::AbstractPool> pool_;
};
}
}
