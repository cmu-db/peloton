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
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {

namespace storage {
class DataTable;
class Tuple;
}

namespace concurrency {
class Transaction;
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

  bool InsertSampleTuple(storage::DataTable *samples_table,
                         std::unique_ptr<storage::Tuple> tuple,
                         concurrency::Transaction *txn);

  void GetTupleSamples(oid_t database_id, oid_t table_id,
                       std::vector<storage::Tuple> &tuple_samples);

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
