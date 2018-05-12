//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metric.h
//
// Identification: src/include/statistics/tuple_access_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <unordered_map>
#include <vector>
#include "statistics/abstract_metric.h"
#include "common/internal_types.h"

namespace peloton {
namespace stats {
class TupleAccessRawData: public AbstractRawData {
 public:
  inline void LogTupleRead(txn_id_t txn_id) {
    tuple_access_counters_[txn_id]++;
  }

  inline void Aggregate(AbstractRawData &other) override {
    auto &other_db_metric = dynamic_cast<TupleAccessRawData &>(other);
    for (auto &entry : other_db_metric.tuple_access_counters_)
      tuple_access_counters_[entry.first] += entry.second;
  }

  void WriteToCatalog() override {

  }

  // TODO(Tianyu): Pretty Print
  const std::string GetInfo() const override { return "TupleAccessRawData"; };
 private:
  std::unordered_map<txn_id_t, uint64_t> tuple_access_counters_;
  std::vector<txn_id_t> commits_;
};

class TupleAccessMetric : public AbstractMetric<AbstractRawData> {
 public:
  void OnTransactionCommit(const concurrency::TransactionContext *context,
                           oid_t oid) override {
    Metric::OnTransactionCommit(context, oid);
  }

  void OnTransactionAbort(const concurrency::TransactionContext *context,
                          oid_t oid) override {
    Metric::OnTransactionAbort(context, oid);
  }

  void OnTupleRead(const concurrency::TransactionContext *context,
                   oid_t oid) override {
    Metric::OnTupleRead(context, oid);
  }
};
} // namespace stats
} // namespace peloton