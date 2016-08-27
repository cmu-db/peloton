//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/include/benchmark/ycsb/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/ycsb/ycsb_configuration.h"
#include "storage/data_table.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace ycsb {

extern configuration state;

extern storage::DataTable* user_table;

void RunWorkload();

/////////////////////////////////////////////////////////

struct MixedPlan {

  executor::IndexScanExecutor* index_scan_executor_;

  executor::IndexScanExecutor* update_index_scan_executor_;
  executor::UpdateExecutor* update_executor_;


  void SetContext(executor::ExecutorContext* context) {
    index_scan_executor_->SetContext(context);

    update_index_scan_executor_->SetContext(context);
    update_executor_->SetContext(context);
  }

  // in a mixed transaction, an executor is reused for several times.
  // so we must reset the state every time we use it.

  void Cleanup() {
    delete index_scan_executor_;
    index_scan_executor_ = nullptr;

    delete update_index_scan_executor_;
    update_index_scan_executor_ = nullptr;

    delete update_executor_;
    update_executor_ = nullptr;
  }
};

MixedPlan PrepareMixedPlan();

bool RunMixed(MixedPlan &mixed_plan, ZipfDistribution &zipf, fast_random &rng, double update_ratio, int operation_count);

/////////////////////////////////////////////////////////


std::vector<std::vector<Value>>
ExecuteRead(executor::AbstractExecutor* executor);

void ExecuteUpdate(executor::AbstractExecutor* executor);


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
