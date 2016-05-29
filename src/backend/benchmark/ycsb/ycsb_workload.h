//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

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
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////

struct ReadPlans {
  
  executor::IndexScanExecutor* index_scan_executor_;

  void ResetState() {
    index_scan_executor_->ResetState();
  }

  void Cleanup() {
    delete index_scan_executor_;
    index_scan_executor_ = nullptr;
  }
};

ReadPlans PrepareReadPlan();

bool RunRead(ReadPlans &read_plans, ZipfDistribution &zipf);

/////////////////////////////////////////////////////////

struct UpdatePlans {

  executor::IndexScanExecutor* index_scan_executor_;
  executor::UpdateExecutor* update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    index_scan_executor_->SetContext(context);
    update_executor_->SetContext(context);
  }

  void ResetState() {
    index_scan_executor_->ResetState();
  }

  void Cleanup() {
    delete index_scan_executor_;
    index_scan_executor_ = nullptr;

    delete update_executor_;
    update_executor_ = nullptr;
  }
};

UpdatePlans PrepareUpdatePlan();

bool RunUpdate(UpdatePlans &update_plans, ZipfDistribution &zipf);


/////////////////////////////////////////////////////////

struct MixedPlans {

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

MixedPlans PrepareMixedPlan();

bool RunMixed(MixedPlans &mixed_plans, ZipfDistribution &zipf);


/////////////////////////////////////////////////////////


std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
