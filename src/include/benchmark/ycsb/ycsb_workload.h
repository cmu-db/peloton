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

#include "benchmark/benchmark_common.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "storage/data_table.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace ycsb {

extern configuration state;

extern storage::DataTable* user_table;

void RunWorkload();

bool RunMixed(const size_t thread_id, ZipfDistribution &zipf, FastRandom &rng);

/////////////////////////////////////////////////////////

std::vector<std::vector<type::Value>> ExecuteRead(executor::AbstractExecutor* executor);

void ExecuteUpdate(executor::AbstractExecutor* executor);

void PinToCore(size_t core);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
