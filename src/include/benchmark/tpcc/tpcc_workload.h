//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_workload.h
//
// Identification: src/include/benchmark/tpcc/tpcc_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/macros.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace tpcc {

extern configuration state;

void RunWorkload();

bool RunNewOrder(const size_t &thread_id);

bool RunPayment(const size_t &thread_id);

bool RunDelivery(const size_t &thread_id);

bool RunOrderStatus(const size_t &thread_id);

bool RunStockLevel(const size_t &thread_id);

size_t GenerateWarehouseId(const size_t &thread_id);

/////////////////////////////////////////////////////////

std::vector<std::vector<common::Value>> ExecuteRead(executor::AbstractExecutor* executor);

void ExecuteUpdate(executor::AbstractExecutor* executor);

void ExecuteDelete(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
