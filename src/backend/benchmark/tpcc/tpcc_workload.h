//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/tpcc/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/executor/abstract_executor.h"

namespace peloton {

namespace storage{
class DataTable;
}

namespace benchmark {
namespace tpcc {

extern configuration state;

void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

bool RunStockLevel();

bool RunDelivery();

bool RunOrderStatus();

bool RunPayment();

// void PrepareNewOrderPlans();

bool RunNewOrder();

std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton