//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/hyadapt/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/hyadapt/configuration.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

extern configuration state;

storage::DataTable *CreateAndLoadTable();

void RunDirectTest(storage::DataTable *table);

void RunAggregateTest(storage::DataTable *table);

void RunArithmeticTest(storage::DataTable *table);

void RunProjectivityExperiment();

void RunSelectivityExperiment();

void RunOperatorExperiment();

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
