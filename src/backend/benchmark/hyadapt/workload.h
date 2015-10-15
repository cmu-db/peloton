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

extern storage::DataTable *hyadapt_table;

void CreateAndLoadTable(LayoutType layout_type);

void RunDirectTest();

void RunAggregateTest();

void RunArithmeticTest();

void RunProjectivityExperiment();

void RunSelectivityExperiment();

void RunOperatorExperiment();

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
