//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_workload.h
//
// Identification: src/include/benchmark/sdbench/sdbench_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/sdbench/sdbench_configuration.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

extern configuration state;

void CreateAndLoadTable(LayoutType layout_type);

void RunDirectTest();

void RunAdaptExperiment();

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
