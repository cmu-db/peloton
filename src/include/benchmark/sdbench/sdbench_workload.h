//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_workload.h
//
// Identification: src/include/benchmark/sdbench/sdbench_workload.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/sdbench/sdbench_configuration.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

extern configuration state;

void RunSDBenchTest();
void RunMultiStageBenchmark();

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
