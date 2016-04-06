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

namespace peloton {
namespace benchmark {
namespace tpcc {

extern configuration state;

void CreateTPCCDatabase();

void LoadTPCCDatabase();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
