//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/ycsb/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/ycsb/ycsb_configuration.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

extern configuration state;

void CreateTable();

void LoadTable();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
