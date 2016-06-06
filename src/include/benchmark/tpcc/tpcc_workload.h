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

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace tpcc {

extern configuration state;

void RunWorkload();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
