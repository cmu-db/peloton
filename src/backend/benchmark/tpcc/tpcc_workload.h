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

namespace peloton {

namespace storage{
class DataTable;
}

namespace benchmark {
namespace tpcc {

extern configuration state;

double RunWorkload();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
