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

namespace storage{
class DataTable;
}

namespace benchmark {
namespace ycsb {

extern configuration state;

extern std::vector<storage::DataTable*> user_tables;

double RunWorkload();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
