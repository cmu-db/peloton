//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/storage/data_table.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace ycsb {

extern configuration state;

extern storage::DataTable* user_table;

void RunWorkload();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
