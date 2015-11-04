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

#include "backend/benchmark/ycsb/configuration.h"

namespace peloton {
namespace benchmark {
namespace ycsb{

extern configuration state;

extern storage::DataTable *user_table;

std::vector<catalog::Column> GetColumns();

void CreateAndLoadTable(LayoutType layout_type);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
