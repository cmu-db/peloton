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

#include "backend/benchmark/tpcc/configuration.h"

namespace peloton {
namespace benchmark {
namespace tpcc{

extern configuration state;

extern storage::DataTable *user_table;

std::vector<catalog::Column> GetColumns();

void CreateAndLoadTable(LayoutType layout_type);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
