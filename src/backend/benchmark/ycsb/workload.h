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

void CreateAndLoadTable(LayoutType layout_type);

void RunRead();

void RunScan();

void RunInsert();

void RunUpdate();

void RunDelete();

void RunReadModifyWrite();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
