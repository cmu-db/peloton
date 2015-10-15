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

storage::DataTable *CreateAndLoadTable(LayoutType layout_type);

void RunRead(storage::DataTable *table);

void RunScan(storage::DataTable *table);

void RunInsert(storage::DataTable *table);

void RunUpdate(storage::DataTable *table);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
