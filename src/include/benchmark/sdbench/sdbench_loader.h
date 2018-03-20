//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_loader.h
//
// Identification: src/include/benchmark/sdbench/sdbench_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/sdbench/sdbench_configuration.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

extern configuration state;

extern std::unique_ptr<storage::DataTable> sdbench_table;

void CreateTable(peloton::LayoutType layout_type);

void LoadTable();

void CreateAndLoadTable(LayoutType layout_type);

void DropIndexes();

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
