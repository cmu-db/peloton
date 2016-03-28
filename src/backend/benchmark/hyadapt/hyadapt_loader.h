//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyadapt_loader.h
//
// Identification: src/backend/benchmark/hyadapt/hyadapt_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/hyadapt/hyadapt_configuration.h"

namespace peloton {
namespace benchmark {
namespace hyadapt {

extern configuration state;

extern std::unique_ptr<storage::DataTable> hyadapt_table;

void CreateTable();

void LoadTable();

void CreateAndLoadTable(LayoutType layout_type);

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
