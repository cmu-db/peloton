//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_loader.h
//
// Identification: src/include/benchmark/ycsb/ycsb_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark/ycsb/ycsb_configuration.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

extern configuration state;

void CreateYCSBDatabase();

void LoadYCSBDatabase();
void LoadYCSBRows(const int begin_rowid, const int end_rowid);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
