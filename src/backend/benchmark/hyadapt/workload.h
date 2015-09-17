//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/hyadapt/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>

namespace peloton {
namespace benchmark {
namespace hyadapt{

void RunDirectTest();

void RunAggregateTest();

void RunArithmeticTest();

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
