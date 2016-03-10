//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/logger/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/logger/logger_configuration.h"

namespace peloton {
namespace benchmark {
namespace logger {

extern configuration state;

void RunActiveExperiment();


}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
