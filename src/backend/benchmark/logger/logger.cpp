//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logger.cpp
//
// Identification: benchmark/logger/logger.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "backend/benchmark/logger/logger.h"
#include "backend/benchmark/logger/logger_configuration.h"
#include "logger_workload.h"

namespace peloton {
namespace benchmark {
namespace logger {

configuration state;

// Main Entry Point
void RunBenchmark() {
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::logger::ParseArguments(
      argc, argv, peloton::benchmark::logger::state);

  peloton::benchmark::logger::RunBenchmark();

  return 0;
}
