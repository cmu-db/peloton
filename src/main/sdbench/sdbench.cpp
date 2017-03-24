//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench.cpp
//
// Identification: src/main/sdbench/sdbench.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "benchmark/sdbench/sdbench_configuration.h"
#include "common/logger.h"
#include "benchmark/sdbench/sdbench_workload.h"
#include "benchmark/sdbench/sdbench_loader.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {
namespace benchmark {
namespace sdbench {

configuration state;

// Main Entry Point
void RunBenchmark() {

  if (state.multi_stage) {
    // Run holistic indexing comparison benchmark
    RunMultiStageBenchmark();
  } else {
    // Run a single sdbench test
    RunSDBenchTest();
  }

}

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::sdbench::ParseArguments(
      argc, argv, peloton::benchmark::sdbench::state);

  peloton::benchmark::sdbench::RunBenchmark();

  peloton::benchmark::sdbench::sdbench_table.release();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}
