//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tpcc.cpp
//
// Identification: benchmark/tpcc/tpcc.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <iomanip>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"

#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat) {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d :: %lf",
           state.scale_factor,
           state.backend_count,
           stat);

  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << stat << "\n";
  out.flush();
  out.close();
}

// Main Entry Point
void RunBenchmark() {

  // Create the database
  CreateTPCCDatabase();

  // Load the database
  LoadTPCCDatabase();

  // Run the workload
  RunWorkload();

  // Emit throughput
  WriteOutput(state.throughput);
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::tpcc::ParseArguments(
      argc, argv, peloton::benchmark::tpcc::state);

  peloton::benchmark::tpcc::RunBenchmark();

  return 0;
}
