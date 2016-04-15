//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb.cpp
//
// Identification: benchmark/ycsb/ycsb.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "backend/common/logger.h"

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat) {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf %d %d :: %lf tps",
           state.update_ratio,
           state.scale_factor,
           state.column_count,
           stat);

  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << state.column_count << " ";
  out << stat << "\n";
  out.flush();
}

// Main Entry Point
void RunBenchmark() {

  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Run the workload
  auto stat = RunWorkload();

  WriteOutput(stat);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::ycsb::ParseArguments(
      argc, argv, peloton::benchmark::ycsb::state);

  peloton::benchmark::ycsb::RunBenchmark();

  return 0;
}
