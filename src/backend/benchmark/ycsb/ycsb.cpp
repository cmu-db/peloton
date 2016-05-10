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

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat) {
  std::cout << "----------------------------------------------------------\n";
  std::cout << state.update_ratio << " "
      << state.scale_factor << " "
      << state.backend_count << " "
      << state.skew_factor << " "
      << state.column_count << " :: ";
  std::cout << stat << " tps\n";

  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << state.skew_factor << " ";
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
  RunWorkload();

  WriteOutput(state.throughput);
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
