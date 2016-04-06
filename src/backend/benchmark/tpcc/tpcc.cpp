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

#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_workload.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat) {
  std::cout << "----------------------------------------------------------\n";
  std::cout << " "
      << state.scale_factor << " :: ";
  std::cout << stat << " tps\n";

  out << state.scale_factor << " ";
  out << stat << "\n";
  out.flush();
}

// Main Entry Point
void RunBenchmark() {

  // Create and load the user table
  CreateTPCCDatabase();

  //LoadtpccDatabase();
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
