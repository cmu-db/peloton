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

// Main Entry Point
void RunBenchmark() {

  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Run the workload
  RunWorkload();

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
