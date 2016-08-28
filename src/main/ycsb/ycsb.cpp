//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb.cpp
//
// Identification: src/main/ycsb/ycsb.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//w

#include <iostream>
#include <fstream>

#include "common/logger.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_workload.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput() {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %d %d %lf %lf :: %lf %lf",
           state.scale_factor,
           state.backend_count,
           state.column_count,
           state.operation_count,
           state.update_ratio,
           state.zipf_theta,
           state.throughput,
           state.abort_rate);

  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << state.column_count << " ";
  out << state.operation_count << " ";
  out << state.update_ratio << " ";
  out << state.zipf_theta << " ";
  out << state.throughput << " ";
  out << state.abort_rate << "\n";
  for (size_t round_id = 0; round_id < state.profile_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.profile_duration * round_id << " - " << std::setw(3)
        << std::left << state.profile_duration * (round_id + 1)
        << " s]: " << state.profile_throughput[round_id] << " "
        << state.profile_abort_rate[round_id] << "\n";
  }
  out.flush();
}

// Main Entry Point
void RunBenchmark() {
  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Run the workload
  RunWorkload();

  // Emit throughput
  WriteOutput();
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::ycsb::ParseArguments(argc, argv,
                                           peloton::benchmark::ycsb::state);

  peloton::benchmark::ycsb::RunBenchmark();

  return 0;
}
