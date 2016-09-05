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
#include <iomanip>

#include "common/logger.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_workload.h"

#include "gc/gc_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput() {

  oid_t total_profile_memory = 0;
  for (auto &entry : state.profile_memory) {
    total_profile_memory += entry;
  }

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %d %d %lf %lf :: %lf %lf %d",
           state.scale_factor,
           state.backend_count,
           state.column_count,
           state.operation_count,
           state.update_ratio,
           state.zipf_theta,
           state.throughput,
           state.abort_rate,
           total_profile_memory);

  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << state.column_count << " ";
  out << state.operation_count << " ";
  out << state.update_ratio << " ";
  out << state.zipf_theta << " ";
  out << state.throughput << " ";
  out << state.abort_rate << " ";
  out << total_profile_memory << "\n";

  for (size_t round_id = 0; round_id < state.profile_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.profile_duration * round_id << " - " << std::setw(3)
        << std::left << state.profile_duration * (round_id + 1)
        << " s]: " << state.profile_throughput[round_id] << " "
        << state.profile_abort_rate[round_id] << " "
        << state.profile_memory[round_id] << "\n";
  }
  out.flush();
  out.close();
}

// Main Entry Point
void RunBenchmark() {

  if (state.gc_mode == true) {
    gc::GCManagerFactory::Configure(state.gc_backend_count);
  }
  
  gc::GCManagerFactory::GetInstance().StartGC();

  // Create the database
  CreateYCSBDatabase();

  // Load the databases
  LoadYCSBDatabase();

  // Run the workload
  RunWorkload();
  
  gc::GCManagerFactory::GetInstance().StopGC();

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
