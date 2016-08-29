//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger.cpp
//
// Identification: src/main/logger/logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iostream>
#include <fstream>

#include "benchmark/logger/logger_configuration.h"
#include "benchmark/logger/logger_workload.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/tpcc/tpcc_configuration.h"

// Logging mode
extern LoggingType peloton_logging_mode;

extern CheckpointType peloton_checkpoint_mode;

extern size_t peloton_data_file_size;

extern int64_t peloton_wait_timeout;

// Flush mode (for NVM WBL)
extern int peloton_flush_mode;

// PCOMMIT latency (for NVM WBL)
extern int peloton_pcommit_latency;

namespace peloton {
namespace benchmark {

namespace ycsb {
configuration state;
void CreateYCSBDatabase();
}
namespace tpcc {
configuration state;
}

namespace logger {

void StartLogging(std::thread &thread);

// Configuration
configuration state;

// Main Entry Point
void RunBenchmark() {
  // First, set the global peloton logging mode and pmem file size
  peloton_logging_mode = state.logging_type;
  peloton_data_file_size = state.data_file_size;
  peloton_wait_timeout = state.wait_timeout;
  peloton_flush_mode = state.flush_mode;
  peloton_pcommit_latency = state.pcommit_latency;

  //===--------------------------------------------------------------------===//
  // WAL
  //===--------------------------------------------------------------------===//
  if (IsBasedOnWriteAheadLogging(peloton_logging_mode)) {
    // Prepare a simple log file
    PrepareLogFile();

    // Reset data
    ResetSystem();

    // Do recovery
    DoRecovery();
  }
  //===--------------------------------------------------------------------===//
  // WBL
  //===--------------------------------------------------------------------===//
  else if (IsBasedOnWriteBehindLogging(peloton_logging_mode)) {
    // Test a simple log process
    PrepareLogFile();

    // Do recovery
    DoRecovery();
  }
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::logger::ParseArguments(argc, argv,
                                             peloton::benchmark::logger::state);

  peloton::benchmark::logger::RunBenchmark();

  return 0;
}
