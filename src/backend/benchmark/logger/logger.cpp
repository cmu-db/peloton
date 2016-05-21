// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // logger.cpp
// //
// // Identification: src/backend/benchmark/logger/logger.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include <iostream>
// #include <fstream>

// #include "backend/benchmark/logger/logger_configuration.h"
// #include "backend/benchmark/logger/logger_workload.h"
// #include "backend/benchmark/ycsb/ycsb_configuration.h"
// #include "backend/benchmark/tpcc/tpcc_configuration.h"

// // Logging mode
// extern LoggingType peloton_logging_mode;

// extern size_t peloton_data_file_size;

// extern int64_t peloton_wait_timeout;

// namespace peloton {
// namespace benchmark {

// namespace ycsb {
// configuration state;
// }
// namespace tpcc {
// configuration state;
// }

// namespace logger {

// // Configuration
// configuration state;

// // Main Entry Point
// void RunBenchmark() {

//   // First, set the global peloton logging mode and pmem file size
//   peloton_logging_mode = state.logging_type;
//   peloton_data_file_size = state.data_file_size;
//   peloton_wait_timeout = state.wait_timeout;

//   //===--------------------------------------------------------------------===//
//   // WAL
//   //===--------------------------------------------------------------------===//
//   if (IsBasedOnWriteAheadLogging(peloton_logging_mode)) {
//     // Prepare a simple log file
//     PrepareLogFile();

//     // Reset data
//     ResetSystem();

//     // Do recovery
//     DoRecovery();

//   }
//   //===--------------------------------------------------------------------===//
//   // WBL
//   //===--------------------------------------------------------------------===//
//   else if (IsBasedOnWriteBehindLogging(peloton_logging_mode)) {
//     // Test a simple log process
//     PrepareLogFile();

//     // Do recovery
//     DoRecovery();
//   }

// }

// }  // namespace logger
// }  // namespace benchmark
// }  // namespace peloton

// int main(int argc, char **argv) {
//   peloton::benchmark::logger::ParseArguments(argc, argv,
//                                              peloton::benchmark::logger::state);

//   peloton::benchmark::logger::RunBenchmark();

//   return 0;
// }
