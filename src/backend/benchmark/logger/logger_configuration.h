//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/logger/configuration.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/pool.h"

namespace peloton {
namespace benchmark {
namespace logger {

enum ExperimentType {
  EXPERIMENT_TYPE_INVALID = 0,

  EXPERIMENT_TYPE_THROUGHPUT = 1,
  EXPERIMENT_TYPE_RECOVERY = 2,
  EXPERIMENT_TYPE_STORAGE = 3,
  EXPERIMENT_TYPE_LATENCY = 4
};

enum BenchmarkType {
  BENCHMARK_TYPE_INVALID = 0,

  BENCHMARK_TYPE_YCSB = 1,
  BENCHMARK_TYPE_TPCC = 2
};

class configuration {
 public:
  // experiment type
  ExperimentType experiment_type;

  // logging type
  LoggingType logging_type;

  // log file dir
  std::string log_file_dir;

  // size of the pmem file (in MB)
  size_t data_file_size;

  // frequency with which the logger flushes
  int64_t wait_timeout;

  // Benchmark type
  BenchmarkType benchmark_type;

  // # of times to run transaction
  unsigned long transaction_count;

  // clflush or clwb
  int flush_mode;

  // nvm latency
  int64_t nvm_latency;

  // pcommit latency
  int64_t pcommit_latency;

  // asynchronous_mode
  int64_t asynchronous_mode;

};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
