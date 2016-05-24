//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_configuration.h
//
// Identification: src/backend/benchmark/logger/logger_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

enum AsynchronousType {
  ASYNCHRONOUS_TYPE_INVALID = 0,

  ASYNCHRONOUS_TYPE_SYNC = 1,     // logging enabled + sync commits
  ASYNCHRONOUS_TYPE_ASYNC = 2,    // logging enabled + async commits
  ASYNCHRONOUS_TYPE_DISABLED = 3  // logging disabled
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
  int wait_timeout;

  // Benchmark type
  BenchmarkType benchmark_type;

  int replication_port;

  char *remote_endpoint;

  // clflush or clwb
  int flush_mode;

  // nvm latency
  int nvm_latency;

  // pcommit latency
  int pcommit_latency;

  // asynchronous_mode
  AsynchronousType asynchronous_mode;
};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
