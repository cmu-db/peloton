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

  EXPERIMENT_TYPE_ACTIVE = 1,
  EXPERIMENT_TYPE_RECOVERY = 2,
  EXPERIMENT_TYPE_STORAGE = 3,
  EXPERIMENT_TYPE_WAIT = 4
};

extern int orig_scale_factor;

class configuration {
 public:
  // experiment type
  ExperimentType experiment_type;

  // logging type
  LoggingType logging_type;

  // # of tuples
  int tuple_count;

  // # of backends (i.e. backend loggers)
  int backend_count;

  // # of columns in each tuple
  oid_t column_count;

  // check if the count matches after recovery
  bool check_tuple_count;

  // log file dir
  std::string log_file_dir;

  // size of the pmem file (in MB)
  size_t data_file_size;

  // frequency with which the logger flushes
  int64_t wait_timeout;
};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
