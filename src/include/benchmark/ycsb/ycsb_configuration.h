//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_configuration.h
//
// Identification: src/include/benchmark/ycsb/ycsb_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <cstring>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "common/internal_types.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

static const oid_t ycsb_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

static const oid_t ycsb_field_length = 100;

class configuration {
 public:

  // index type
  IndexType index;

  // epoch type
  EpochType epoch;

  // size of the table
  int scale_factor;

  // execution duration (in s)
  double duration;

  // profile duration (in s)
  double profile_duration;

  // number of backends
  int backend_count;

  // column count
  int column_count;

  // operation count in a transaction
  int operation_count;

  // update ratio
  double update_ratio;

  // contention level
  double zipf_theta;

  // exponential backoff
  bool exp_backoff;

  // store strings
  bool string_mode;

  // garbage collection
  bool gc_mode;

  // number of gc threads
  int gc_backend_count;

  // number of loaders
  int loader_count;

  // throughput
  double throughput = 0;

  // abort rate
  double abort_rate = 0;

  std::vector<double> profile_throughput;

  std::vector<double> profile_abort_rate;

  std::vector<int> profile_memory;

};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateIndex(const configuration &state);

void ValidateScaleFactor(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateProfileDuration(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateColumnCount(const configuration &state);

void ValidateOperationCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateZipfTheta(const configuration &state);

void ValidateGCBackendCount(const configuration &state);

void WriteOutput();

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
