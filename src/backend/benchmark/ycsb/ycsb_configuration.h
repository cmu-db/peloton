//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/ycsb/configuration.h
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

namespace peloton {
namespace benchmark {
namespace ycsb {

static const oid_t ycsb_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

static const oid_t ycsb_field_length = 100;

enum SkewFactor {
  SKEW_FACTOR_INVALID = 0,

  SKEW_FACTOR_LOW = 1,
  SKEW_FACTOR_HIGH = 2
};

class configuration {
 public:
  // size of the table
  int scale_factor;

  // column count
  int column_count;

  // update ratio
  double update_ratio;

  // execution duration (in ms)
  int duration;

  // number of backends
  int backend_count;

  // throughput
  double throughput;

  // skew
  SkewFactor skew_factor;

  // latency average
  double latency;
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void ValidateColumnCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateSkewFactor(const configuration &state);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
