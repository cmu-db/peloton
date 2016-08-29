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
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "common/types.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

static const oid_t ycsb_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

static const oid_t ycsb_field_length = 100;

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

  // latency average
  double latency;

  // # of transaction
  int transaction_count;

  // store ints
  bool ints_mode;
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void ValidateColumnCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateTransactionCount(const configuration &state);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
