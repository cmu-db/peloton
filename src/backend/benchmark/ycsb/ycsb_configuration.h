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

class configuration {
 public:

  // size of the table
  int scale_factor;

  // column count
  int column_count;

  // update ratio
  double update_ratio;

  // # of times to run operator
  unsigned long transaction_count;

  // number of backends
  int backend_count;

};

extern configuration state;

void Usage(FILE *out);

void ValidateScaleFactor(const configuration &state);

void ValidateColumnCount(const configuration &state);

void ValidateUpdateRatio(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateTransactionCount(const configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
