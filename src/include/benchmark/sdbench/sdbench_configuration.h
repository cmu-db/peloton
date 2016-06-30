//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_configuration.h
//
// Identification: src/include/benchmark/sdbench/sdbench_configuration.h
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

#include "storage/data_table.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

enum OperatorType {
  OPERATOR_TYPE_INVALID = 0, /* invalid */

  OPERATOR_TYPE_DIRECT = 1,
  OPERATOR_TYPE_INSERT = 2

};

enum ExperimentType {
  EXPERIMENT_TYPE_INVALID = 0,

  EXPERIMENT_TYPE_ADAPT = 1

};

extern int orig_scale_factor;

class configuration {
 public:
  // scan type
  HybridScanType hybrid_scan_type;

  OperatorType operator_type;

  // experiment
  ExperimentType experiment_type;

  // size of the table
  int scale_factor;

  int tuples_per_tilegroup;

  // tile group layout
  LayoutType layout_mode;

  double selectivity;

  double projectivity;

  // column count
  int column_count;

  // index count
  int index_count;

  // update ratio
  double write_ratio;

  // # of times to run operator
  unsigned long transactions;

  bool adapt;

  bool fsm;
};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void GenerateSequence(oid_t column_count);

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
