//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_configuration.h
//
// Identification: src/benchmark/sdbench/sdbench_configuration.h
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
  OPERATOR_TYPE_AGGREGATE = 2,
  OPERATOR_TYPE_ARITHMETIC = 3,
  OPERATOR_TYPE_JOIN = 4,
  OPERATOR_TYPE_INSERT = 5,
  OPERATOR_TYPE_UPDATE = 6

};

enum ExperimentType {
  EXPERIMENT_TYPE_INVALID = 0,

  EXPERIMENT_TYPE_PROJECTIVITY = 1,
  EXPERIMENT_TYPE_SELECTIVITY = 2,
  EXPERIMENT_TYPE_OPERATOR = 3,
  EXPERIMENT_TYPE_ADAPT = 6,

};

extern int orig_scale_factor;

class configuration {
 public:
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
