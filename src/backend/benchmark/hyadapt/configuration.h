//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/hyadapt/configuration.h
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

#include "backend/storage/data_table.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

enum OperatorType{
  OPERATOR_TYPE_INVALID = 0,         /* invalid */

  OPERATOR_TYPE_DIRECT = 1,          /* direct */
  OPERATOR_TYPE_AGGREGATE = 2,       /* aggregate */
  OPERATOR_TYPE_ARITHMETIC = 3       /* arithmetic */
};

enum ExperimentType{
  EXPERIMENT_TYPE_INVALID = 0,

  EXPERIMENT_TYPE_PROJECTIVITY = 1,
  EXPERIMENT_TYPE_SELECTIVITY = 2,
  EXPERIMENT_TYPE_OPERATOR = 3

};

class configuration {
 public:

  OperatorType operator_type;

  // experiment
  ExperimentType experiment_type;

  // size of the table
  int scale_factor;

  // tile group layout
  LayoutType layout;

  double selectivity;

  double projectivity;

  // column count
  int column_count;

  // # of times to run operator
  int transactions;

 };

void Usage(FILE *out);

void ParseArguments(int argc, char* argv[], configuration& state);

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
