//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/tpcc/configuration.h
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
namespace tpcc{

enum OperatorType{
  OPERATOR_TYPE_INVALID = 0,         /* invalid */

  OPERATOR_TYPE_NEW_ORDER = 1,
};

class configuration {
 public:

  OperatorType operator_type;

  LayoutType layout;

  // size of the table
  int scale_factor;

  // value length
  int value_length;

  int tuples_per_tilegroup;

  // # of times to run operator
  unsigned long transactions;

 };

void Usage(FILE *out);

void ParseArguments(int argc, char* argv[], configuration& state);

void GenerateSequence(oid_t column_count);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
