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

#include "backend/storage/data_table.h"

namespace peloton {
namespace benchmark {
namespace ycsb{

enum OperatorType{
  OPERATOR_TYPE_INVALID = 0,         /* invalid */

  OPERATOR_TYPE_READ = 1,
  OPERATOR_TYPE_UPDATE = 2
};

class configuration {
 public:

  OperatorType operator_type;

  LayoutType layout;

  // size of the table
  int scale_factor;

  // column count
  int column_count;

  // value length
  int value_length;

  int tuples_per_tilegroup;

  // # of times to run operator
  unsigned long transactions;

 };

void Usage(FILE *out);

void ParseArguments(int argc, char* argv[], configuration& state);

void GenerateSequence(oid_t column_count);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
