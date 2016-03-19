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

namespace peloton {
namespace benchmark {
namespace ycsb {

extern int orig_scale_factor;

class configuration {
 public:

  // size of the table
  int scale_factor;

  // column count
  int column_count;

  // update ratio
  double update_ratio;

  // # of times to run operator
  unsigned long transactions;
};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
