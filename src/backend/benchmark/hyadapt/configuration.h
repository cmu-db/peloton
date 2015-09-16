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

namespace peloton {
namespace benchmark {
namespace hyadapt{

class configuration {
 public:

  int num_keys;
  int num_txns;

};

void usage_exit(FILE *out);

void parse_arguments(int argc, char* argv[], configuration& state);

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
