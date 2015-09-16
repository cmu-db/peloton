//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/hyadapt/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/hyadapt/configuration.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

void usage_exit(FILE *out) {
  fprintf(out, "Command line options : hyadapt <options> \n"
          "   -h --help              :  Print help message \n"
          "   -x --num-txns          :  Number of transactions \n"
          "   -k --num-keys          :  Number of keys \n"
          );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    { "num-txns", optional_argument, NULL, 'x' },
    { "num-keys", optional_argument, NULL, 'k' },
    { NULL, 0, NULL, 0 }
};

void parse_arguments(int argc, char* argv[], configuration& state) {

  // Default Values
  state.num_keys = 10;
  state.num_txns = 10;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:x:k:e:p:g:q:b:j:svwascmhludytzori", opts,
                        &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'x':
        state.num_txns = atoi(optarg);
        std::cout << "num_txns: " << state.num_txns << std::endl;
        break;
      case 'k':
        state.num_keys = atoi(optarg);
        std::cout << "num_keys: " << state.num_keys << std::endl;
        break;

      case 'h':
        usage_exit(stderr);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
    }
  }
}



}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton

