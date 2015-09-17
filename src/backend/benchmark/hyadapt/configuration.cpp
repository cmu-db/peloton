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
          "   -o --operator-type     :  Operator type \n"
          "   -k --scale-factor      :  Scale factor \n"
          "   -t --transactions      :  # of Transactions \n"
          "   -s --selectivity       :  Selectivity \n"
          "   -p --projectivity      :  Projectivity \n"
          "   -l --layout            :  Layout \n"
          );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    { "operator-type", optional_argument, NULL, 'o' },
    { "scale-factor", optional_argument, NULL, 'k' },
    { "selectivity", optional_argument, NULL, 's' },
    { "projectivity", optional_argument, NULL, 'p' },
    { "layout", optional_argument, NULL, 'l' },
    { "transactions", optional_argument, NULL, 't' },
    { NULL, 0, NULL, 0 }
};

void parse_arguments(int argc, char* argv[], configuration& state) {

  // Default Values
  state.operator_type = OPERATOR_TYPE_INVALID;

  state.scale_factor = 1.0;
  state.transactions = 1;
  state.selectivity = 1.0;
  state.projectivity = 1.0;

  state.layout = LAYOUT_ROW;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ho:k:s:p:l:t:", opts,
                        &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'o':
        state.operator_type  = (OperatorType) atoi(optarg);
        std::cout << "operator_type: " << state.operator_type << std::endl;
        break;
      case 'k':
        state.scale_factor  = atoi(optarg);
        std::cout << "scale_factor: " << state.scale_factor << std::endl;
        break;
      case 's':
        state.selectivity  = atof(optarg);
        std::cout << "selectivity: " << state.selectivity << std::endl;
        break;
      case 'p':
        state.projectivity  = atof(optarg);
        std::cout << "projectivity: " << state.projectivity << std::endl;
        break;
      case 'l':
        state.layout  = (LayoutType) atoi(optarg);
        std::cout << "layout: " << state.layout << std::endl;
        break;
      case 't':
        state.transactions  = atoi(optarg);
        std::cout << "transactions: " << state.transactions << std::endl;
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

