//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/tpcc/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/tpcc/configuration.h"

namespace peloton {
namespace benchmark {
namespace tpcc{

void Usage(FILE *out) {
  fprintf(out, "Command line options : tpcc <options> \n"
          "   -h --help              :  Print help message \n"
          "   -o --operator-type     :  Operator type \n"
          "   -k --scale-factor      :  # of warehouses \n"
          "   -l --layout            :  Layout \n"
          "   -t --transactions      :  # of transactions \n"
          "   -g --tuples_per_tg     :  # of tuples per tilegroup \n"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    { "operator-type", optional_argument, NULL, 'o' },
    { "scale-factor", optional_argument, NULL, 'k' },
    { "layout", optional_argument, NULL, 'l' },
    { "transactions", optional_argument, NULL, 't' },
    { "tuples_per_tg", optional_argument, NULL, 'g' },
    { NULL, 0, NULL, 0 }
};

static void ValidateOperator(const configuration& state) {
  if(state.operator_type < 1 || state.operator_type > 2) {
    std::cout << "Invalid operator type :: " << state.operator_type << "\n";
    exit(EXIT_FAILURE);
  }
  else {
    switch(state.operator_type) {
      case OPERATOR_TYPE_NEW_ORDER:
        std::cout << std::setw(20) << std::left << "operator_type " << " : " << "NEW_ORDER" << std::endl;
        break;
      default:
        break;
    }
  }
}

static void ValidateScaleFactor(const configuration& state) {
  if(state.scale_factor <= 0) {
    std::cout << "Invalid scalefactor :: " <<  state.scale_factor << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left
      << "scale_factor " << " : " << state.scale_factor << std::endl;

}

static void ValidateLayout(const configuration& state) {
  if(state.layout < 0 || state.layout > 2) {
    std::cout << "Invalid layout :: " << state.layout << "\n";
    exit(EXIT_FAILURE);
  }
  else {
    switch(state.layout) {
      case LAYOUT_ROW:
        std::cout << std::setw(20) << std::left << "layout " << " : " << "ROW" << std::endl;
        break;
      case LAYOUT_COLUMN:
        std::cout << std::setw(20) << std::left << "layout " << " : " << "COLUMN" << std::endl;
        break;
      case LAYOUT_HYBRID:
        std::cout << std::setw(20) << std::left << "layout " << " : " << "HYBRID" << std::endl;
        break;
      default:
        break;
    }
  }
}

void ParseArguments(int argc, char* argv[], configuration& state) {

  // Default Values
  state.operator_type = OPERATOR_TYPE_INVALID;

  state.scale_factor = 1;

  state.transactions = 1;

  state.layout = LAYOUT_ROW;

  state.value_length = 100;

  state.tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "aho:k:l:t:", opts,
                        &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'o':
        state.operator_type  = (OperatorType) atoi(optarg);
        break;
      case 'k':
        state.scale_factor  = atoi(optarg);
        break;
      case 'l':
        state.layout  = (LayoutType) atoi(optarg);
        break;
      case 't':
        state.transactions  = atoi(optarg);
        break;
      case 'h':
        Usage(stderr);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
    }
  }

  // Print configuration
  ValidateOperator(state);
  ValidateLayout(state);
  ValidateScaleFactor(state);

  std::cout << std::setw(20) << std::left
      << "transactions " << " : " << state.transactions << std::endl;

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

