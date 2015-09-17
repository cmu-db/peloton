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

void Usage(FILE *out) {
  fprintf(out, "Command line options : hyadapt <options> \n"
          "   -h --help              :  Print help message \n"
          "   -o --operator-type     :  Operator type \n"
          "   -k --scale-factor      :  Scale factor \n"
          "   -s --selectivity       :  Selectivity \n"
          "   -p --projectivity      :  Projectivity \n"
          "   -l --layout            :  Layout \n"
          "   -t --transactions      :  # of Transactions \n"
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

static void ValidateOperator(const configuration& state) {
  if(state.operator_type < 1 || state.operator_type > 3) {
    std::cout << "Invalid operator type :: " << state.layout << "\n";
    exit(EXIT_FAILURE);
  }
  else {
    switch(state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        std::cout << "operator_type: " << "DIRECT" << std::endl;
        break;
      case OPERATOR_TYPE_AGGREGATE:
        std::cout << "operator_type: " << "AGGREGATE" << std::endl;
        break;
      case OPERATOR_TYPE_ARITHMETIC:
        std::cout << "operator_type: " << "ARITHMETIC" << std::endl;
        break;
      default:
        break;
    }
  }
}

static void ValidateLayout(const configuration& state) {
  if(state.layout < 0 || state.layout > 2) {
    std::cout << "Invalid layout :: " << state.layout << "\n";
    exit(EXIT_FAILURE);
  }
  else {
    switch(state.layout) {
      case LAYOUT_ROW:
        std::cout << "layout: " << "ROW" << std::endl;
        break;
      case LAYOUT_COLUMN:
        std::cout << "layout: " << "COLUMN" << std::endl;
        break;
      case LAYOUT_HYBRID:
        std::cout << "layout: " << "HYBRID" << std::endl;
        break;
      default:
        break;
    }
  }
}

static void ValidateProjectivity(const configuration& state) {
  if(state.projectivity < 0 || state.projectivity > 1) {
    std::cout << "Invalid projectivity :: " <<  state.projectivity << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << "projectivity: " << state.projectivity << std::endl;
}

static void ValidateSelectivity(const configuration& state) {
  if(state.selectivity < 0 || state.selectivity > 1) {
    std::cout << "Invalid selectivity :: " <<  state.selectivity << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << "selectivity: " << state.selectivity << std::endl;
}

void ParseArguments(int argc, char* argv[], configuration& state) {

  // Default Values
  state.operator_type = OPERATOR_TYPE_INVALID;

  state.scale_factor = 100.0;
  state.transactions = 1;
  state.selectivity = 1.0;
  state.projectivity = 1.0;

  state.column_count = 250;
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
        ValidateOperator(state);
        break;
      case 'k':
        state.scale_factor  = atoi(optarg);
        std::cout << "scale_factor: " << state.scale_factor << std::endl;
        break;
      case 's':
        state.selectivity  = atof(optarg);
        ValidateSelectivity(state);
        break;
      case 'p':
        state.projectivity  = atof(optarg);
        ValidateProjectivity(state);
        break;
      case 'l':
        state.layout  = (LayoutType) atoi(optarg);
        ValidateLayout(state);
        break;
      case 't':
        state.transactions  = atoi(optarg);
        std::cout << "transactions: " << state.transactions << std::endl;
        break;

      case 'h':
        Usage(stderr);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
    }
  }
}



}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton

