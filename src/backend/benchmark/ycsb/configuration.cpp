//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/ycsb/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/ycsb/configuration.h"

namespace peloton {
namespace benchmark {
namespace ycsb{

void Usage(FILE *out) {
  fprintf(out, "Command line options : ycsb <options> \n"
          "   -h --help              :  Print help message \n"
          "   -o --operator-type     :  Operator type \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -l --layout            :  Layout \n"
          "   -t --transactions      :  # of transactions \n"
          "   -c --column_count      :  # of columns \n"
          "   -g --tuples_per_tg     :  # of tuples per tilegroup \n"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    { "operator-type", optional_argument, NULL, 'o' },
    { "scale-factor", optional_argument, NULL, 'k' },
    { "layout", optional_argument, NULL, 'l' },
    { "transactions", optional_argument, NULL, 't' },
    { "column_count", optional_argument, NULL, 'c' },
    { "tuples_per_tg", optional_argument, NULL, 'g' },
    { NULL, 0, NULL, 0 }
};

static void ValidateOperator(const configuration& state) {
  if(state.operator_type < 1 || state.operator_type > 3) {
    std::cout << "Invalid operator type :: " << state.operator_type << "\n";
    exit(EXIT_FAILURE);
  }
  else {
    switch(state.operator_type) {
      case OPERATOR_TYPE_READ:
        std::cout << std::setw(20) << std::left << "operator_type " << " : " << "READ" << std::endl;
        break;
      case OPERATOR_TYPE_UPDATE:
        std::cout << std::setw(20) << std::left << "operator_type " << " : "  << "UPDATE" << std::endl;
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

static void ValidateColumnCount(const configuration& state) {
  if(state.column_count <= 0) {
    std::cout << "Invalid column_count :: " <<  state.column_count << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "attribute_count " << " : " << state.column_count << std::endl;
}

static void ValidateTuplesPerTileGroup(const configuration& state) {
  if(state.tuples_per_tilegroup <= 0) {
    std::cout << "Invalid tuples_per_tilegroup :: " <<  state.tuples_per_tilegroup << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "tuples_per_tgroup " << " : " << state.tuples_per_tilegroup << std::endl;
}

void ParseArguments(int argc, char* argv[], configuration& state) {

  // Default Values
  state.operator_type = OPERATOR_TYPE_INVALID;

  state.scale_factor = 100.0;

  state.transactions = 1;

  state.layout = LAYOUT_ROW;

  state.value_length = 100;

  state.column_count = 10;
  state.tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "aho:k:l:t:c:", opts,
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
      case 'c':
        state.column_count = atoi(optarg);
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
  ValidateColumnCount(state);

  std::cout << std::setw(20) << std::left
      << "transactions " << " : " << state.transactions << std::endl;

}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

