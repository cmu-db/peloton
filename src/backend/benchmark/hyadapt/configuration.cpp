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
          "   -k --scale-factor      :  # of tuples \n"
          "   -s --selectivity       :  Selectivity \n"
          "   -p --projectivity      :  Projectivity \n"
          "   -l --layout            :  Layout \n"
          "   -t --transactions      :  # of transactions \n"
          "   -e --experiment_type   :  Experiment Type \n"
          "   -c --column_count      :  # of columns \n"
          "   -w --write_ratio       :  Fraction of writes \n"
          "   -g --tuples_per_tg     :  # of tuples per tilegroup \n"
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
    { "experiment-type", optional_argument, NULL, 'e' },
    { "column_count", optional_argument, NULL, 'c' },
    { "write_ratio", optional_argument, NULL, 'w' },
    { "tuples_per_tg", optional_argument, NULL, 'g' },
    { NULL, 0, NULL, 0 }
};

void GenerateSequence(oid_t column_count){
  // Reset sequence
  hyadapt_column_ids.clear();

  // Generate sequence
  for(oid_t column_id = 1; column_id <= column_count; column_id++)
    hyadapt_column_ids.push_back(column_id);

  std::random_shuffle(hyadapt_column_ids.begin(), hyadapt_column_ids.end());
}

static void ValidateOperator(const configuration& state) {
  if(state.operator_type < 1 || state.operator_type > 3) {
    std::cout << "Invalid operator type :: " << state.operator_type << "\n";
    exit(EXIT_FAILURE);
  }
  else {
    switch(state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        std::cout << std::setw(20) << std::left << "operator_type " << " : " << "DIRECT" << std::endl;
        break;
      case OPERATOR_TYPE_AGGREGATE:
        std::cout << std::setw(20) << std::left << "operator_type " << " : "  << "AGGREGATE" << std::endl;
        break;
      case OPERATOR_TYPE_ARITHMETIC:
        std::cout << std::setw(20) << std::left << "operator_type " << " : " << "ARITHMETIC" << std::endl;
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

static void ValidateProjectivity(const configuration& state) {
  if(state.projectivity < 0 || state.projectivity > 1) {
    std::cout << "Invalid projectivity :: " <<  state.projectivity << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "projectivity " << " : " << state.projectivity << std::endl;
}

static void ValidateSelectivity(const configuration& state) {
  if(state.selectivity < 0 || state.selectivity > 1) {
    std::cout << "Invalid selectivity :: " <<  state.selectivity << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "selectivity " << " : " << state.selectivity << std::endl;
}

static void ValidateExperiment(const configuration& state) {
  if(state.experiment_type <= 0 || state.experiment_type > 4) {
    std::cout << "Invalid experiment_type :: " <<  state.experiment_type << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "experiment_type " << " : " << state.experiment_type << std::endl;
}

static void ValidateColumnCount(const configuration& state) {
  if(state.column_count <= 0) {
    std::cout << "Invalid attribute_count :: " <<  state.column_count << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "attribute_count " << " : " << state.column_count << std::endl;
}

static void ValidateWriteRatio(const configuration& state) {
  if(state.write_ratio < 0 || state.write_ratio > 1) {
    std::cout << "Invalid write_ratio :: " <<  state.write_ratio << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "write_ratio " << " : " << state.write_ratio << std::endl;
}

static void ValidateTuplesPerTileGroup(const configuration& state) {
  if(state.tuples_per_tilegroup <= 0) {
    std::cout << "Invalid tuples_per_tilegroup :: " <<  state.tuples_per_tilegroup << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "tuples_per_tgroup " << " : " << state.tuples_per_tilegroup << std::endl;
}

int orig_scale_factor;

void ParseArguments(int argc, char* argv[], configuration& state) {

  // Default Values
  state.operator_type = OPERATOR_TYPE_INVALID;

  state.scale_factor = 100.0;
  state.tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;

  state.transactions = 1;
  state.selectivity = 1.0;
  state.projectivity = 1.0;

  state.layout = LAYOUT_ROW;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;

  state.column_count = 100;
  state.write_ratio = 0.0;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "aho:k:s:p:l:t:e:c:w:g:", opts,
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
      case 's':
        state.selectivity  = atof(optarg);
        break;
      case 'p':
        state.projectivity  = atof(optarg);
        break;
      case 'l':
        state.layout  = (LayoutType) atoi(optarg);
        break;
      case 't':
        state.transactions  = atoi(optarg);
        break;
      case 'e':
        state.experiment_type = (ExperimentType) atoi(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'w':
        state.write_ratio = atof(optarg);
        break;
      case 'g':
        state.tuples_per_tilegroup = atoi(optarg);
        break;
      case 'h':
        Usage(stderr);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
    }
  }

  if(state.experiment_type == EXPERIMENT_TYPE_INVALID) {
    // Print configuration
    ValidateOperator(state);
    ValidateLayout(state);
    ValidateSelectivity(state);
    ValidateProjectivity(state);
    ValidateScaleFactor(state);
    ValidateColumnCount(state);
    ValidateWriteRatio(state);
    ValidateTuplesPerTileGroup(state);

    std::cout << std::setw(20) << std::left
        << "transactions " << " : " << state.transactions << std::endl;
  }
  else{
    ValidateExperiment(state);
  }

  // cache orig scale factor
  orig_scale_factor = state.scale_factor;

}



}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton

