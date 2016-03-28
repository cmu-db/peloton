//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyadapt_configuration.cpp
//
// Identification: src/backend/benchmark/hyadapt/hyadapt_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#undef NDEBUG

#include <iomanip>
#include <algorithm>

#include "backend/benchmark/hyadapt/hyadapt_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace hyadapt {

void Usage() {
  LOG_INFO("Command line options : hyadapt <options>"
          "   -h --help              :  Print help message"
          "   -o --operator-type     :  Operator type"
          "   -k --scale-factor      :  # of tuples"
          "   -s --selectivity       :  Selectivity"
          "   -p --projectivity      :  Projectivity"
          "   -l --layout            :  Layout"
          "   -t --transactions      :  # of transactions"
          "   -e --experiment_type   :  Experiment Type"
          "   -c --column_count      :  # of columns"
          "   -w --write_ratio       :  Fraction of writes"
          "   -g --tuples_per_tg     :  # of tuples per tilegroup");
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"operator-type", optional_argument, NULL, 'o'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"selectivity", optional_argument, NULL, 's'},
    {"projectivity", optional_argument, NULL, 'p'},
    {"layout", optional_argument, NULL, 'l'},
    {"transactions", optional_argument, NULL, 't'},
    {"experiment-type", optional_argument, NULL, 'e'},
    {"column_count", optional_argument, NULL, 'c'},
    {"write_ratio", optional_argument, NULL, 'w'},
    {"tuples_per_tg", optional_argument, NULL, 'g'},
    {NULL, 0, NULL, 0}};

void GenerateSequence(oid_t column_count) {
  // Reset sequence
  hyadapt_column_ids.clear();

  // Generate sequence
  for (oid_t column_id = 1; column_id <= column_count; column_id++)
    hyadapt_column_ids.push_back(column_id);

  std::random_shuffle(hyadapt_column_ids.begin(), hyadapt_column_ids.end());
}

static void ValidateOperator(const configuration &state) {
  if (state.operator_type < 1 || state.operator_type > 4) {
    LOG_ERROR("Invalid operator type :: %d", state.operator_type);
    exit(EXIT_FAILURE);
  } else {
    switch (state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        LOG_INFO("%s : DIRECT", "operator_type ");
        break;
      case OPERATOR_TYPE_AGGREGATE:
        LOG_INFO("%s : AGGREGATE", "operator_type ");
        break;
      case OPERATOR_TYPE_ARITHMETIC:
        LOG_INFO("%s : ARITHMETIC", "operator_type ");
        break;
      case OPERATOR_TYPE_JOIN:
        LOG_INFO("%s : JOIN", "operator_type ");
        break;
      default:
        break;
    }
  }
}

static void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
}

static void ValidateLayout(const configuration &state) {
  if (state.layout_mode < 0 || state.layout_mode > 2) {
    LOG_ERROR("Invalid layout :: %d", state.layout_mode);
    exit(EXIT_FAILURE);
  } else {
    switch (state.layout_mode) {
      case LAYOUT_ROW:
        LOG_INFO("%s : ROW", "layout ");
        break;
      case LAYOUT_COLUMN:
        LOG_INFO("%s : COLUMN", "layout ");
        break;
      case LAYOUT_HYBRID:
        LOG_INFO("%s : HYBRID", "layout ");
        break;
      default:
        break;
    }
  }
}

static void ValidateProjectivity(const configuration &state) {
  if (state.projectivity < 0 || state.projectivity > 1) {
    LOG_ERROR("Invalid projectivity :: %lf", state.projectivity);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "projectivity", state.projectivity);
}

static void ValidateSelectivity(const configuration &state) {
  if (state.selectivity < 0 || state.selectivity > 1) {
    LOG_ERROR("Invalid selectivity :: %lf", state.selectivity);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "selectivity", state.selectivity);
}

static void ValidateExperiment(const configuration &state) {
  if (state.experiment_type <= 0 || state.experiment_type > 14) {
    LOG_ERROR("Invalid experiment_type :: %d", state.experiment_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "experiment_type", state.experiment_type);
}

static void ValidateColumnCount(const configuration &state) {
  if (state.column_count <= 0) {
    LOG_ERROR("Invalid column_count :: %d", state.column_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "column_count", state.column_count);
}

static void ValidateWriteRatio(const configuration &state) {
  if (state.write_ratio < 0 || state.write_ratio > 1) {
    LOG_ERROR("Invalid write_ratio :: %lf", state.write_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "write_ratio", state.write_ratio);
}

static void ValidateTuplesPerTileGroup(const configuration &state) {
  if (state.tuples_per_tilegroup <= 0) {
    LOG_ERROR("Invalid tuples_per_tilegroup :: %d", state.tuples_per_tilegroup);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "tuples_per_tilegroup", state.tuples_per_tilegroup);
}

int orig_scale_factor;

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.operator_type = OPERATOR_TYPE_INVALID;

  state.scale_factor = 100.0;
  state.tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;

  state.transactions = 1;
  state.selectivity = 1.0;
  state.projectivity = 1.0;

  state.layout_mode = LAYOUT_ROW;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;

  state.column_count = 100;
  state.write_ratio = 0.0;

  state.access_num_groups = 1;
  state.subset_ratio = 1.0;
  state.subset_experiment_type = SUBSET_TYPE_INVALID;

  state.adapt = false;
  state.theta = 0.0;
  state.reorg = false;
  state.distribution = false;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "aho:k:s:p:l:t:e:c:w:g:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'o':
        state.operator_type = (OperatorType)atoi(optarg);
        break;
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 's':
        state.selectivity = atof(optarg);
        break;
      case 'p':
        state.projectivity = atof(optarg);
        break;
      case 'l':
        state.layout_mode = (LayoutType)atoi(optarg);
        break;
      case 't':
        state.transactions = atoi(optarg);
        break;
      case 'e':
        state.experiment_type = (ExperimentType)atoi(optarg);
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
        Usage();
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage();
    }
  }

  if (state.experiment_type == EXPERIMENT_TYPE_INVALID) {
    // Print configuration
    ValidateOperator(state);
    ValidateLayout(state);
    ValidateSelectivity(state);
    ValidateProjectivity(state);
    ValidateScaleFactor(state);
    ValidateColumnCount(state);
    ValidateWriteRatio(state);
    ValidateTuplesPerTileGroup(state);

    LOG_INFO("%s : %lu", "transactions", state.transactions);
  } else {
    ValidateExperiment(state);
  }

  // cache orig scale factor
  orig_scale_factor = state.scale_factor;
}

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
