//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_configuration.h
//
// Identification: src/include/benchmark/sdbench/sdbench_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <getopt.h>
#include <sys/time.h>
#include <iostream>
#include <string>
#include <vector>

#include "storage/data_table.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

enum IndexUsageType {
  INDEX_USAGE_TYPE_INVALID = 0,

  INDEX_USAGE_TYPE_PARTIAL_FAST = 1,    // use partially materialized indexes (fast)
  INDEX_USAGE_TYPE_PARTIAL_MEDIUM = 2,  // use partially materialized indexes (medium)
  INDEX_USAGE_TYPE_PARTIAL_SLOW = 3,    // use partially materialized indexes (slow)
  INDEX_USAGE_TYPE_FULL = 4,       // use only fully materialized indexes
  INDEX_USAGE_TYPE_NEVER = 5,      // never use ad-hoc indexes
};

enum QueryComplexityType {
  QUERY_COMPLEXITY_TYPE_INVALID = 0,

  QUERY_COMPLEXITY_TYPE_SIMPLE = 1,
  QUERY_COMPLEXITY_TYPE_MODERATE = 2,
  QUERY_COMPLEXITY_TYPE_COMPLEX = 3

};

enum WriteComplexityType {
  WRITE_COMPLEXITY_TYPE_INVALID = 0,

  WRITE_COMPLEXITY_TYPE_SIMPLE = 1,
  WRITE_COMPLEXITY_TYPE_COMPLEX = 2,
  // This is a special complexity type, where we do insert instead of update.
  WRITE_COMPLEXITY_TYPE_INSERT = 3
};

extern int orig_scale_factor;

static const int generator_seed = 50;

class configuration {
 public:
  // What kind of indexes can be used ?
  IndexUsageType index_usage_type;

  // Complexity of the query.
  QueryComplexityType query_complexity_type;

  // Complexity of update.
  WriteComplexityType write_complexity_type;

  // size of the table
  int scale_factor;

  int tuples_per_tilegroup;

  // tile group layout
  LayoutType layout_mode;

  double selectivity;

  double projectivity;

  // column count
  oid_t attribute_count;

  // write ratio
  double write_ratio;

  // # of times to run operator
  std::size_t phase_length;

  // total number of ops
  size_t total_ops;

  // Verbose output
  bool verbose;

  // Convergence test?
  bool convergence;

  // INDEX TUNER PARAMETERS

  // duration between pauses
  oid_t duration_between_pauses;

  // duration of pause
  oid_t duration_of_pause;

  // sample count threshold after which
  // tuner analyze iteration takes place
  oid_t analyze_sample_count_threshold;

  // max tile groups indexed per tuning iteration per table
  oid_t tile_groups_indexed_per_iteration;

  // CONVERGENCE PARAMETER

  // number of queries for which index configuration must remain stable
  oid_t convergence_op_threshold;

  // VARIABILITY PARAMETER
  oid_t variability_threshold;

  // DROP PARAMETER

  // index utility threshold
  double index_utility_threshold;

  // maximum # of indexes per table
  oid_t index_count_threshold;

  // write intensive workload ratio threshold
  double write_ratio_threshold;

  // wether run multi stage experiment or not.
  bool multi_stage;

  // whether in holistic indexing mode or not.
  bool holistic_indexing;

  oid_t multi_stage_idx = 0;
};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void GenerateSequence(oid_t column_count);

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
