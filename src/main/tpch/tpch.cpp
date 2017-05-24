//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch.cpp
//
// Identification: src/main/tpch/tpch.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <getopt.h>
#include <string>

#include "benchmark/tpch/tpch_configuration.h"
#include "benchmark/tpch/tpch_database.h"
#include "benchmark/tpch/tpch_workload.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpch {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : tpch <options> \n"
          "   -h --help              :  print help message \n"
          "   -i --input-dir         :  location of data \n"
          "   -n --num-runs          :  the number of runs to execute for each query \n"
          "   -s --suffix            :  input file suffix \n"
          "   -d --dict-encode       :  dictionary encode \n"
          "   -q --queries           :  comma-separated list of queries to run (i.g., 1,14 for Q1 and Q14) \n");
}

static struct option opts[] = {
    {"input-dir", required_argument, NULL, 'i'},
    {"dict-encode", optional_argument, NULL, 'd'},
    {"queries", optional_argument, NULL, 'q'},
    {NULL, 0, NULL, 0}};

void ParseArguments(int argc, char **argv, Configuration &config) {
  config.suffix = "tbl";

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hi:n:s:dq:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'i': {
        char *input = optarg;
        config.data_dir = input;
        break;
      }
      case 'n': {
        char *input = optarg;
        config.num_runs = static_cast<uint32_t>(std::atoi(input));
      }
      case 'd': {
        config.dictionary_encode = true;
        break;
      }
      case 'q': {
        char *csv_queries = optarg;
        config.SetRunnableQueries(csv_queries);
        break;
      }
      case 'h': {
        Usage(stderr);
        exit(EXIT_FAILURE);
      }
      default: {
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
      }
    }
  }

  // Validate everything
  if (!config.IsValid()) {
    exit(EXIT_FAILURE);
  }

  LOG_INFO("Input directory   : '%s'", config.data_dir.c_str());
  LOG_INFO("Dictionary encode : %s",
           config.dictionary_encode ? "true" : "false");
  for (uint32_t i = 0; i < 22; i++) {
    LOG_INFO("Run query %u : %s", i + 1,
             config.queries_to_run[i] ? "true" : "false");
  }
}

void RunBenchmark(const Configuration &config) {
  // Create the DB instance
  TPCHDatabase tpch_db{config};

  // Create the benchmark
  TPCHBenchmark tpch_benchmark{config, tpch_db};

  // Run the benchmark
  tpch_benchmark.RunBenchmark();
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton

// Entry point
int main(int argc, char **argv) {
  // The configuration
  peloton::benchmark::tpch::Configuration config;

  // Parse arguments
  peloton::benchmark::tpch::ParseArguments(argc, argv, config);

  // Run workload
  peloton::benchmark::tpch::RunBenchmark(config);

  return 0;
}