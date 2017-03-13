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
#include <cstring>
#include <getopt.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>

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
          "   -s --suffix            :  input file suffix \n"
          "   -d --dict-encode       :  dictionary encode \n"
          "   -q --queries           :  comma-separated list of queries to run (i.g., 1,14 for Q1 and Q14) \n");
}

static struct option opts[] = {
    {"input-dir", required_argument, NULL, 'i'},
    {"dict-encode", optional_argument, NULL, 'd'},
    {"queries", optional_argument, NULL, 'q'},
    {NULL, 0, NULL, 0}};

bool ValidateConfig(const Configuration &config) {
  struct stat info;
  if (stat(config.data_dir.c_str(), &info) != 0) {
    LOG_ERROR("Data directory [%s] isn't accessible", config.data_dir.c_str());
    return false;
  } else if ((info.st_mode & S_IFDIR) == 0) {
    LOG_ERROR("Data directory [%s] isn't a directory", config.data_dir.c_str());
    return false;
  }
  auto inputs = {config.GetCustomerPath(), config.GetLineitemPath(),
                 config.GetNationPath(),   config.GetOrdersPath(),
                 config.GetPartSuppPath(), config.GetPartPath(),
                 config.GetSupplierPath(), config.GetRegionPath()};
  for (const auto &input : inputs) {
    struct stat info;
    if (stat(input.c_str(), &info) != 0) {
      LOG_ERROR("Input file [%s] isn't accessible", input.c_str());
      return false;
    }
  }

  // All good
  return true;
}

void ParseArguments(int argc, char **argv, Configuration &config) {
  config.suffix = "tbl";

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hi:sdq:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'i': {
        char *input = optarg;
        config.data_dir = input;
        break;
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
  if (!ValidateConfig(config)) {
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