//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_configuration.cpp
//
// Identification: src/main/ycsb/ycsb_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <iostream>
#include <fstream>

#include "benchmark/ycsb/ycsb_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  print help message \n"
          "   -i --index             :  index type: bwtree (default) or btree\n"
          "   -k --scale_factor      :  # of K tuples \n"
          "   -d --duration          :  execution duration \n"
          "   -p --profile_duration  :  profile duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -c --column_count      :  # of columns \n"
          "   -o --operation_count   :  # of operations \n"
          "   -u --update_ratio      :  fraction of updates \n"
          "   -z --zipf_theta        :  theta to control skewness \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -m --string_mode       :  store strings \n"
          "   -g --gc_mode           :  enable garbage collection \n"
          "   -n --gc_backend_count  :  # of gc backends \n"
  );
}

static struct option opts[] = {
    { "index", optional_argument, NULL, 'i' },
    { "scale_factor", optional_argument, NULL, 'k' },
    { "duration", optional_argument, NULL, 'd' },
    { "profile_duration", optional_argument, NULL, 'p' },
    { "backend_count", optional_argument, NULL, 'b' },
    { "column_count", optional_argument, NULL, 'c' },
    { "operation_count", optional_argument, NULL, 'o' },
    { "update_ratio", optional_argument, NULL, 'u' },
    { "zipf_theta", optional_argument, NULL, 'z' },
    { "exp_backoff", no_argument, NULL, 'e' },
    { "string_mode", no_argument, NULL, 'm' },
    { "gc_mode", no_argument, NULL, 'g' },
    { "gc_backend_count", optional_argument, NULL, 'n' },
    { NULL, 0, NULL, 0 }
};

void ValidateIndex(const configuration &state) {
  if (state.index != INDEX_TYPE_BTREE && state.index != INDEX_TYPE_BWTREE) {
    LOG_ERROR("Invalid index");
    exit(EXIT_FAILURE);
  }
}

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "scale_factor", state.scale_factor);
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "duration", state.duration);
}

void ValidateProfileDuration(const configuration &state) {
  if (state.profile_duration <= 0) {
    LOG_ERROR("Invalid profile_duration :: %lf", state.profile_duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "profile_duration", state.profile_duration);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "backend_count", state.backend_count);
}

void ValidateColumnCount(const configuration &state) {
  if (state.column_count <= 0) {
    LOG_ERROR("Invalid column_count :: %d", state.column_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "column_count", state.column_count);
}

void ValidateOperationCount(const configuration &state) {
  if (state.operation_count <= 0) {
    LOG_ERROR("Invalid operation_count :: %d", state.operation_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "operation_count", state.operation_count);
}

void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    LOG_ERROR("Invalid update_ratio :: %lf", state.update_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "update_ratio", state.update_ratio);
}

void ValidateZipfTheta(const configuration &state) {
  if (state.zipf_theta < 0 || state.zipf_theta > 1.0) {
    LOG_ERROR("Invalid zipf_theta :: %lf", state.zipf_theta);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "zipf_theta", state.zipf_theta);
}

void ValidateGCBackendCount(const configuration &state) {
  if (state.gc_backend_count <= 0) {
    LOG_ERROR("Invalid gc_backend_count :: %d", state.gc_backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "gc_backend_count", state.gc_backend_count);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.index = INDEX_TYPE_BWTREE;
  state.scale_factor = 1;
  state.duration = 10;
  state.profile_duration = 1;
  state.backend_count = 2;
  state.column_count = 10;
  state.operation_count = 10;
  state.update_ratio = 0.5;
  state.zipf_theta = 0.0;
  state.exp_backoff = false;
  state.string_mode = false;
  state.gc_mode = false;
  state.gc_backend_count = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hemgi:k:d:p:b:c:o:u:z:n:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'i': {
        char *index = optarg;
        if (strcmp(index, "btree") == 0) {
          state.index = INDEX_TYPE_BTREE;
        } else if (strcmp(index, "bwtree") == 0) {
          state.index = INDEX_TYPE_BWTREE;
        } else {
          LOG_ERROR("Unknown index: %s", index);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 'p':
        state.profile_duration = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'o':
        state.operation_count = atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;
      case 'z':
        state.zipf_theta = atof(optarg);
        break;
      case 'e':
        state.exp_backoff = true;
        break;
      case 'm':
        state.string_mode = true;
        break;
      case 'g':
        state.gc_mode = true;
        break;
      case 'n':
        state.gc_backend_count = atof(optarg);
        break;
        
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configuration
  ValidateIndex(state);
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateProfileDuration(state);
  ValidateBackendCount(state);
  ValidateColumnCount(state);
  ValidateOperationCount(state);
  ValidateUpdateRatio(state);
  ValidateZipfTheta(state);
  ValidateGCBackendCount(state);

  LOG_TRACE("%s : %d", "Run exponential backoff", state.exp_backoff);
  LOG_TRACE("%s : %d", "Run string mode", state.string_mode);
  LOG_TRACE("%s : %d", "Run garbage collection", state.gc_mode);
  
}


void WriteOutput() {
  std::ofstream out("outputfile.summary");

  oid_t total_profile_memory = 0;
  for (auto &entry : state.profile_memory) {
    total_profile_memory += entry;
  }

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %d %d %lf %lf :: %lf %lf %d",
           state.scale_factor,
           state.backend_count,
           state.column_count,
           state.operation_count,
           state.update_ratio,
           state.zipf_theta,
           state.throughput,
           state.abort_rate,
           total_profile_memory);

  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << state.column_count << " ";
  out << state.operation_count << " ";
  out << state.update_ratio << " ";
  out << state.zipf_theta << " ";
  out << state.throughput << " ";
  out << state.abort_rate << " ";
  out << total_profile_memory << "\n";

  for (size_t round_id = 0; round_id < state.profile_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.profile_duration * round_id << " - " << std::setw(3)
        << std::left << state.profile_duration * (round_id + 1)
        << " s]: " << state.profile_throughput[round_id] << " "
        << state.profile_abort_rate[round_id] << " "
        << state.profile_memory[round_id] << "\n";
  }
  out.flush();
  out.close();
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
