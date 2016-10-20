//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_configuration.cpp
//
// Identification: src/main/tpcc/tpcc_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <fstream>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : tpcc <options> \n"
          "   -h --help              :  print help message \n"
          "   -i --index             :  index type: bwtree (default) or btree\n"
          "   -k --scale_factor      :  scale factor \n"
          "   -d --duration          :  execution duration \n"
          "   -p --profile_duration  :  profile duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -w --warehouse_count   :  # of warehouses \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -a --affinity          :  enable client affinity \n"
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
    { "warehouse_count", optional_argument, NULL, 'w' },
    { "exp_backoff", no_argument, NULL, 'e' },
    { "affinity", no_argument, NULL, 'a' },
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
    LOG_ERROR("Invalid scale_factor :: %lf", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "scale_factor", state.scale_factor);
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

void ValidateWarehouseCount(const configuration &state) {
  if (state.warehouse_count <= 0) {
    LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "warehouse_count", state.warehouse_count);
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
  state.warehouse_count = 2;
  state.exp_backoff = false;
  state.affinity = false;
  state.gc_mode = false;
  state.gc_backend_count = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "heagi:k:d:p:b:w:n:", opts, &idx);

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
        state.scale_factor = atof(optarg);
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
      case 'w':
        state.warehouse_count = atoi(optarg);
        break;
      case 'e':
        state.exp_backoff = true;
        break;
      case 'a':
        state.affinity = true;
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
    }
  }

  // Static TPCC parameters
  state.item_count = 100000 * state.scale_factor;
  state.districts_per_warehouse = 10;
  state.customers_per_district = 3000 * state.scale_factor;
  state.new_orders_per_district = 900 * state.scale_factor;

  // Print configuration
  ValidateIndex(state);
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateProfileDuration(state);
  ValidateBackendCount(state);
  ValidateWarehouseCount(state);
  ValidateGCBackendCount(state);

  LOG_TRACE("%s : %d", "Run client affinity", state.affinity);
  LOG_TRACE("%s : %d", "Run exponential backoff", state.exp_backoff);
  LOG_TRACE("%s : %d", "Run garbage collection", state.gc_mode);
}



void WriteOutput() {
  std::ofstream out("outputfile.summary");

  oid_t total_profile_memory = 0;
  for (auto &entry : state.profile_memory) {
    total_profile_memory += entry;
  }

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf %d %d :: %lf %lf %d",
           state.scale_factor,
           state.backend_count,
           state.warehouse_count,
           state.throughput,
           state.abort_rate,
           total_profile_memory);

  out << state.scale_factor << " ";
  out << state.backend_count << " ";
  out << state.warehouse_count << " ";
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


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
