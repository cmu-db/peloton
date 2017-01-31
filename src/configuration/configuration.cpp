//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// config.cpp
//
// Identification: src/common/config.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "type/types.h"
#include "configuration/configuration.h"

//===----------------------------------------------------------------------===//
// GENERAL
//===----------------------------------------------------------------------===//

DEFINE_bool(h,
            false,
            "Show help");

namespace peloton {
namespace configuration {

void PrintConfiguration(){

  LOG_INFO("%30s", "//===-------------- PELOTON CONFIGURATION --------------===//");
  LOG_INFO(" ");

  LOG_INFO("%30s", "//===----------------- CONNECTIONS ---------------------===//");
  LOG_INFO(" ");

  LOG_INFO("%30s: %10lu","Port", FLAGS_port);
  LOG_INFO("%30s: %10s","Socket Family", FLAGS_socket_family.c_str());
  LOG_INFO("%30s: %10lu","Statistics", FLAGS_stats_mode);
  LOG_INFO("%30s: %10lu","Max Connections", FLAGS_max_connections);

  LOG_INFO(" ");
  LOG_INFO("%30s", "//===---------------------------------------------------===//");

}

}  // End configuration namespace
}  // End peloton namespace

//===----------------------------------------------------------------------===//
// FILE LOCATIONS
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONNECTIONS
//===----------------------------------------------------------------------===//

DEFINE_uint64(port,
              15721,
              "Peloton port (default: 15721)");

DEFINE_uint64(max_connections,
              64,
              "Maximum number of connections (default: 64)");

DEFINE_string(socket_family,
              "AF_INET",
              "Socket family (default: AF_INET)");

//===----------------------------------------------------------------------===//
// RESOURCE USAGE
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// WRITE AHEAD LOG
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// ERROR REPORTING AND LOGGING
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONFIGURATION
//===----------------------------------------------------------------------===//

DEFINE_bool(display_configuration,
            false,
            "Display configuration (default: false)");

//===----------------------------------------------------------------------===//
// STATISTICS
//===----------------------------------------------------------------------===//

DEFINE_uint64(stats_mode,
              peloton::STATS_TYPE_INVALID,
              "Enable statistics collection (default: 0)");

//===----------------------------------------------------------------------===//
// AI
//===----------------------------------------------------------------------===//

DEFINE_bool(index_tuner,
            false,
            "Enable index tuner (default: false)");

DEFINE_bool(layout_tuner,
            false,
            "Enable layout tuner (default: false)");

// Layout mode
int peloton_layout_mode = peloton::LAYOUT_TYPE_ROW;

// Logging mode
peloton::LoggingType peloton_logging_mode = peloton::LoggingType::INVALID;

// GC mode
peloton::GarbageCollectionType peloton_gc_mode;

// Checkpoint mode
peloton::CheckpointType peloton_checkpoint_mode;

// Directory for peloton logs
char *peloton_log_directory;

// Wait Time Out
int64_t peloton_wait_timeout;

int peloton_flush_frequency_micros;

int peloton_flush_mode;

// pcommit latency (for NVM WBL)
int peloton_pcommit_latency;
