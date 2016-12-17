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

#include "configuration/configuration.h"
#include "common/types.h"

DEFINE_uint64(port, 15721, "Peloton port (default: 15721)");

DEFINE_uint64(max_connections, 64,
              "Maximum number of connections (default: 64)");

DEFINE_string(socket_family, "AF_INET", "Socket family (AF_UNIX, AF_INET)");

DEFINE_uint64(stats_mode, peloton::STATS_TYPE_INVALID,
              "Enable statistics collection (default: STATS_TYPE_INVALID)");

DEFINE_bool(h, false, "Show help");

// Layout mode
int peloton_layout_mode = peloton::LAYOUT_TYPE_ROW;

// Logging mode
peloton::LoggingType peloton_logging_mode = peloton::LOGGING_TYPE_INVALID;

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
