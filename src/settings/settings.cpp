//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings.cpp
//
// Identification: src/settings/settings.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <gflags/gflags.h>

#include "type/types.h"

// Layout mode
peloton::LayoutType peloton_layout_mode = peloton::LAYOUT_TYPE_ROW;

// Logging mode
// peloton::LoggingType peloton_logging_mode = peloton::LoggingTypeId::INVALID;

// GC mode
peloton::GarbageCollectionType peloton_gc_mode;

// Checkpoint mode
// peloton::CheckpointType peloton_checkpoint_mode;

// Directory for peloton logs
char *peloton_log_directory;

// Wait Time Out
int64_t peloton_wait_timeout;

int peloton_flush_frequency_micros;

int peloton_flush_mode;

// pcommit latency (for NVM WBL)
int peloton_pcommit_latency;
