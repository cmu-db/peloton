//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.cpp
//
// Identification: src/common/configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/types.h"

// Layout mode
int peloton_layout_mode = LAYOUT_TYPE_ROW;

// Logging mode
LoggingType peloton_logging_mode = LOGGING_TYPE_INVALID;

// GC mode
GCType peloton_gc_mode;

// Checkpoint mode
CheckpointType peloton_checkpoint_mode;

// Directory for peloton logs
char *peloton_log_directory;

// Wait Time Out
int64_t peloton_wait_timeout;

int peloton_flush_frequency_micros;
