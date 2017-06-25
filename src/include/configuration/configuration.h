//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// config.h
//
// Identification: src/include/common/config.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <gflags/gflags.h>

//===----------------------------------------------------------------------===//
// FILE LOCATIONS
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONNECTIONS
//===----------------------------------------------------------------------===//

// Peloton port
DECLARE_uint64(port);

// Maximum number of connections
DECLARE_uint64(max_connections);

// Socket family
DECLARE_string(socket_family);

// Added for SSL only begins

// Peloton private key file
DECLARE_string(private_key_file);

// Peloton certificate file
DECLARE_string(certificate_file);

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

// Display configuration
DECLARE_bool(display_configuration);

//===----------------------------------------------------------------------===//
// STATISTICS
//===----------------------------------------------------------------------===//

// Enable or disable statistics collection
DECLARE_uint64(stats_mode);

//===----------------------------------------------------------------------===//
// AI
//===----------------------------------------------------------------------===//

// Enable or disable index tuner
DECLARE_bool(index_tuner);

// Enable or disable layout tuner
DECLARE_bool(layout_tuner);

//===----------------------------------------------------------------------===//
// CODEGEN
//===----------------------------------------------------------------------===//

DECLARE_bool(codegen);

//===----------------------------------------------------------------------===//
// GENERAL
//===----------------------------------------------------------------------===//

// Both for showing the help info
DECLARE_bool(h);
DECLARE_bool(help);

namespace peloton {
namespace configuration {

// Print configuration
void PrintConfiguration();

}  // End configuration namespace
}  // End peloton namespace

