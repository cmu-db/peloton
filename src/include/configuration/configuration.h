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

// internal use only in
// configuration/configuration_manager.cpp
#ifdef __PELOTON_CONFIG__

#include "configuration/configuration_utils.h"

//===----------------------------------------------------------------------===//
// Use following macros to define configuration parameters
//===----------------------------------------------------------------------===//
// CONFIG_int(name, description, default_value, is_mutable, is_persistent)
// CONFIG_bool(name, description, default_value, is_mutable, is_persistent)
// CONFIG_string(name, description, default_value, is_mutable, is_persistent)
//===----------------------------------------------------------------------===//
// Use REGISTER macro in initialize_configuration_manager() to
// enable a parameter
//===----------------------------------------------------------------------===//
// REGISTER(name)
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// FILE LOCATIONS
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONNECTIONS
//===----------------------------------------------------------------------===//

// Peloton port
CONFIG_int(port,
           "Peloton port (default: 15721)",
           15721,
           false, false);

// Maximum number of connections
CONFIG_int(max_connections,
           "Maximum number of connections (default: 64)",
           64,
           true, true);

// Socket family
CONFIG_string(socket_family,
              "Socket family (default: AF_INET)",
              "AF_INET",
              false, false);

// Added for SSL only begins

// Peloton private key file
// Currently use hardcoded private key path, may need to change
// to generate file dynamically at runtime
// The same applies to certificate file
CONFIG_string(private_key_file,
              "path to private key file",
              "/home/vagrant/temp/server.key",
              false, false);

// Peloton certificate file
CONFIG_string(certificate_file,
              "path to certificate file",
              "/home/vagrant/temp/server.crt",
              false, false);

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
CONFIG_bool(display_configuration,
            "Display configuration (default: false)",
            false,
            true, true);

//===----------------------------------------------------------------------===//
// STATISTICS
//===----------------------------------------------------------------------===//

// Enable or disable statistics collection
CONFIG_int(stats_mode,
           "Enable statistics collection (default: 0)",
           peloton::STATS_TYPE_INVALID,
           true, true);

//===----------------------------------------------------------------------===//
// AI
//===----------------------------------------------------------------------===//

// Enable or disable index tuner
CONFIG_bool(index_tuner,
            "Enable index tuner (default: false)",
            false,
            true, true);

// Enable or disable layout tuner
CONFIG_bool(layout_tuner,
            "Enable layout tuner (default: false)",
            false,
            true, true);

//===----------------------------------------------------------------------===//
// CODEGEN
//===----------------------------------------------------------------------===//

CONFIG_bool(codegen,
            "Enable code-generation for query execution (default: true)",
            true,
            true, true);

//===----------------------------------------------------------------------===//
// GENERAL
//===----------------------------------------------------------------------===//

// Both for showing the help info
CONFIG_bool(h,
            "Show help",
            false,
            false, false);


// special case: -help
DECLARE_bool(help);

namespace peloton {
namespace configuration {

void register_parameters() {
  REGISTER(port);
  REGISTER(max_connections);
  REGISTER(socket_family);
  REGISTER(private_key_file);
  REGISTER(certificate_file);
  REGISTER(display_configuration);
  REGISTER(stats_mode);
  REGISTER(index_tuner);
  REGISTER(layout_tuner);
  REGISTER(codegen);
  REGISTER(h);

  // special case: -help
  auto config = peloton::configuration::ConfigurationManager::GetInstance();
  config->DefineConfig("help",
                       &FLAGS_help,
                       type::TypeId::BOOLEAN,
                       "Show help",
                       false,
                       false, false);
}

} // End configuration namespace
} // End peloton namespace

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

#endif
