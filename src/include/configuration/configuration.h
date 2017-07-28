//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.h
//
// Identification: src/include/configuration/configuration.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


//===----------------------------------------------------------------------===//
// Use following macros to define configuration parameters
//===----------------------------------------------------------------------===//
// CONFIG_int(name, description, default_value, is_mutable, is_persistent)
// CONFIG_bool(name, description, default_value, is_mutable, is_persistent)
// CONFIG_string(name, description, default_value, is_mutable, is_persistent)
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
           false, false)

// Maximum number of connections
CONFIG_int(max_connections,
           "Maximum number of connections (default: 64)",
           64,
           true, true)

// Socket family
CONFIG_string(socket_family,
              "Socket family (default: AF_INET)",
              "AF_INET",
              false, false)

// Added for SSL only begins

// Peloton private key file
// Currently use hardcoded private key path, may need to change
// to generate file dynamically at runtime
// The same applies to certificate file
CONFIG_string(private_key_file,
              "path to private key file",
              "/home/vagrant/temp/server.key",
              false, false)

// Peloton certificate file
CONFIG_string(certificate_file,
              "path to certificate file",
              "/home/vagrant/temp/server.crt",
              false, false)

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
            true, true)

//===----------------------------------------------------------------------===//
// STATISTICS
//===----------------------------------------------------------------------===//

// Enable or disable statistics collection
CONFIG_int(stats_mode,
           "Enable statistics collection (default: 0)",
           peloton::STATS_TYPE_INVALID,
           true, true)

//===----------------------------------------------------------------------===//
// AI
//===----------------------------------------------------------------------===//

// Enable or disable index tuner
CONFIG_bool(index_tuner,
            "Enable index tuner (default: false)",
            false,
            true, true)

// Enable or disable layout tuner
CONFIG_bool(layout_tuner,
            "Enable layout tuner (default: false)",
            false,
            true, true)

//===----------------------------------------------------------------------===//
// CODEGEN
//===----------------------------------------------------------------------===//

CONFIG_bool(codegen,
            "Enable code-generation for query execution (default: true)",
            true,
            true, true)

//===----------------------------------------------------------------------===//
// GENERAL
//===----------------------------------------------------------------------===//

// Both for showing the help info
CONFIG_bool(h,
            "Show help",
            false,
            false, false)


