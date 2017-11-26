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
// FILE LOCATIONS
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONNECTIONS
//===----------------------------------------------------------------------===//
// Peloton port
SETTING_int(port,
           "Peloton port (default: 15721)",
           15721,
           false, false)

// Maximum number of connections
SETTING_int(max_connections,
           "Maximum number of connections (default: 64)",
           64,
           true, true)

// Socket family
SETTING_string(socket_family,
              "Socket family (default: AF_INET)",
              "AF_INET",
              false, false)

// Added for SSL only begins

// Enables SSL connection. The default value is false
SETTING_bool(ssl, "Enable SSL connection (default: true)", true, false, false)

// Peloton private key file
// Currently use hardcoded private key path, may need to change
// to generate file dynamically at runtime
// The same applies to certificate file
SETTING_string(private_key_file,
              "path to private key file",
              "server.key",
              false, false)

// Peloton certificate file
SETTING_string(certificate_file,
              "path to certificate file",
              "server.crt",
              false, false)

// Peloton root certificate file
SETTING_string(root_cert_file,
               "path to root certificate file",
               "root.crt",
               false, false)

//===----------------------------------------------------------------------===//
// RESOURCE USAGE
//===----------------------------------------------------------------------===//

SETTING_double(bnlj_buffer_size,
             "The default buffer size to use for blockwise nested loop joins (default: 1 MB)",
             1.0 * 1024.0 * 1024.0,
             true, true)

//===----------------------------------------------------------------------===//
// WRITE AHEAD LOG
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// ERROR REPORTING AND LOGGING
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// SETTINGURATION
//===----------------------------------------------------------------------===//

// Display configuration
SETTING_bool(display_settings,
            "Display settings (default: false)",
            false,
            true, true)

//===----------------------------------------------------------------------===//
// STATISTICS
//===----------------------------------------------------------------------===//

// Enable or disable statistics collection
SETTING_int(stats_mode,
           "Enable statistics collection (default: 0)",
           static_cast<int>(peloton::StatsType::INVALID),
           true, true)

//===----------------------------------------------------------------------===//
// AI
//===----------------------------------------------------------------------===//

// Enable or disable index tuner
SETTING_bool(index_tuner,
            "Enable index tuner (default: false)",
            false,
            true, true)

// Enable or disable layout tuner
SETTING_bool(layout_tuner,
            "Enable layout tuner (default: false)",
            false,
            true, true)

//===----------------------------------------------------------------------===//
// CODEGEN
//===----------------------------------------------------------------------===//

SETTING_bool(codegen,
            "Enable code-generation for query execution (default: true)",
            true,
            true, true)


//===----------------------------------------------------------------------===//
// Optimizer
//===----------------------------------------------------------------------===//
SETTING_bool(predicate_push_down,
             "Enable predicate push-down optimization (default: true)",
             true,
             true, true)

SETTING_bool(hash_join_bloom_filter,
             "Enable bloom filter for hash join in codegen (default: true)",
             true,
             true, true)

//===----------------------------------------------------------------------===//
// GENERAL
//===----------------------------------------------------------------------===//



