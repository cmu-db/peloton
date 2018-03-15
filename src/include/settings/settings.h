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

SETTING_int(rpc_port,
            "Peloton rpc port (default: 15445)",
             15445,
             false, false)

// TODO(tianyu): Remove when we change to a different rpc framework
// This is here only because capnp cannot exit gracefully and thus causes
// test failure. This is an issue with the capnp implementation and has
// been such way for a while, so it's unlikely it gets fixed.
// See: https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
SETTING_bool(rpc_enabled,
             "Enable rpc, this should be turned off when testing",
             false, false, false)

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
              "peloton_insecure_server.key",
              false, false)

// Peloton certificate file
SETTING_string(certificate_file,
              "path to certificate file",
              "peloton_insecure_server.crt",
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

// Size of the MonoQueue task queue
SETTING_int(monoqueue_task_queue_size,
            "MonoQueue Task Queue Size (default: 32)",
            32,
            false, false)

// Size of the MonoQueue worker pool
SETTING_int(monoqueue_worker_pool_size,
            "MonoQueue Worker Pool Size (default: 4)",
            4,
            false, false)

// Number of connection threads used by peloton
SETTING_int(connection_thread_count,
            "Number of connection threads (default: std::hardware_concurrency())",
            std::thread::hardware_concurrency(), false, false)

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
// BRAIN
//===----------------------------------------------------------------------===//

// Enable or disable brain
SETTING_bool(brain,
            "Enable brain (default: false)",
            false,
            true, true)

SETTING_string(peloton_address,
               "ip and port of the peloton rpc service, address:port",
               "127.0.0.1:15445",
               false, false)

// Size of the brain task queue
SETTING_int(brain_task_queue_size,
            "Brain Task Queue Size (default: 32)",
            32,
            false, false)

// Size of the brain worker pool
SETTING_int(brain_worker_pool_size,
            "Brain Worker Pool Size (default: 1)",
            1,
            false, false)

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



