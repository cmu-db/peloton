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

#include "common/config.h"
#include "common/types.h"

DEFINE_uint64(port, 15721, "Peloton port (default: 15721)");

DEFINE_uint64(max_connections, 64,
              "Maximum number of connections (default: 64)");

DEFINE_string(socket_family, "AF_INET", "Socket family (AF_UNIX, AF_INET)");

DEFINE_uint64(stats_mode, peloton::STATS_TYPE_INVALID,
              "Enable statistics collection (default: STATS_TYPE_INVALID)");

DEFINE_bool(h, false, "Show help");
