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

#include <gflags/gflags.h>

DEFINE_uint64(port, 5432, "Peloton port (default: 5432)");
DEFINE_uint64(max_connections, 64, "Maximum number of connections (default: 64)");
DEFINE_string(socket_family, "AF_UNIX", "Socket family (AF_UNIX, AF_INET)");
DEFINE_bool(h, false, "Show help");
