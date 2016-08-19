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

// Peloton port
DECLARE_uint64(port);

// Maximum number of connections
DECLARE_uint64(max_connections);

// Socket family (AF_UNIX, AF_INET)
DECLARE_string(socket_family);

// Both for showing the help info
DECLARE_bool(h);
DECLARE_bool(help);
