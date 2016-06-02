//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_endpoint.h
//
// Identification: src/backend/networking/peloton_endpoint.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_service.h"
#include <iostream>

namespace peloton {
namespace networking {

#define PELOTON_ENDPOINT_ADDR "127.0.0.1:9000"
#define PELOTON_ENDPOINT_INTER "128.237.168.175:9000"
#define PELOTON_ENDPOINT_FILE "ipc:///tmp/echo.sock"
#define PELOTON_SERVER_PORT 9000

}  // namespace networking
}  // namespace peloton
