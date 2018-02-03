//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_endpoint.h
//
// Identification: src/include/network/service/peloton_endpoint.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rpc_server.h"
#include "peloton_service.h"

namespace peloton {
namespace network {
namespace service {

#define PELOTON_ENDPOINT_ADDR "127.0.0.1:9000"
#define PELOTON_ENDPOINT_INTER "128.237.168.175:9000"
#define PELOTON_ENDPOINT_FILE "ipc:///tmp/echo.sock"
#define PELOTON_SERVER_PORT 9000

}  // namespace service
}  // namespace network
}  // namespace peloton
