//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// peloton_endpoint.h
//
// Identification: src/backend/networking/peloton_endpoint.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_service.h"
#include <iostream>

namespace peloton {
namespace message {

#define PELOTON_ENDPOINT_ADDR "127.0.0.1:9000"
#define PELOTON_ENDPOINT_FILE "ipc:///tmp/echo.sock"
#define PELOTON_SERVER_PORT 9000


}  // namespace message
}  // namespace peloton
