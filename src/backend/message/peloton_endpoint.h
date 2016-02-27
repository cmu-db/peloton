//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// peloton_endpoint.h
//
// Identification: src/backend/message/peloton_endpoint.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/message/rpc_server.h"
#include "backend/message/peloton_service.h"
#include <iostream>

namespace peloton {
namespace message {

#define PELOTON_ENDPOINT_ADDR "tcp://127.0.0.1:9999"
#define PELOTON_ENDPOINT_FILE "ipc:///tmp/echo.sock"


}  // namespace message
}  // namespace peloton
