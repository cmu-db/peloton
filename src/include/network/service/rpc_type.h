//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_type.h
//
// Identification: src/include/network/service/rpc_type.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace network {
namespace service {

enum MessageType { MSG_TYPE_INVALID = 0, MSG_TYPE_REQ, MSG_TYPE_REP };

}  // namespace service
}  // namespace message
}  // namespace peloton
