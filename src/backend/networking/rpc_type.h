//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_type.h
//
// Identification: /peloton/src/backend/networking/rpc_type.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace networking {

enum MessageType {
    MSG_TYPE_INVALID = 0,
    MSG_TYPE_HEARTBEAT = 1,
    MSG_TYPE_INITIALIZE = 2,
    MSG_TYPE_QUERYESTIMATE = 3,
    MSG_TYPE_SENDDATA = 4,
    MSG_TYPE_SHUTDOWNPREPARE = 5,
    MSG_TYPE_SHUTDOWN = 6,
    MSG_TYPE_TIMESYNC = 7,
    MSG_TYPE_TRANSACTIONDEBUG = 8,
    MSG_TYPE_TRANSACTIONFINISH = 9,
    MSG_TYPE_TRANSACTIONINIT = 10,
    MSG_TYPE_TRANSACTIONMAP = 11,
    MSG_TYPE_TRANSACTIONPREFETCH = 12,
    MSG_TYPE_TRANSACTIONPREPARE = 13,
    MSG_TYPE_TRANSACTIONREDIRECT = 14,
    MSG_TYPE_TRANSACTIONREDUCE = 15,
    MSG_TYPE_TRANSACTIONWORK = 16,
    MSG_TYPE_UNEVICTDATA = 17,
    MSG_TYPE_WORKFRAGMENT = 18,
    MSG_TYPE_WORKRESULT = 19
};

} // namespace message
} // namespace peloton
