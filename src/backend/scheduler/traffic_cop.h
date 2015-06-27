/*-------------------------------------------------------------------------
 *
 * traffic_cop.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/scheduler/traffic_cop.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "tbb/concurrent_queue.h"

#include <iostream>
#include <string>

namespace peloton {
namespace scheduler {

//===--------------------------------------------------------------------===//
// Traffic Cop
//===--------------------------------------------------------------------===//

class Payload {

 public:
  Payload() :
    msg_type(PAYLOAD_TYPE_INVALID) {
  }

  Payload(PayloadType msg_type) :
    msg_type(msg_type) {
  }

  Payload(PayloadType msg_type, std::string data) :
    data(data),
    msg_type(msg_type) {
  }

  // helper for istream
  friend std::istream& operator >> (std::istream& in, Payload& msg);

  std::string data;

  oid_t transaction_id = START_OID;

  // type of message
  PayloadType msg_type;

};

// Queue for transmitting results from enqueued tasks
static tbb::concurrent_queue<Payload> responses;

class TrafficCop {

 public:
  void Execute();

  // Singleton
  static TrafficCop& GetInstance();

 private:
  TrafficCop() :
    stmts_executed(0){
  }

  TrafficCop(TrafficCop const&) = delete;
  TrafficCop& operator=(TrafficCop const&) = delete;

  const std::string prompt = "peloton => ";
  oid_t stmts_executed;
};

} // namespace scheduler
} // namespace peloton
