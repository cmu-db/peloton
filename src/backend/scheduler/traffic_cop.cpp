/*-------------------------------------------------------------------------
 *
 * traffic_cop.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/traffic_cop.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <tbb/tbb.h>
#include <tbb/concurrent_queue.h>

#include "backend/scheduler/traffic_cop.h"
#include <iostream>

namespace peloton {
namespace scheduler {

std::istream& operator >> (std::istream& in, Payload& msg) {

  int type = PAYLOAD_TYPE_INVALID;
  std::cin >> type;
  msg.msg_type = (PayloadType) type;

  return in;
};

//===--------------------------------------------------------------------===//
// Traffic Cop
//===--------------------------------------------------------------------===//

TrafficCop& TrafficCop::GetInstance() {
  static TrafficCop tcop;
  return tcop;
}

void TrafficCop::Execute() {
  Payload msg;

  for(;;) {
    std::cout << prompt;

    std::cin >> msg;
    stmts_executed++;

    switch(msg.msg_type){
      case PAYLOAD_TYPE_CLIENT_REQUEST:

        std::cin >> msg.transaction_id;
        std::getline (std::cin, msg.data);
        std::cout << "Txn :: " << msg.transaction_id << " Request :: " << msg.data << "\n";

        // Route it ?

        break;

      case PAYLOAD_TYPE_CLIENT_RESPONSE:

        std::cin >> msg.transaction_id;
        std::getline (std::cin, msg.data);

        // Pop out the response
        responses.try_pop(msg);
        std::cout << "Txn :: " << msg.transaction_id << " Response :: " << msg.data << "\n";

        break;

      case PAYLOAD_TYPE_STOP:
        std::cout << "Stopping server.\n";
        std::cout << "Stats :: Executed statements : " << stmts_executed << "\n" ;
        return;

      case PAYLOAD_TYPE_INVALID:
      default:
        std::cout << "Unknown message type : " << msg.msg_type << "\n";
        break;
    }

  }

}

} // namespace scheduler
} // namespace peloton


