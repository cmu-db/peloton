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

#include "scheduler/traffic_cop.h"
#include <iostream>

namespace nstore {
namespace scheduler {

std::istream& operator >> (std::istream& in, Payload& msg) {

  int type = PAYLOAD_TYPE_INVALID;
  std::cin >> type >> msg.transaction_id;

  std::getline (std::cin, msg.data);
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
    std::cin >> msg;
    stmts_executed++;

    switch(msg.msg_type){
      case PAYLOAD_TYPE_CLIENT_REQUEST:
        std::cout << "Txn :: " << msg.transaction_id << " Data :: " << msg.data << "\n";

        //LongTask* t = new( tbb::task::allocate_root() ) LongTask(hWnd);
        //tbb::task::enqueue(*t);
        break;

      case PAYLOAD_TYPE_STOP:
        std::cout << "Stopping server.\n";
        std::cout << "Stats :: Executed statements : " << stmts_executed << "\n" ;
        return;

      case PAYLOAD_TYPE_INVALID:
      default:
        std::cout << "Unknown message type : " << msg.msg_type << "\n";
        return;
    }

  }

}

} // namespace scheduler
} // namespace nstore


