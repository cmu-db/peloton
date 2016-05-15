//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_client_test.cpp
//
// Identification: tests/networking/rpc_client_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/networking/tcp_address.h"
#include "backend/networking/rpc_type.h"
#include "backend/networking/rpc_client.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/tcp_connection.h"
#include "backend/networking/peloton_service.h"
#include "backend/networking/abstract_service.pb.h"

#include <functional>
#include <pthread.h>

namespace peloton {
namespace test {

class RpcCoordinatorTests : public PelotonTest {};

void* Coordinator(__attribute__((unused)) void* arg) {

    google::protobuf::Service* service = NULL;

    try {
        peloton::networking::RpcServer rpc_server(PELOTON_SERVER_PORT);
        service = new peloton::networking::PelotonService();
        rpc_server.RegisterService(service);
        rpc_server.Start();
    } catch (std::exception& e) {
        std::cerr << "STD EXCEPTION : " << e.what() << std::endl;
        delete service;
    } catch (...) {
        std::cerr << "UNTRAPPED EXCEPTION " << std::endl;
        delete service;
    }

    return NULL;
}

TEST_F(RpcCoordinatorTests, BasicTest) {
  // create PRC server
  pthread_t server_thread;
  int ret = pthread_create(&server_thread, NULL, Coordinator, NULL);
  if( -1 == ret ) {
    std::cerr << "create thread error\n" << std::endl;
  }
  //pthread_detach(server_thread);

  sleep(1);

  auto pclient = std::make_shared<peloton::networking::RpcClient>(PELOTON_ENDPOINT_ADDR);

  try {
    for (int i = 1; i < 2; i++) {
      // create PRC request
      peloton::networking::HeartbeatRequest request;
      peloton::networking::HeartbeatResponse response;
      request.set_sender_site(i);
      request.set_last_transaction_id(i*10);

      // send RPC request
      pclient->Heartbeat(&request, &response);

      sleep(1);

      // check RPC response
      // FIXME: I don't know why this is failing. Maybe I misunderstood something..
      peloton::networking::Status status = peloton::networking::ABORT_SPECULATIVE;
      EXPECT_EQ(response.sender_site(), 9876);
      EXPECT_EQ(response.status(), status);
    }
  } catch (std::exception& e) {
    std::cerr << "STD EXCEPTION : " << e.what() << std::endl;
  } catch (...) {
    std::cerr << " UNTRAPPED EXCEPTION " << std::endl;
  }

  // FIXME: This part is problematic. I don't really know how to quit RPC server...
  pthread_cancel(server_thread);
  pthread_join(server_thread, NULL);
}

}
}

