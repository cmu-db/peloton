//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton.cpp
//
// Identification: src/main/peloton/peloton.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <unistd.h>
#include <iostream>

#include "common/stack_trace.h"
#include "common/init.h"
#include "common/config.h"
#include "common/macros.h"
#include "wire/libevent_server.h"
#include "wire/socket_base.h"

// Peloton process begins execution here.
int main(UNUSED_ATTRIBUTE int argc, UNUSED_ATTRIBUTE char *argv[]) {

  // Setup
  peloton::PelotonInit::Initialize();

  // Launch server
  peloton::PelotonConfiguration configuration;
  peloton::wire::Server server(configuration);

//  peloton::wire::StartServer(configuration, &server);
//  peloton::wire::HandleConnections<peloton::wire::PacketManager,
//                                   peloton::wire::PktBuf>(&server);



  // Teardown
  peloton::PelotonInit::Shutdown();

  return 0;
}
