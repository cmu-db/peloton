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

#include "common/macros.h"
#include "wire/socket_base.h"
#include "wire/wire.h"

/*
 * Any Postgres server process begins execution here.
 */
int main(UNUSED_ATTRIBUTE int argc, UNUSED_ATTRIBUTE char *argv[]) {
  peloton::wire::Server server(DEFAULT_PORT, MAX_CONNECTIONS);
  peloton::wire::StartServer(&server);
  peloton::wire::HandleConnections<peloton::wire::PacketManager,
                                   peloton::wire::PktBuf>(&server);

  return 0;
}
