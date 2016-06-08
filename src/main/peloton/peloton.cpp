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

#include "wire/socket_base.h"
#include "wire/wire.h"

DECLARE_bool(help);
DECLARE_bool(h);

// Peloton process begins execution here.
int main(int argc, char *argv[]) {

  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  if (FLAGS_help || FLAGS_h) {
    FLAGS_help = true;
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Launch server
  peloton::wire::Server server;
  peloton::wire::StartServer(&server);
  peloton::wire::HandleConnections<peloton::wire::PacketManager,
                                   peloton::wire::PktBuf>(&server);

  return 0;
}
