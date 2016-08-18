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

  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  if (FLAGS_help || FLAGS_h) {
    FLAGS_help = true;
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Setup
  peloton::PelotonInit::Initialize();

  // Launch server
  peloton::wire::Server server;

  // Teardown
  peloton::PelotonInit::Shutdown();

  return 0;
}
