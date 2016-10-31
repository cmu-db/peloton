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

#include <iostream>
#include "common/init.h"
#include "common/config.h"
#include "wire/libevent_server.h"

// Peloton process begins execution here.
int main(int argc, char *argv[]) {

  // Parse the command line flags using GFLAGS
  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  // If "-h" or "-help" is passed in, set up the help messages.
  if (FLAGS_help || FLAGS_h) {
    FLAGS_help = true;
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Setup
  peloton::PelotonInit::Initialize();

  // Launch server
  peloton::wire::LibeventServer libeventserver;

  // Teardown
  peloton::PelotonInit::Shutdown();

  return 0;
}
