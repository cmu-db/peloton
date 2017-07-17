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

#include "configuration/configuration.h"
#include "common/init.h"
#include "common/logger.h"
#include "networking/network_server.h"

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

  // Print configuration
  if (FLAGS_display_configuration == true) {
    peloton::configuration::PrintConfiguration();
  }

  try {
    // Setup
    peloton::PelotonInit::Initialize();

    // Create LibeventServer object
    peloton::networking::NetworkServer networkserver;
    
    // Start Libevent Server    
    networkserver.StartServer();

    // Teardown
    peloton::PelotonInit::Shutdown();
  }
  catch(peloton::ConnectionException exception){
    // Nothing to do here!
  }

  return 0;
}
