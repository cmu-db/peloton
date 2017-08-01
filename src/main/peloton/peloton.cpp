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
#include "wire/libevent_server.h"

// workaround a bug in the EPEL packaging of gflags
#ifndef GFLAGS_NAMESPACE
#define GFLAGS_NAMESPACE gflags
#endif

// Peloton process begins execution here.
int main(int argc, char *argv[]) {

  // Parse the command line flags using GFLAGS
  ::GFLAGS_NAMESPACE::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  // If "-h" or "-help" is passed in, set up the help messages.
  if (FLAGS_help || FLAGS_h) {
    FLAGS_help = true;
    ::GFLAGS_NAMESPACE::SetUsageMessage("Usage Info: \n");
    ::GFLAGS_NAMESPACE::HandleCommandLineHelpFlags();
  }

  // Print configuration
  if (FLAGS_display_configuration == true) {
    peloton::configuration::PrintConfiguration();
  }

  try {
    // Setup
    peloton::PelotonInit::Initialize();

    // Create LibeventServer object
    peloton::wire::LibeventServer libeventserver;
    
    // Start Libevent Server    
    libeventserver.StartServer();
  }
  catch(peloton::ConnectionException exception){
    // Nothing to do here!
  }

  // Teardown
  peloton::PelotonInit::Shutdown();

  return 0;
}
