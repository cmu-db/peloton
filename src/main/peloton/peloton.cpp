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

#include "configuration/configuration_manager.h"
#include "common/init.h"
#include "common/logger.h"
#include "network/network_manager.h"
#include "wire/libevent_server.h"

// Peloton process begins execution here.
int main(int argc, char *argv[]) {

  // Parse the command line flags
  peloton::configuration::init_parameters(&argc, &argv);

  // If "-h" or "-help" is passed in, set up the help messages.
  if (GET_BOOL("h") || GET_BOOL("help")) {
    SET_BOOL("help", true);
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Print configuration
  if (GET_BOOL("display_configuration")) {
    auto config = peloton::configuration::ConfigurationManager::GetInstance();
    config->PrintConfiguration();
  }

  try {
    // Setup
    peloton::PelotonInit::Initialize();

    // Create NetworkManager object
    peloton::network::NetworkManager network_manager;
    
    // Start NetworkManager
    network_manager.StartServer();
  }
  catch(peloton::ConnectionException exception){
    // Nothing to do here!
  }

  // Teardown
  peloton::PelotonInit::Shutdown();

  return 0;
}
