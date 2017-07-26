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

#include <gflags/gflags.h>
#include "configuration/configuration_manager.h"
#include "wire/libevent_server.h"
#include "common/init.h"
#include "common/logger.h"
#include "network/network_manager.h"
#include "wire/libevent_server.h"

// Peloton process begins execution here.
int main(int argc, char *argv[]) {

  // Parse the command line flags
  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
  Config::init_parameters();

  // If "-h" or "-help" is passed in, set up the help messages.
  if (Config::GET_BOOL("h") || Config::GET_BOOL("help")) {
    Config::SET_BOOL("help", true);
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Print configuration
  if (Config::GET_BOOL("display_configuration")) {
    auto config = peloton::configuration::ConfigurationManager::GetInstance();
    config->Display();
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
