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
#include "network/network_manager.h"
#include "wire/libevent_server.h"
#include "configuration/configuration_manager.h"

// Peloton process begins execution here.
int main(int argc, char *argv[]) {

  // Parse the command line flags using GFLAGS
  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
  auto config = peloton::configuration::ConfigurationManager::GetInstance();
  peloton::configuration::initialize_configuration_manager();

  // If "-h" or "-help" is passed in, set up the help messages.
  if (config->GetBool("help") || config->GetBool("h")) {
    config->SetBool("help", true);
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Print configuration
  if (config->GetBool("display_configuration")) {
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
