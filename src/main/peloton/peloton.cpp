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
#include "common/init.h"
#include "common/logger.h"
#include "network/peloton_server.h"
#include "settings/settings_manager.h"
#include "brain/brain.h"

// For GFlag's built-in help message flag
DECLARE_bool(help);

void RunPelotonServer() {
  try {
    // Setup
    peloton::PelotonInit::Initialize();

    peloton::network::PelotonServer peloton_server;

    peloton::network::PelotonServer::LoadSSLFileSettings();
    peloton::network::PelotonServer::SSLInit();

    peloton_server.SetupServer().ServerLoop();
  } catch (peloton::ConnectionException &exception) {
    // Nothing to do here!
  }

  // Teardown
  peloton::PelotonInit::Shutdown();
}


void RunPelotonBrain() {
  // TODO(tianyu): boot up other peloton resources as needed here
  peloton::brain::Brain brain;
  evthread_use_pthreads();
  // TODO(tianyu): register jobs here
  struct timeval one_second;
  one_second.tv_sec = 1;
  one_second.tv_usec = 0;

  auto example_task = [](peloton::brain::BrainEnvironment *) {
    // TODO(tianyu): Replace with real address
    capnp::EzRpcClient client("localhost:15445");
    PelotonService::Client peloton_service = client.getMain<PelotonService>();
    auto request = peloton_service.createIndexRequest();
    request.getRequest().setIndexKeys(42);
    auto response = request.send().wait(client.getWaitScope());
  };

  brain.RegisterJob<peloton::brain::SimpleBrainJob>(&one_second, "test", example_task);
  brain.Run();
}

int main(int argc, char *argv[]) {

  // Parse the command line flags
  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  // If "-h" or "-help" is passed in, set up the help messages.
  if (FLAGS_help) {
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  try {
    // Print settings
    if (peloton::settings::SettingsManager::GetBool(
      peloton::settings::SettingId::display_settings)) {
      auto &settings = peloton::settings::SettingsManager::GetInstance();
      settings.ShowInfo();
    }
  } catch (peloton::SettingsException &exception) {
    peloton::LOG_ERROR("Cannot load settings. Failed with %s\n", exception.GetMessage().c_str());
    return 0; // TODO: Use an enum with exit error codes
  }

  if (peloton::settings::SettingsManager::GetBool(
      peloton::settings::SettingId::brain))
    RunPelotonBrain();
  else
    RunPelotonServer();
  return 0;
}
