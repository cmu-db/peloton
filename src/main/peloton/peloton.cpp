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

void RunPelotonEngine() {
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
  peloton::brain::ExampleBrainJob job;
  brain.RegisterJob(&one_second, "test", job);
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

  // Print settings
  if (peloton::settings::SettingsManager::GetBool(
          peloton::settings::SettingId::display_settings)) {
    auto &settings = peloton::settings::SettingsManager::GetInstance();
    settings.ShowInfo();
  }

  if (peloton::settings::SettingsManager::GetBool(
      peloton::settings::SettingId::brain))
    RunPelotonBrain();
  else
    RunPelotonEngine();
  return 0;
}
