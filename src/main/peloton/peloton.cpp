//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton.cpp
//
// Identification: src/main/peloton/peloton.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include <gflags/gflags.h>
#include "common/init.h"
#include "common/logger.h"
#include "network/peloton_server.h"
#include "settings/settings_manager.h"
#include "brain/brain.h"
#include "brain/index_selection_job.h"

// For GFlag's built-in help message flag
DECLARE_bool(help);

int RunPelotonServer() {
  int return_code = 0;
  try {
    // Setup
    peloton::PelotonInit::Initialize();

    peloton::network::PelotonServer peloton_server;

    peloton::network::PelotonServer::LoadSSLFileSettings();
    peloton::network::PelotonServer::SSLInit();

    peloton_server.SetupServer().ServerLoop();
  } catch (peloton::ConnectionException &exception) {
    // log error message and mark failure
    peloton::LOG_ERROR("Cannot start server. Failure detail : %s\n",
                       exception.GetMessage().c_str());
    return_code = EXIT_FAILURE;
  }

  // Teardown
  peloton::PelotonInit::Shutdown();
  return return_code;
}

int RunPelotonBrain() {
  // TODO(tianyu): boot up other peloton resources as needed here
  peloton::brain::Brain brain;
  evthread_use_pthreads();
  // TODO(tianyu): register jobs here
  struct timeval one_minute;
  one_minute.tv_sec = 10;
  one_minute.tv_usec = 0;

  // The handler for the Index Suggestion related RPC calls to create/drop
  // indexes
  // TODO[vamshi]: Remove this hard coding
  auto num_queries_threshold = 2;
  brain.RegisterJob<peloton::brain::IndexSelectionJob>(
      &one_minute, "index_suggestion", num_queries_threshold);
  brain.Run();
  return 0;
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
    peloton::LOG_ERROR("Cannot load settings. Failed with %s\n",
                       exception.GetMessage().c_str());
    return EXIT_FAILURE;  // TODO: Use an enum with exit error codes
  }

//  int exit_code = 0;
//  if (peloton::settings::SettingsManager::GetBool(
//          peloton::settings::SettingId::brain))
//    exit_code = RunPelotonBrain();
//  else {
//    exit_code = RunPelotonServer();

    // TODO[Siva]: Remove this from the final PR. Uncomment this to run brain
    // and server in the same process for testing. This is a temporary to way to
    // run both peloton server and the brain together to test the index suggestion
    // at the brain end without catalog replication between the server and the
    // brain
//    peloton::settings::SettingsManager::SetBool(
//        peloton::settings::SettingId::brain, true);
//    peloton::settings::SettingsManager::SetBool(
//        peloton::settings::SettingId::rpc_enabled, true);

    int exit_code = 0;
    if (peloton::settings::SettingsManager::GetBool(
        peloton::settings::SettingId::brain)) {
      std::thread brain(RunPelotonBrain);
      exit_code = RunPelotonServer();
      brain.join();
    } else {
      exit_code = RunPelotonServer();
    }


  return exit_code;
}
