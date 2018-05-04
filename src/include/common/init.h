//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.h
//
// Identification: src/include/common/init.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <include/settings/settings_manager.h>
#pragma once

namespace peloton {

class ThreadPool;

extern ThreadPool thread_pool;

//===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

class PelotonInit {
 public:
  static void Initialize(std::string log_dir = settings::SettingsManager::GetString(settings::SettingId::log_directory_name),
                         std::string log_file = settings::SettingsManager::GetString(settings::SettingId::log_file_name),
                         bool enable_logging = true);

  static void Shutdown();

  static void SetUpThread();

  static void TearDownThread();
};

}  // namespace peloton
