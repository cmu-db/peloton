//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_log_manager.h
//
// Identification: src/logging/wal_log_manager.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <fstream>

#include "settings/settings_manager.h"
#include "util/file_util.h"
#include "logging/wal_recovery.h"


namespace peloton{
namespace logging{

class LogManager{
public:

  LogManager() : enable_logging_(false) {}
  ~LogManager() {}

  inline bool init() {

    log_dir_ = settings::SettingsManager::GetString(settings::SettingId::log_directory_name);
    log_file_ = settings::SettingsManager::GetString(settings::SettingId::log_file_name);

    if (!peloton::FileUtil::Exists(log_dir_)) {
      boost::filesystem::path dir(log_dir_.c_str());
      try {
        boost::filesystem::create_directory(dir);
      }
      catch (boost::filesystem::filesystem_error &e) {
        LOG_INFO("Failed to create log directory %s. Reason %s", log_dir_.c_str(), e.what());
        return false;
      }
    }

    if(!logger_ofstream_.is_open()){
      logger_ofstream_.open(log_dir_ + "/" + log_file_, std::ofstream::out | std::ofstream::app);

      if(!logger_ofstream_.fail()) {
        enable_logging_ = true;
        return true;
      }
    }

    return false;
  }

  inline static LogManager &GetInstance(){
    static LogManager log_manager;
    return log_manager;
  }

  inline static size_t GetTransactionBufferSize() {
    return settings::SettingsManager::GetInt(settings::SettingId::transaction_buffer_size);
  }

  inline static size_t GetLoggerBufferSize() {
    return settings::SettingsManager::GetInt(settings::SettingId::log_buffer_size);
  }

  inline bool IsLoggingEnabled() { return enable_logging_; }

  inline std::string GetDirectory() { return log_dir_; }
  inline std::string GetLogFilePath() { return log_dir_ + "/" + log_file_; }



  inline void DoRecovery(){
    WalRecovery* wr = new WalRecovery(GetLogFilePath());
    wr->StartRecovery();
    delete wr;
  }


  inline std::ofstream *GetFileStream() { return &logger_ofstream_; }



private:
  // NOTE: ofstream is not thread safe, might need to change it if more than one logger thread is used
  bool enable_logging_;
  std::string log_dir_;
  std::string log_file_;
  std::ofstream logger_ofstream_;


};

}
}
