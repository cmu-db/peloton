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
#include <include/settings/settings_manager.h>


namespace peloton{
namespace logging{

class LogManager{
public:

  LogManager() {}
  ~LogManager() {}

  inline bool init(std::string log_dir, std::string log_file, bool enable_logging) {
    LOG_INFO("aaron: LogManagerInit");
    if (enable_logging == false) {
      enable_logging_ = false;
      return false;
    }
    directory_ = log_dir;

    if(!logger_ofstream_.is_open()){
      logger_ofstream_.open(log_dir+"/"+log_file, std::ofstream::out | std::ofstream::app);

      if(!logger_ofstream_.fail()) {
        enable_logging_ = true;
        return true;
      }
    }
    enable_logging_ = false;
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
  inline std::string GetDirectory() { return directory_; }
  inline std::string GetLogFilePath() { return directory_+"/tmp"; }
  inline std::ofstream *GetFileStream() { return &logger_ofstream_; }

//  TODO(gandeevan): support StartLogging and StopLogging
//  static void StartLogging() {}
//  static void StopLogging() {}


private:
  // NOTE: ofstream is not thread safe, might need to change it if more than one logger thread is used
  bool enable_logging_;
  std::string directory_;
  std::ofstream logger_ofstream_;


};

}
}
