//
// Created by Gandeevan R on 2/12/18.
//

#pragma once

#include "logging/log_record.h"
#include "type/serializeio.h"


namespace peloton {

namespace storage {
    class TileGroupHeader;
}

namespace logging {

#define TRANSACTION_BUFFER_LIMIT 2048;

class WalLogger {
  public:

//    WalLogger(const size_t &log_id, const std::string &log_dir)
//            : log_id_(log_id), log_dir_(log_dir) {}

    WalLogger() {}

    ~WalLogger() {}



//  private:
//    std::string GetLogFileFullPath() {
//      return log_dir_ + "/" + logging_filename_prefix_ + "_" +
//             std::to_string(log_id_);
//    }


//  private:
//    size_t log_id_;
//    std::string log_dir_;
//
//    const std::string logging_filename_prefix_ = "logfile";
};
}
}


