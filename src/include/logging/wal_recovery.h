#pragma once

#include <map>
#include "type/serializeio.h"
#include "logging/log_record.h"

namespace peloton{
namespace logging {

class WalRecovery{

public:
    WalRecovery() {}
    ~WalRecovery(){}


    void StartRecovery();

private:

    void ReplayLogFile();
    void ParseLog(std::map<txn_id_t, LogRecordType> &all_txns);
    LogRecord ReadRecord(CopySerializeInput record_decode);


//  void DoRecovery();
//  void ReplayLogRecord();

  //TODO(graghura): don't hardcode the path
  std::string logpath_ = "/tmp/log";
  std::fstream fstream_;




};

}
}