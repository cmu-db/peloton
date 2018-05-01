#pragma once

#include <map>
#include "type/serializeio.h"
#include "logging/log_record.h"

namespace peloton{
namespace logging {



class WalRecovery{

private:

    // (graghura): this is a dirty hack to establish the
    // replay order. This is tightly coupled with the way
    // transaction id is generated :(
    // The transaction with the smaller epoch_id is replayed
    // first. If the epoch's are the same, then the txn with the
    // lower next_txn_id (decentralized_epoch_manager.cpp)
    // is replayed first.

    // TODO(graghura) : change this after the mvcc bugs are fixed.

    struct RecoveryComparator {
        bool operator()(txn_id_t txn_id_1, txn_id_t txn_id_2) const {
          eid_t epoch_txn_1 =  txn_id_1 >> 32;
          eid_t epoch_txn_2 =  txn_id_2 >> 32;

          int next_id_1 = txn_id_1 & 0xFFFFFFFF;
          int next_id_2 = txn_id_2 & 0xFFFFFFFF;

          if(txn_id_1==txn_id_2)
            return false;

          if(epoch_txn_1==epoch_txn_2){
            if(next_id_1<=next_id_2)
              return true;
            else
              return false;
          } else if(epoch_txn_1<epoch_txn_2) {
            return true;
          } else {
            return false;
          }
        }
    };

    // key: txn_id, value: {COMMITED/ABORTED, len of all log_records of txn_id}
    using ReplayTxnMap = std::map<txn_id_t, std::pair<LogRecordType, int>, RecoveryComparator>;


public:
    WalRecovery() {}
    ~WalRecovery(){}


    void StartRecovery();

private:

    void ReplayLogFile();
    void FirstPass(ReplayTxnMap &, size_t &log_buffer_size);
    void SecondPass( std::map<txn_id_t, std::pair<int, int>> &all_txns,
                                  char *buffer, size_t buf_len);

//    void SecondPass(ReplayTxnMap &all_txns, char *buffer, size_t buf_size);



//  LogRecord ReadRecord(CopySerializeInput record_decode);
//  void DoRecovery();
//  void ReplayLogRecord();

  //TODO(graghura): don't hardcode the path
  std::string logpath_ = "/tmp/log";
  std::fstream fstream_;




};

}
}