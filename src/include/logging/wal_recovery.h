#pragma once

#include <fstream>
#include <map>

#include "type/serializeio.h"
#include "logging/log_record.h"
#include "storage/storage_manager.h"
#include "storage/tuple.h"

namespace peloton{
namespace logging {



class WalRecovery{

private:

    enum class ReplayStage{
        PASS_1,
        PASS_2
    };

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
    WalRecovery(std::string logpath) :
      log_path_(logpath),
      max_epoch_id_(INVALID_EID),
      log_buffer_size_(0),
      log_buffer_(nullptr) {}

    ~WalRecovery(){}


    void StartRecovery();

private:

  void ReplayLogFile();
  void ParseFromDisk(ReplayStage stage);
  void ReplayAllTxns();
  void ReplaySingleTxn(txn_id_t txn_id);

  bool InstallCatalogTuple(LogRecordType type, storage::Tuple *tuple,
                                   storage::DataTable *table, cid_t cur_cid,
                                   ItemPointer location);


  void CreateTableOnRecovery(std::unique_ptr<storage::Tuple> &tuple,
                                            std::vector<catalog::Column> &columns);


  std::string log_path_;
  std::fstream fstream_;

  eid_t max_epoch_id_;

  int log_buffer_size_;
  char *log_buffer_;

  ReplayTxnMap all_txns_;
  std::map<txn_id_t, std::pair<int, int>> commited_txns_;  // {txn_id, {log_buf_start_offset, log_buf_curr_offset}}


};

}
}