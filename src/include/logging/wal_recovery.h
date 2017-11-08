//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_recovery.h
//
// Identification: src/include/logging/wal_recovery.h
//
// Recovery component, called on init.
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "type/types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
#include "common/logger.h"
#include "type/abstract_pool.h"

namespace peloton {

namespace storage {
class TileGroupHeader;
}

namespace logging {

class WalRecovery {
 public:
  WalRecovery(const size_t &logger_id, const std::string &log_dir)
      : logger_id_(logger_id), log_dir_(log_dir) {}

  ~WalRecovery() {}

  void StartRecovery();
  void WaitForRecovery();

 private:
  // void Run();

  std::string GetLogFileFullPath(size_t epoch_id) {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" +
           std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
  }

  void GetSortedLogFileIdList();

  void RunRecovery();

  bool ReplayLogFile(FileHandle &file_handle);

  bool InstallTupleRecord(LogRecordType type, storage::Tuple *tuple,
                          storage::DataTable *table, cid_t cur_cid,
                          ItemPointer location);

  // Return value is the swapped txn id, either INVALID_TXNID or INITIAL_TXNID
  txn_id_t LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset);
  void UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset,
                   txn_id_t new_txn_id);

 private:
  size_t logger_id_;
  std::string log_dir_;

  std::vector<size_t> file_eids_;
  std::atomic<int> max_replay_file_id_;

  // Since every thing is copied to the
  const std::string logging_filename_prefix_ = "log";

};
}
}
