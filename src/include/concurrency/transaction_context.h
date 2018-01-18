//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context.h
//
// Identification: src/include/concurrency/transaction_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog_cache.h"
#include "common/exception.h"
#include "common/item_pointer.h"
#include "common/printable.h"
#include "common/internal_types.h"

namespace peloton {

namespace trigger {
class TriggerSet;
class TriggerData;
}  // namespace trigger

namespace concurrency {

//===--------------------------------------------------------------------===//
// TransactionContext
//===--------------------------------------------------------------------===//

class TransactionContext : public Printable {
  TransactionContext(TransactionContext const &) = delete;

 public:
  TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id);

  TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id, const cid_t &commit_id);

  ~TransactionContext();

 private:
  void Init(const size_t thread_id, const IsolationLevelType isolation,
            const cid_t &read_id) {
    Init(thread_id, isolation, read_id, read_id);
  }

  void Init(const size_t thread_id, const IsolationLevelType isolation,
            const cid_t &read_id, const cid_t &commit_id);

 public:
  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  inline size_t GetThreadId() const { return thread_id_; }

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline cid_t GetReadId() const { return read_id_; }

  inline cid_t GetCommitId() const { return commit_id_; }

  inline eid_t GetEpochId() const { return epoch_id_; }

  inline void SetCommitId(const cid_t commit_id) { commit_id_ = commit_id; }

  inline void SetEpochId(const eid_t epoch_id) { epoch_id_ = epoch_id; }

  void RecordCreate(oid_t database_oid, oid_t table_oid, oid_t index_oid) {
    rw_object_set_.emplace_back(database_oid, table_oid, index_oid,
                                DDLType::CREATE);
  }

  void RecordDrop(oid_t database_oid, oid_t table_oid, oid_t index_oid) {
    rw_object_set_.emplace_back(database_oid, table_oid, index_oid,
                                DDLType::DROP);
  }

  void RecordRead(const ItemPointer &);

  void RecordReadOwn(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  // Return true if we detect INS_DEL
  bool RecordDelete(const ItemPointer &);

  RWType GetRWType(const ItemPointer &);

  void AddOnCommitTrigger(trigger::TriggerData &trigger_data);

  void ExecOnCommitTriggers();

  bool IsInRWSet(const ItemPointer &location) {
    oid_t tile_group_id = location.block;
    oid_t tuple_id = location.offset;

    if (rw_set_.find(tile_group_id) != rw_set_.end() &&
        rw_set_.at(tile_group_id).find(tuple_id) !=
            rw_set_.at(tile_group_id).end()) {
      return true;
    } else {
      return false;
    }
  }

  inline const ReadWriteSet &GetReadWriteSet() { return rw_set_; }
  inline const CreateDropSet &GetCreateDropSet() { return rw_object_set_; }

  inline std::shared_ptr<GCSet> GetGCSetPtr() { return gc_set_; }

  inline std::shared_ptr<GCObjectSet> GetGCObjectSetPtr() {
    return gc_object_set_;
  }

  inline bool IsGCSetEmpty() { return gc_set_->size() == 0; }

  inline bool IsGCObjectSetEmpty() { return gc_object_set_->size() == 0; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Set result and status
  inline void SetResult(ResultType result) { result_ = result; }

  // Get result and status
  inline ResultType GetResult() const { return result_; }

  inline bool IsReadOnly() const {
    return is_written_ == false && insert_count_ == 0;
  }

  inline IsolationLevelType GetIsolationLevel() const {
    return isolation_level_;
  }

  // cache for table catalog objects
  catalog::CatalogCache catalog_cache;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // transaction id
  txn_id_t txn_id_;

  // thread id
  size_t thread_id_;

  // read id
  // this id determines which tuple versions the transaction can access.
  cid_t read_id_;

  // commit id
  // this id determines the id attached to the tuple version written by the
  // transaction.
  cid_t commit_id_;

  // epoch id can be extracted from read id.
  // GC manager uses this id to check whether a version is still visible.
  eid_t epoch_id_;

  ReadWriteSet rw_set_;
  CreateDropSet rw_object_set_;

  // this set contains data location that needs to be gc'd in the transaction.
  std::shared_ptr<GCSet> gc_set_;
  std::shared_ptr<GCObjectSet> gc_object_set_;

  // result of the transaction
  ResultType result_ = ResultType::SUCCESS;

  bool is_written_;
  size_t insert_count_;

  IsolationLevelType isolation_level_;

  std::unique_ptr<trigger::TriggerSet> on_commit_triggers_;
};

}  // namespace concurrency
}  // namespace peloton
