//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpwo_txn_manager.h
//
// Identification: src/backend/concurrency/rpwo_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

extern thread_local std::unordered_map<oid_t, std::unordered_map<oid_t, bool>>
    rpwo_released_rdlock;

class RpwoTxnManager : public TransactionManager {
 public:
  RpwoTxnManager() {}
  virtual ~RpwoTxnManager() {}

  static RpwoTxnManager &GetInstance();

  virtual bool IsVisible(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader * const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id);

  virtual bool AcquireLock(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual void PerformDelete(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual void PerformUpdate(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

 private:
#define MAX_READER_COUNT 0xFFFFFFFFFFFFFFFF
#define READER_COUNT_MASK 0xFF
#define WRITER_ID_MASK 0x00FFFFFFFFFFFFFF

  inline txn_id_t GET_WRITER_ID(txn_id_t tuple_txn_id) {
    return txn_id_t(tuple_txn_id & WRITER_ID_MASK);
  }

  inline int GET_READER_COUNT(txn_id_t tuple_txn_id) {
    return int((tuple_txn_id >> 56) & READER_COUNT_MASK);
  }

  inline txn_id_t PACK_TXN_ID(txn_id_t writer_id, int read_count) {
    return txn_id_t(((long)(read_count & READER_COUNT_MASK) << 56) | (writer_id & WRITER_ID_MASK));
  }

  // inline txn_id_t EXTRACT_TXNID(txn_id_t txn_id) { return txn_id & TXNID_MASK; }
  // inline int EXTRACT_READ_COUNT(txn_id_t txn_id) {
  //   return int((txn_id >> 56) & READ_COUNT_MASK);
  // }

  bool ReleaseReadLock(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);
};
}
}