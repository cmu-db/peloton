//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rowo_txn_manager.h
//
// Identification: src/backend/concurrency/to_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {

class ToTxnManager : public TransactionManager {
 public:
  ToTxnManager() {}

  virtual ~ToTxnManager() {}

  static ToTxnManager &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id);

  virtual bool IsAccessable(storage::TileGroup *tile_group,
                            const oid_t &tuple_id);

  virtual bool AcquireLock(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id);

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
};
}
}