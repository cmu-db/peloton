//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimistic_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {

class OptimisticTxnManager : public TransactionManager {
 public:
  OptimisticTxnManager() {}

  virtual ~OptimisticTxnManager() {}

  static OptimisticTxnManager &GetInstance();

  virtual bool IsVisible(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader * const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id);

  virtual bool AcquireOwnership(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);
  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);
  
  virtual void PerformDelete(const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();
};
}
}