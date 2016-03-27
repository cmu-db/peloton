//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/optimistic_transaction_manager_reverse.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {

class OptimisticTransactionManagerReverse : public TransactionManager {
 public:
  OptimisticTransactionManagerReverse() {}

  virtual ~OptimisticTransactionManagerReverse() {}

  static OptimisticTransactionManagerReverse &GetInstance();

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid);

  virtual bool IsOwner(storage::TileGroup *tile_group, const oid_t &tuple_id);

  virtual bool IsAccessable(storage::TileGroup *tile_group, const oid_t &tuple_id);

  virtual bool AcquireTuple(storage::TileGroup *tile_group, const oid_t &physical_tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id, const ItemPointer &new_location);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id, const ItemPointer &new_location);

  virtual void SetDeleteVisibility(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetUpdateVisibility(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetInsertVisibility(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

};
}
}