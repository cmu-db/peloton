//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ts_order_txn_manager.h
//
// Identification: src/backend/concurrency/ts_order_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// timestamp ordering
//===--------------------------------------------------------------------===//

class TsOrderTxnManager : public TransactionManager {
 public:
  TsOrderTxnManager() {}

  virtual ~TsOrderTxnManager() {}

  static TsOrderTxnManager &GetInstance();

  virtual bool IsVisible(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader * const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tuple_id);

  virtual bool AcquireOwnership(const storage::TileGroupHeader * const tile_group_header,
                            const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetOwnership(const oid_t &tile_group_id,
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

private:
  inline cid_t GetLastReaderCid(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id) {
    char *reserved_field = tile_group_header->GetReservedFieldRef(tuple_id);
    cid_t read_ts = 0;
    memcpy(&read_ts, reserved_field, sizeof(cid_t));
    return read_ts;
  }

  inline void SetLastReaderCid(const storage::TileGroupHeader * const tile_group_header, const oid_t &tuple_id, const cid_t &last_read_ts) {
    char *reserved_field = tile_group_header->GetReservedFieldRef(tuple_id);
    cid_t read_ts = 0;
    memcpy(&read_ts, reserved_field, sizeof(cid_t));
    if (last_read_ts > read_ts) {
      memcpy(reserved_field, &last_read_ts, sizeof(cid_t));
    }
  }


};

}
}