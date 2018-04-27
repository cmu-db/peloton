//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric_new.h
//
// Identification: src/statistics/abstract_metric_new.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/internal_types.h"
#include "common/printable.h"
#include "statistics/stat_insertion_point.h"

namespace peloton {
namespace stats {

// Any metric that derives from this class will override
// whatever insertion point methods it needs
class AbstractMetricNew : public Printable {
 public:
  virtual ~AbstractMetricNew() = default;

  virtual void OnTransactionBegin() {};

  virtual void OnTransactionCommit(UNUSED_ATTRIBUTE oid_t db_id) {};

  virtual void OnTransactionAbort(UNUSED_ATTRIBUTE oid_t db_id) {};

  virtual void OnTupleRead() {};

  virtual void OnTupleUpdate() {};

  virtual void OnTupleInsert() {};

  virtual void OnTupleDelete() {};

  virtual void OnIndexRead() {};

  virtual void OnIndexUpdate() {};

  virtual void OnIndexInsert() {};

  virtual void OnIndexDelete() {};

  virtual void OnQueryBegin() {};

  virtual void OnQueryEnd() {};

  virtual void CollectIntoCatalog() = 0;
};

}  // namespace stats
}  // namespace peloton
