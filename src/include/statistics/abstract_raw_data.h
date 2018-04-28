#pragma once
#include "common/printable.h"

namespace peloton {
namespace stats {
class AbstractRawData : public Printable {
 public:
  virtual void Aggregate(AbstractRawData &other) = 0;
  virtual void WriteToCatalog() = 0;
  // This will only be called by the aggregating thread
  inline bool SafeToAggregate() { return safe_; }
  // These methods would only be called by the collecting thread
  inline void MarkSafe() { safe_ = true; }
  inline void MarkUnsafe() { safe_ = false; }
 private:
  bool safe_;
};
}
}