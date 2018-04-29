#pragma once
#include "common/printable.h"

namespace peloton {
namespace stats {
class AbstractRawData : public Printable {
 public:
  virtual void Aggregate(AbstractRawData &other) = 0;
  virtual void WriteToCatalog() = 0;
};
}
}