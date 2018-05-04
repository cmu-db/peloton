#pragma once
#include <boost/functional/hash.hpp>
#include "common/printable.h"

namespace peloton {
namespace stats {
class AbstractRawData : public Printable {
 public:
  virtual void Aggregate(AbstractRawData &other) = 0;
  virtual void WriteToCatalog() = 0;

 protected:
  struct pair_hash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &p) const {
      size_t seed = 0;
      boost::hash_combine(seed, p.first);
      boost::hash_combine(seed, p.second);

      return seed;
    }
  };
};
}  // namespace stats
}  // namespace peloton
