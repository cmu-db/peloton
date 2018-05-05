#pragma once
#include <boost/functional/hash.hpp>
#include "common/printable.h"

namespace peloton {
namespace stats {
/**
 * @brief An always-consistent storage unit for intermediate stats results.
 *
 * These objects hold raw data points processed by a metric on the thread-local
 * level. Entries into this object must be always consistent. (i.e. future
 *entries
 * should not rely on some early entries being in this object) This is because
 * an aggregator can come at any time and swap out the object for aggregation.
 *
 * @see Metric for detailed description of how this would work.
 */
class AbstractRawData : public Printable {
 public:
  /**
   * Given another AbstractRawData classes, combine the other's content with the
   * content of this one. It is guaranteed that nobody will have access to the
   * other object at this point or after.
   * @param other The other AbstractRawData to be merged
   */
  virtual void Aggregate(AbstractRawData &other) = 0;
  /**
   * Persist the content of this RawData into the Catalog. Expect this object
   * to be garbage-collected after this method is called.
   */
  virtual void WriteToCatalog() = 0;

  /**
   * Fetch additional data based on current stats collections
   */
  virtual void FetchData(){};

 protected:
  struct pair_hash {
    template <class T1, class T2>
    inline std::size_t operator()(const std::pair<T1, T2> &p) const {
      size_t seed = 0;
      boost::hash_combine(seed, p.first);
      boost::hash_combine(seed, p.second);
      return seed;
    }
  };
};
}  // namespace stats
}  // namespace peloton
