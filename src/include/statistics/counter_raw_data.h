#pragma once

#include <vector>
#include <algorithm>
#include "statistics/abstract_raw_data.h"
#include "common/exception.h"

namespace peloton {
namespace stats {
template<std::string... names>
class CounterRawData : public AbstractRawData {
 public:
  CounterRawData() : counter_names_{names...},
                     counters_(counter_names_.size(), 0) {}

  size_t OffsetFromName(std::string name) {
    for (size_t offset = 0; offset < counter_names_.size(); offset++)
      if (counter_names_[offset] == name) return offset;
    throw StatException("Unknown counter name " + name);
  }

  void Increment(size_t offset) {
    MarkUnsafe();
    counters_[offset]++;
    MarkSafe();
  }

  void Aggregate(AbstractRawData &other) override {
    auto &other_counter = dynamic_cast<CounterRawData &>(other);
    for (size_t i = 0; i < counters_.size(); i++)
      counters_[i] += other_counter.counters_[i];
  }
  // TODO(tianyu): Implement me
  void WriteToCatalog() override {}

 private:
  std::vector<std::string> counter_names_;
  std::vector<uint64_t> counters_;
};
}
}
