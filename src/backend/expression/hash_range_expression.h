//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_range_expression.h
//
// Identification: src/backend/expression/hash_range_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/common/abstract_tuple.h"
#include <iostream>
#include <string>
#include <sstream>
#include <boost/scoped_array.hpp>

namespace peloton {
namespace expression {

typedef std::pair<int32_t, int32_t> srange_type;

class HashRangeExpression : public AbstractExpression {
 public:
  HashRangeExpression(int value_idx, std::vector<srange_type> &ranges)
      : AbstractExpression(EXPRESSION_TYPE_HASH_RANGE, VALUE_TYPE_BOOLEAN),
        value_idx_(value_idx),
        ranges_(ranges) {
    LOG_TRACE("HashRangeExpression %d", value_idx_);
    for (size_t ii = 0; ii < ranges_.size(); ii++) {
      if (ii > 0) {
        if (ranges_[ii - 1].first >= ranges_[ii].first) {
          throw Exception("Ranges overlap or are out of order");
        }
        if (ranges_[ii - 1].second > ranges_[ii].first) {
          throw Exception("Ranges overlap or are out of order");
        }
      }
      if (ranges_[ii].first > ranges_[ii].second) {
        throw Exception(
            "Range begin.Is > range end, we don't support spanning Long.MAX to "
            "Long.MIN");
      }
    }
  }

  virtual Value Evaluate(const AbstractTuple *tuple1,
                         UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                         UNUSED_ATTRIBUTE
                         executor::ExecutorContext *context) const override {
    PL_ASSERT(tuple1);
    if (!tuple1) {
      throw Exception(
          "TupleValueExpression::"
          "Evaluate:"
          " Couldn't find tuple 1 (possible index scan planning error)");
    }
    const int32_t hash = tuple1->GetValue(value_idx_).MurmurHash3();

    return BinarySearch(hash);
  }

  Value BinarySearch(const int32_t hash) const {
    // The binary search blows up on only one range
    if (ranges_.size() == 1) {
      if (hash >= ranges_[0].first && hash <= ranges_[0].second)
        return Value::GetTrue();
      return Value::GetFalse();
    }

    /*
     * Bottom of a range.Is inclusive as well as the top. Necessary because we
     * no longer support wrapping
     * from Integer.MIN_VALUE
     * Doing a binary search,.Is just a hair easier than std::lower_bound
     */
    int32_t min = 0;
    int32_t max = ranges_.size() - 1;
    while (min <= max) {
      PL_ASSERT(min >= 0);
      PL_ASSERT(max >= 0);
      uint32_t mid = (min + max) >> 1;
      if (ranges_[mid].second < hash) {
        min = mid + 1;
      } else if (ranges_[mid].first > hash) {
        max = mid - 1;
      } else {
        return Value::GetTrue();
      }
    }

    return Value::GetFalse();
  }

  std::string DebugInfo(const std::string &spacer) const override {
    std::ostringstream buffer;
    buffer << spacer << "Hash range expression on column[" << value_idx_
           << "]\n";
    buffer << "ranges: \n";
    for (size_t ii = 0; ii < ranges_.size(); ii++) {
      buffer << "start " << ranges_[ii].first << " end " << ranges_[ii].second
             << std::endl;
    }
    return buffer.str();
  }

  int GetColumnId() const { return value_idx_; }

  AbstractExpression *Copy() const override {
    std::vector<srange_type> copied_ranges;
    for (size_t i = 0; i < ranges_.size(); i++) {
      copied_ranges[i] = ranges_[i];
    }
    return new HashRangeExpression(value_idx_, copied_ranges);
  }

 private:
  int value_idx_;  // which (offset) column of the tuple
  std::vector<srange_type> ranges_;
};

}  // namespace expression
}  // namespace peloton
