//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_range_expression.h
//
// Identification: src/backend/expression/hash_range_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/common/abstract_tuple.h"

#include <string>
#include <sstream>
#include <boost/scoped_array.hpp>

namespace peloton {
namespace expression {

typedef std::pair<int32_t, int32_t> srange_type;

class HashRangeExpression : public AbstractExpression {
 public:
  HashRangeExpression(int value_idx, srange_type *ranges, int num_ranges )
 : AbstractExpression(EXPRESSION_TYPE_HASH_RANGE), value_idx(value_idx), ranges(ranges), num_ranges(num_ranges)
 {
    VOLT_TRACE("HashRangeExpression %d %d", m_type, value_idx);
    for (int ii = 0; ii < num_ranges; ii++) {
      if (ii > 0) {
        if (ranges[ii - 1].first >= ranges[ii].first) {
          throwFatalException("Ranges overlap or are out of order");
        }
        if (ranges[ii - 1].second > ranges[ii].first) {
          throwFatalException("Ranges overlap or are out of order");
        }
      }
      if (ranges[ii].first > ranges[ii].second) {
        throwFatalException("Range begin is > range end, we don't support spanning Long.MAX to Long.MIN");
      }
    }
 };

  virtual Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2) const {
    assert(tuple1);
    if ( ! tuple1 ) {
      throw Exception(
          "TupleValueExpression::"
          "Evaluate:"
          " Couldn't find tuple 1 (possible index scan planning error)");
    }
    const int32_t hash = tuple1->GetValue(this->value_idx).murmurHash3();

    return binarySearch(hash);
  }

  Value binarySearch(const int32_t hash) const {
    //The binary search blows up on only one range
    if (num_ranges == 1) {
      if (hash >= ranges[0].first && hash <= ranges[0].second) return Value::getTrue();
      return Value::getFalse();
    }

    /*
     * Bottom of a range is inclusive as well as the top. Necessary because we no longer support wrapping
     * from Integer.MIN_VALUE
     * Doing a binary search, is just a hair easier than std::lower_bound
     */
    int32_t min = 0;
    int32_t max = num_ranges - 1;
    while (min <= max) {
      assert(min >= 0);
      assert(max >= 0);
      uint32_t mid = (min + max) >> 1;
      if (ranges[mid].second < hash) {
        min = mid + 1;
      } else if (ranges[mid].first > hash) {
        max = mid - 1;
      } else {
        return Value::getTrue();
      }
    }

    return Value::getFalse();
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "Hash range expression on column[" << this->value_idx << "]\n";
    buffer << "ranges \n";
    for (int ii = 0; ii < num_ranges; ii++) {
      buffer << "start " << ranges[ii].first << " end " << ranges[ii].second << std::endl;
    }
    return (buffer.str());
  }

  int getColumnId() const {return this->value_idx;}

 private:
  const int value_idx;           // which (offset) column of the tuple
  boost::scoped_array<srange_type> ranges;
  const int num_ranges;
};

}  // End expression namespace
}  // End peloton namespace
