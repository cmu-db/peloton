//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// sample.h
//
// Identification: src/backend/brain/sample.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/common/types.h"

namespace peloton {
namespace brain {

#define DEFAULT_SAMPLE_WEIGHT 1.0
#define DEFAULT_COLUMN_VALUE 0.5

//===--------------------------------------------------------------------===//
// Sample
//===--------------------------------------------------------------------===//

class Sample {
 public:
  Sample(const size_t column_count)
      : columns_accessed_(
            std::vector<double>(column_count, DEFAULT_COLUMN_VALUE)),
        weight_(DEFAULT_SAMPLE_WEIGHT) {}

  Sample(const std::vector<double> &columns_accessed,
         double weight = DEFAULT_SAMPLE_WEIGHT)
      : columns_accessed_(columns_accessed), weight_(weight) {}

  // get the distance from other sample
  double GetDistance(const Sample &other) const;

  // get difference after removing other sample
  Sample GetDifference(const Sample &other) const;

  // multiplication operator with a scalar
  Sample &operator*(const double &rhs);

  // addition operator with a sample
  Sample &operator+(const Sample &rhs);

  // get enabled columns
  std::vector<oid_t> GetEnabledColumns() const;

  // Get a string representation of sample
  friend std::ostream &operator<<(std::ostream &os, const Sample &sample);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // column accessed bitmap
  std::vector<double> columns_accessed_;

  // weight of the sample
  double weight_;
};

}  // End brain namespace
}  // End peloton namespace
