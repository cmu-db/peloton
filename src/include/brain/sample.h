//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sample.h
//
// Identification: src/include/brain/sample.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/printable.h"
#include "common/printable.h"
#include "common/types.h"

namespace peloton {
namespace brain {

#define DEFAULT_SAMPLE_WEIGHT 1.0
#define DEFAULT_COLUMN_VALUE 0.5
#define DEFAULT_METRIC_VALUE 0

enum SampleType {
  SAMPLE_TYPE_INVALID = 0,

  SAMPLE_TYPE_ACCESS = 1,  // accessed attributes
  SAMPLE_TYPE_UPDATE = 2   // updated attributes
};

//===--------------------------------------------------------------------===//
// Sample
//===--------------------------------------------------------------------===//

class Sample : public Printable {
 public:
  Sample(const size_t column_count)
      : columns_accessed_(
            std::vector<double>(column_count, DEFAULT_COLUMN_VALUE)),
        weight_(DEFAULT_SAMPLE_WEIGHT),
        sample_type_(SAMPLE_TYPE_ACCESS) {}

  Sample(const std::vector<double> &columns_accessed,
         double weight = DEFAULT_SAMPLE_WEIGHT,
         SampleType sample_type = SAMPLE_TYPE_ACCESS,
         double metric = DEFAULT_METRIC_VALUE)
      : columns_accessed_(columns_accessed),
        weight_(weight),
        sample_type_(sample_type),
        metric_(metric) {}

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

  // Get a string representation for debugging
  const std::string GetInfo() const;

  bool operator==(const Sample &other) const;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // column accessed bitmap
  std::vector<double> columns_accessed_;

  // weight of the sample
  double weight_;

  // type of sample
  SampleType sample_type_;

  // more info about the sample
  double metric_ = 0;
};

}  // End brain namespace
}  // End peloton namespace

namespace std {

template <>
struct hash<peloton::brain::Sample> {
  size_t operator()(const peloton::brain::Sample &sample) const {
    // Compute individual hash values using XOR and bit shifting:
    long hash = 31;
    auto sample_size = sample.columns_accessed_.size();
    for (size_t sample_itr = 0; sample_itr < sample_size; sample_itr++) {
      hash *= (sample.columns_accessed_[sample_itr] + 31);
    }

    return hash;
  }
};
}
