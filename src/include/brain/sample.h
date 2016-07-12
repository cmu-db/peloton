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
#include "common/types.h"
#include "common/printable.h"

namespace peloton {
namespace brain {

#define DEFAULT_SAMPLE_WEIGHT 1.0
#define DEFAULT_COLUMN_VALUE 0.5

enum SampleType {
  SAMPLE_TYPE_INVALID = 0,

  SAMPLE_TYPE_ACCESS = 1,       // accessed attributes
  SAMPLE_TYPE_UPDATE = 2        // updated attributes
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
         SampleType sample_type = SAMPLE_TYPE_ACCESS)
      : columns_accessed_(columns_accessed), weight_(weight), sample_type_(sample_type) {}

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

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // column accessed bitmap
  std::vector<double> columns_accessed_;

  // weight of the sample
  double weight_;

  // type of sample
  SampleType sample_type_;
};

}  // End brain namespace
}  // End peloton namespace
