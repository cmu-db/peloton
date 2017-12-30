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
#include "common/internal_types.h"

namespace peloton {
namespace brain {

#define DEFAULT_SAMPLE_WEIGHT 1.0
#define DEFAULT_COLUMN_VALUE 0.5
#define DEFAULT_METRIC_VALUE 0

enum class SampleType {
  INVALID = 0,
  ACCESS = 1,  // accessed attributes
  UPDATE = 2   // updated attributes
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
        sample_type_(SampleType::ACCESS) {}

  Sample(const std::vector<double> &columns_accessed,
         double weight = DEFAULT_SAMPLE_WEIGHT,
         SampleType sample_type = SampleType::ACCESS)
      : columns_accessed_(columns_accessed),
        weight_(weight),
        sample_type_(sample_type) {}

  // get the distance from other sample
  double GetDistance(const Sample &other) const;

  // get difference after removing other sample
  Sample GetDifference(const Sample &other) const;

  // multiplication operator with a scalar
  Sample &operator*(const double &rhs);

  // addition operator with a sample
  Sample &operator+(const Sample &rhs);

  // the sample's weight
  inline double GetWeight() const { return (weight_); }

  // the sample type
  inline SampleType GetSampleType() const { return (sample_type_); }

  inline const std::vector<double> GetColumnsAccessed() const {
    return (columns_accessed_);
  }

  // set the columns accessed
  inline void SetColumnsAccessed(const std::vector<double> columns_accessed) {
    columns_accessed_ = columns_accessed;
  }

  // get enabled columns
  std::vector<oid_t> GetEnabledColumns() const;

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Convert this sample into a parseable string
  const std::string ToString() const;

  bool operator==(const Sample &other) const;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

 private:
  // column accessed bitmap
  std::vector<double> columns_accessed_;

  // weight of the sample
  double weight_;

  // type of sample
  SampleType sample_type_;

};

}  // namespace brain
}  // namespace peloton

namespace std {

template <>
struct hash<peloton::brain::Sample> {
  size_t operator()(const peloton::brain::Sample &sample) const {
    // Compute individual hash values using XOR and bit shifting:
    long hash = 31;
    auto columns = sample.GetColumnsAccessed();
    auto sample_size = columns.size();
    for (size_t sample_itr = 0; sample_itr < sample_size; sample_itr++) {
      hash *= (columns[sample_itr] + 31);
    }

    return hash;
  }
};
}
