/*-------------------------------------------------------------------------
 *
 * sample.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/brain/sample.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "backend/common/types.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// Sample
//===--------------------------------------------------------------------===//

class Sample {
 public:

  Sample(const size_t column_count) :
    columns_accessed_(std::vector<double>(column_count, 0)),
    weight_(0){
  }

  Sample(const std::vector<double>& columns_accessed,
         double weight)
      : columns_accessed_(columns_accessed),
        weight_(weight) {
  }

  // get the distance from sample
  double GetDistance(const Sample& other, bool absolute) const;

  // multiplication operator with a scalar
  Sample& operator+(const double& rhs);

  // Get a string representation of sample
  friend std::ostream& operator<<(std::ostream& os, const Sample& sample);

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
