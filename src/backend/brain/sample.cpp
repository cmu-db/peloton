/*-------------------------------------------------------------------------
 *
 * sample.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/brain/sample.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <sstream>
#include <cmath>

#include "backend/brain/sample.h"

namespace peloton {
namespace brain {

double Sample::GetDistance(const Sample& other, bool absolute) const {

  double dist = 0;

  // Absolute difference
  if(absolute) {
    size_t column_itr = 0;
    for(auto column : columns_accessed_) {
      auto other_val = other.columns_accessed_[column_itr++];
      dist += std::abs(column - other_val);
    }
  }
  // Direct difference
  else {
    size_t column_itr = 0;
    for(auto column : columns_accessed_) {
      auto other_val = other.columns_accessed_[column_itr++];
      dist += (column - other_val);
    }
  }

  return dist;
}

// addition operator
Sample& Sample::operator+(const double& rhs) {

  for(auto column : columns_accessed_)
    column += rhs;

  return *this;
}

std::ostream &operator<<(std::ostream &os, const Sample &sample) {

  os << "Sample :: ";

  for (auto column : sample.columns_accessed_)
    os << column << " ";

  os << " :: " << sample.weight_;

  return os;
}

}  // End brain namespace
}  // End peloton namespace
