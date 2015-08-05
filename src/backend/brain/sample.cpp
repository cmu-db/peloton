//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// sample.cpp
//
// Identification: src/backend/brain/sample.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <cmath>
#include <iostream>

#include "backend/brain/sample.h"

namespace peloton {
namespace brain {

double Sample::GetDistance(const Sample &other) const {
  double dist = 0;

  // Absolute difference
  size_t column_itr = 0;
  for (auto column : columns_accessed_) {
    auto other_val = other.columns_accessed_[column_itr++];
    dist += std::abs(column - other_val);
  }

  return dist;
}

Sample Sample::GetDifference(const Sample &other) const {
  std::vector<double> columns_accessed;

  size_t column_itr = 0;
  for (auto column : columns_accessed_) {
    auto other_val = other.columns_accessed_[column_itr++];
    columns_accessed.push_back(column - other_val);
  }

  auto diff = Sample(columns_accessed);

  return diff;
}

Sample &Sample::operator*(const double &rhs) {
  size_t column_itr;
  size_t column_count = columns_accessed_.size();

  for (column_itr = 0; column_itr < column_count; column_itr++) {
    this->columns_accessed_[column_itr] *= rhs;
  }

  return *this;
}

Sample &Sample::operator+(const Sample &rhs) {
  size_t column_itr;
  size_t column_count = columns_accessed_.size();

  for (column_itr = 0; column_itr < column_count; column_itr++) {
    auto other_val = rhs.columns_accessed_[column_itr];
    this->columns_accessed_[column_itr] += other_val;
  }

  return *this;
}

std::vector<oid_t> Sample::GetEnabledColumns() const {
  std::vector<oid_t> enabled_columns;

  oid_t column_itr = 0;
  for (auto column : columns_accessed_) {
    if (std::round(column) == 1.0) enabled_columns.push_back(column_itr);
    column_itr++;
  }

  return enabled_columns;
}

std::ostream &operator<<(std::ostream &os, const Sample &sample) {
  os << "Sample :: ";

  for (auto column : sample.columns_accessed_) os << std::round(column) << " ";
  os << "\n";

  return os;
}

}  // End brain namespace
}  // End peloton namespace
