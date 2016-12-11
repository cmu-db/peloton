//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sample.cpp
//
// Identification: src/brain/sample.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <common/macros.h>
#include <cmath>
#include <iostream>
#include <sstream>

#include "brain/sample.h"
#include "common/logger.h"

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

  PL_ASSERT(rhs.columns_accessed_.size() == column_count);

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

const std::string Sample::GetInfo() const {
  std::ostringstream os;

  os << "Sample :: ";

  for (auto column_value : columns_accessed_) os << column_value << " ";

  os << "  ::  " << std::round(metric_);

  return os.str();
}

bool Sample::operator==(const Sample &other) const {
  auto sample_size = columns_accessed_.size();

  for (oid_t sample_itr = 0; sample_itr < sample_size; sample_itr++) {
    if (columns_accessed_[sample_itr] != other.columns_accessed_[sample_itr]) {
      return false;
    }
  }

  return true;
}

}  // End brain namespace
}  // End peloton namespace
