//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// config_enumeration.h
//
// Identification: src/include/brain/config_enumeration.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "brain/configuration.h"


namespace peloton {
namespace brain {


  class ConfigEnumeration {

 public:
  /**
   * @brief Constructor
   */
  ConfigEnumeration(int num_indexes)
      : intial_size_(0), optimal_size_(num_indexes) {}


  Configuration getBestIndexes(Configuration c, std::vector<std::string> w);

  

 private:

  /**
   * @brief Helper function to build the index from scratch
   */
  // void Greedy(Configuration c, std::vector<string> w);

  // the initial size for which exhaustive enumeration happens
  int intial_size_;
  // the optimal number of index configuations
  int optimal_size_;

  };



}  // namespace brain
}  // namespace peloton