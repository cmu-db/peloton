
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// brain_util.h
//
// Identification: /peloton/src/include/brain/brain_util.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "brain/sample.h"

namespace peloton {
namespace brain {

/**
 * Brain Utility Functions
 */
class BrainUtil {
 public:
  /**
   * Load Samples for from a file
   */
  static std::map<std::string, brain::Sample> LoadSamplesFile(
      const std::string file_path) {
    std::map<std::string, brain::Sample> samples;

    // Parse the input file line-by-line
    std::ifstream infile(file_path);
    std::string line;
    while (std::getline(infile, line)) {
      if (line.empty()) continue;
      std::istringstream iss(line);

      // FORMAT: <NAME> <WEIGHT> <METRIC> <NUM_COLS> <COLUMNS...>
      // TODO: Need include SAMPLE_TYPE
      std::string name;
      double weight;
      double metric = 0;
      int num_cols;
      std::vector<double> columns;

      iss >> name >> weight >> metric >> num_cols;
      for (int i = 0; i < num_cols; i++) {
        int col;
        iss >> col;
        columns.push_back(col);
      }

      brain::Sample sample(columns, weight, brain::SAMPLE_TYPE_ACCESS, metric);
      samples.insert(
          std::map<std::string, brain::Sample>::value_type(name, sample));

    }  // WHILE
    return (samples);
  }
};
}
}
