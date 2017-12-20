
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
#include <algorithm>

#include "brain/sample.h"

namespace peloton {
namespace brain {

/**
 * Brain Utility Functions
 */
class BrainUtil {
 public:
  /**
   * Load Samples for from a file.
   * It's a vector because there could be more multiple samples per table.
   * TableName -> Sample
   */
  static std::unordered_map<std::string, std::vector<brain::Sample>> LoadSamplesFile(
      const std::string file_path) {
    std::unordered_map<std::string, std::vector<brain::Sample>> samples;

    // Parse the input file line-by-line
    std::ifstream infile(file_path);
    std::string line;
    while (std::getline(infile, line)) {
      if (line.empty()) continue;
      std::istringstream iss(line);

      // FORMAT: <NAME> <WEIGHT> <NUM_COLS> <COLUMNS...>
      // TODO: Need include SAMPLE_TYPE
      std::string name;
      double weight;
      int num_cols;
      std::vector<double> columns;

      iss >> name >> weight >> num_cols;
      for (int i = 0; i < num_cols; i++) {
        int col;
        iss >> col;
        columns.push_back(col);
      }
      std::transform(name.begin(), name.end(), name.begin(), ::tolower);

      brain::Sample sample(columns, weight, brain::SampleType::ACCESS);
      samples[name].push_back(sample);

    }  // WHILE
    return (samples);
  }
};
}
}
