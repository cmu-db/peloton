
#pragma once

#include "common/logger.h"

#include <murmur3/MurmurHash3.h>

#include <cmath>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <cinttypes>

namespace peloton{
namespace optimizer{

class CountMinSketch {
 public:

  using SketchElemType = uint64_t;

  const int depth;
  const int width;
  const double eps;
  const double confidence;
  uint64_t size;
  std::vector<std::vector<SketchElemType>> table;
  std::vector<SketchElemType> row_hash;

  CountMinSketch(int depth, int width, unsigned int seed) :
      depth{depth},
      width{width},
      eps{2.0 / width},
      confidence{1 - 1 / pow(2, depth)},
      size{0} {
    InitTable(depth, width, seed);
    PrintSketch();
  }

  CountMinSketch(double eps, double confidence, unsigned int seed) :
      depth{(int) ceil(-log(1 - confidence) / log(2))},
      width{(int) ceil(2 / eps)},
      eps{eps},
      confidence{confidence},
      size{0} {
    InitTable(depth, width, seed);
    PrintSketch();
  }

  // Have two different method headers for integer and string
  // in case later we need to change only one of them
  void Add(int64_t item, int count = 1) {
    std::vector<int> bins = getHashBins(item);
    for (int i = 0; i < depth; i++) {
      table[i][bins[i]] += count;
    }
  }

  void Add(const char* item, int count = 1) {
    std::vector<int> bins = getHashBins(item);
    for (int i = 0; i < depth; i++) {
      table[i][bins[i]] += count;
    }
  }

  uint64_t EstimateItemCount(int64_t item) {
      uint64_t count = UINT64_MAX;
      std::vector<int> bins = getHashBins(item);
      for (int i = 0; i < depth; i++) {
        count = std::min(count, table[i][bins[i]]);
      }
      LOG_INFO("Item count is: %d", (int) count);
      return count;
  }

  uint64_t EstimateItemCount(const char* item) {
      uint64_t count = UINT64_MAX;
      std::vector<int> bins = getHashBins(item);
      for (int i = 0; i < depth; i++) {
        count = std::min(count, table[i][bins[i]]);
      }
      LOG_INFO("Item count is: %d", (int) count);
      return count;
  }

 private:

  void InitTable(int depth, int width, unsigned int seed) {
    table = std::vector<std::vector<SketchElemType>>(depth, std::vector<SketchElemType>(width));
    std::srand(seed);
    for (int i = 0; i < depth; i++) {
      row_hash.push_back(std::rand()); // change RAND_MAX to define different max value
    }
  }

  std::vector<int> getHashBins(int64_t item) {
    std::vector<int> bins;
    int32_t h1 = MurmurHash3_x64_128(item, 0);
    int32_t h2 = MurmurHash3_x64_128(item, h1);
    for (int i = 0; i < depth; i++) {
      bins.push_back(abs((h1 + i * h2) % width));
    }
    return bins;
  }

  std::vector<int> getHashBins(const char* item) {
    std::vector<int> bins;
    int32_t h1 = MurmurHash3_x64_128(item, strlen(item), 0);
    int32_t h2 = MurmurHash3_x64_128(item, strlen(item), h1);
    for (int i = 0; i < depth; i++) {
      bins.push_back(abs((h1 + i * h2) % width));
    }
    return bins;
  }

  void PrintSketch() {
    LOG_INFO("\n {CountMinSketch} depth[%d] width[%d] eps[%f] confidence[%f] size[%d]",
             depth, width, eps, confidence, (int) size);
  }

};

} /* namespace optimizer */
} /* namespace peloton */
