//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_min_sketch.h
//
// Identification: src/include/optimizer/stats/count_min_sketch.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "murmur3/MurmurHash3.h"

#include <cmath>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <cinttypes>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Count-min Sketch
//===--------------------------------------------------------------------===//
class CountMinSketch {
 public:
  using SketchElemType = uint64_t;

  const int depth;
  const int width;
  // eps: error range 0.01 < ep < 1
  const double eps;
  // gamma: probability of error (the smaller the better) 0 < gamma < 1
  const double gamma;
  uint64_t size;
  std::vector<std::vector<SketchElemType>> table;
  std::vector<SketchElemType> row_hash;

  // Constructor with specific sketch size.
  CountMinSketch(int depth, int width, unsigned int seed)
      : depth{depth},
        width{width},
        eps{exp(1) / width},
        gamma{exp(-depth)},
        size{0} {
    InitTable(depth, width, seed);
  }

  // Constructor with specific error bound.
  CountMinSketch(double eps, double gamma, unsigned int seed)
      : depth{(int)ceil(log(1 / gamma))},
        width{(int)ceil(exp(1) / eps)},
        eps{eps},
        gamma{gamma},
        size{0} {
    InitTable(depth, width, seed);
  }

  void Add(int64_t item, int count = 1) {
    std::vector<int> bins = getHashBins(item);
    uint64_t former_min = UINT64_MAX;
    for (int i = 0; i < depth; i++) {
      former_min = std::min(table[i][bins[i]], former_min);
      table[i][bins[i]] += count;
    }
    if (former_min == 0) {
      ++size;
    }
  }

  void Add(const char* item, int count = 1) {
    std::vector<int> bins = getHashBins(item);
    uint64_t former_min = UINT64_MAX;
    for (int i = 0; i < depth; i++) {
      former_min = std::min(table[i][bins[i]], former_min);
      table[i][bins[i]] += count;
    }
    if (former_min == 0) {
      ++size;
    }
  }

  void Remove(int64_t item, unsigned int count = 1) {
    std::vector<int> bins = getHashBins(item);
    uint64_t former_min = UINT64_MAX, latter_min = UINT64_MAX;
    for (int i = 0; i < depth; i++) {
      former_min = std::min(table[i][bins[i]], former_min);
      if (table[i][bins[i]] > count)
        table[i][bins[i]] -= count;
      else
        table[i][bins[i]] = 0;

      latter_min = std::min(latter_min, table[i][bins[i]]);
    }
    if (former_min != 0 && latter_min == 0) {
      --size;
    }
  }

  void Remove(const char* item, unsigned int count = 1) {
    std::vector<int> bins = getHashBins(item);
    uint64_t former_min = UINT64_MAX, latter_min = UINT64_MAX;
    for (int i = 0; i < depth; i++) {
      former_min = std::min(table[i][bins[i]], former_min);
      if (table[i][bins[i]] > count)
        table[i][bins[i]] -= count;
      else
        table[i][bins[i]] = 0;

      latter_min = std::min(latter_min, table[i][bins[i]]);
    }
    if (former_min != 0 && latter_min == 0) {
      --size;
    }
  }

  uint64_t EstimateItemCount(int64_t item) {
    uint64_t count = UINT64_MAX;
    std::vector<int> bins = getHashBins(item);
    for (int i = 0; i < depth; i++) {
      count = std::min(count, table[i][bins[i]]);
    }
    LOG_TRACE("Item count: %" PRIu64, count);
    return count;
  }

  uint64_t EstimateItemCount(const char* item) {
    uint64_t count = UINT64_MAX;
    std::vector<int> bins = getHashBins(item);
    for (int i = 0; i < depth; i++) {
      count = std::min(count, table[i][bins[i]]);
    }
    LOG_TRACE("Item count: %" PRIu64, count);
    return count;
  }

 private:
  void InitTable(int depth, int width, unsigned int seed) {
    table = std::vector<std::vector<SketchElemType>>(
        depth, std::vector<SketchElemType>(width));
    std::srand(seed);
    for (int i = 0; i < depth; i++) {
      row_hash.push_back(std::rand());
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
};

} /* namespace optimizer */
} /* namespace peloton */
