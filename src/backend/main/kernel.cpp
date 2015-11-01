//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// kernel.cpp
//
// Identification: src/backend/main/kernel.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <cstdio>
#include <vector>

#include "backend/main/kernel.h"
#include "backend/common/logger.h"

namespace peloton {
namespace backend {

int size = 10000000;
int chunk_size = 100000;
int *data;

class table_iterator_task {
 public:
  table_iterator_task(int l) : num_tilegroups(l), next_tilegroup(0) {}

  bool operator()(int &v) {
    if (next_tilegroup < num_tilegroups) {
      v = next_tilegroup++;
      return true;
    } else {
      return false;
    }
  }

 private:
  const int num_tilegroups;
  int next_tilegroup;
};

int predicate() {
  int sum = 0;
  for (auto ii = 0; ii < 1000; ii++) sum += ii;
  return sum;
}

class seq_scanner_task {
 public:
  std::vector<int> operator()(const int &v) const {
    std::vector<int> matching;

    int offset = v * chunk_size;
    int end = offset + chunk_size;

    for (auto ii = offset; ii < end; ii++)
      if (data[ii] % 5 == 0 && predicate()) matching.push_back(ii);

    return matching;
  }
};

class summer_task {
 public:
  int operator()(const std::vector<int> &matching) const {
    long local_sum = 0;
    for (auto ii : matching) local_sum += data[ii];

    return local_sum;
  }
};

long long sum = 0;

class aggregator_task {
 public:
  int operator()(const int &local_sum) const {
    sum += local_sum;
    return sum;
  }
};

Result Kernel::Handler(__attribute__((unused)) const char *query) {
  Result status = RESULT_INVALID;
  status = RESULT_SUCCESS;
  return status;
}

}  // namespace backend
}  // namespace peloton
