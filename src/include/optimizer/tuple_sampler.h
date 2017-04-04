//===----------------------------------------------------------------------===//
////
////                         Peloton
////
//// tuple_sampler.h
////
//// Identification: src/include/optimizer/tuple_sampler.h
////
//// Copyright (c) 2015-16, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"

namespace peloton {
namespace storage {
    class DataTable;
    class Tuple;
    class TileGroup;
}

namespace optimizer {


//===--------------------------------------------------------------------===//
// Tuple Sampler
// Use Reservoir Sampling Algorithm
//===--------------------------------------------------------------------===//
class TupleSampler {
 public:
  TupleSampler(storage::DataTable *table)
                : table{table} {}

  size_t AcquireSampleTuples(size_t target_sample_count, size_t *total_tuple_count);
  bool GetTupleInTileGroup(storage::TileGroup *tile_group, size_t tuple_offset,
                            storage::Tuple *tuple);

 private:
  void InitState();
  bool HasNext();
  int GetNext();

  storage::DataTable *table;

  std::vector<storage::Tuple> sampled_tuples;

};

} /* namespace optimizer */
} /* namespace peloton */
