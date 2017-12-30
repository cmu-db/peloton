//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sampler.h
//
// Identification: src/include/optimizer/tuple_sampler.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace storage {
class DataTable;
class Tuple;
class TileGroup;
}

namespace optimizer {

//===--------------------------------------------------------------------===//
// Tuple Sampler
// Use Random Sampling
//===--------------------------------------------------------------------===//
class TupleSampler {
 public:
  TupleSampler(storage::DataTable *table) : table{table} {
    pool_.reset(new type::EphemeralPool());
  }

  size_t AcquireSampleTuples(size_t target_sample_count);
  bool GetTupleInTileGroup(storage::TileGroup *tile_group, size_t tuple_offset,
                           std::unique_ptr<storage::Tuple> &tuple);

  std::vector<std::unique_ptr<storage::Tuple>> &GetSampledTuples();

 private:
  std::unique_ptr<type::AbstractPool> pool_;

  storage::DataTable *table;

  std::vector<std::unique_ptr<storage::Tuple>> sampled_tuples;
};

}  // namespace optimizer
}  // namespace peloton
