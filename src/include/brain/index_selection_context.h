//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_context.h
//
// Identification: src/include/brain/index_selection_context.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "brain/index_selection_util.h"

namespace parser {
  class SQLStatement;
}

namespace peloton {
namespace brain {

struct KeyHasher {
  std::size_t operator()(const std::pair<IndexConfiguration, parser::SQLStatement *> &key) const {
    auto indexes = key.first.GetIndexes();
    //TODO[Siva]: This might be a problem
    auto result = std::hash<std::string>()(key.second->GetInfo());
    for (auto index : indexes) {
      // result ^= std::hash<std::string>()(index->ToString());
    }
    return result;
  }
};

//===--------------------------------------------------------------------===//
// IndexSelectionContext
//===--------------------------------------------------------------------===//
class IndexSelectionContext {
public:
  IndexSelectionContext();

private:
  friend class IndexSelection;

  std::unordered_map<std::pair<IndexConfiguration, parser::SQLStatement *>, double, KeyHasher> memo_;

  unsigned long num_iterations;
  IndexObjectPool pool;
};

}  // namespace brain
}  // namespace peloton
