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

// Hasher for the KeyType of the memo used for cost evalutation
struct KeyHasher {
  std::size_t operator()(
      const std::pair<IndexConfiguration, parser::SQLStatement *> &key) const {
    auto indexes = key.first.GetIndexes();
    // TODO[Siva]: Can we do better?
    auto result = std::hash<std::string>()(key.second->GetInfo());
    for (auto index : indexes) {
      // TODO[Siva]: Use IndexObjectHasher to hash this
      result ^= std::hash<std::string>()(index->ToString());
    }
    return result;
  }
};

//===--------------------------------------------------------------------===//
// IndexSelectionContext
//===--------------------------------------------------------------------===//

class IndexSelectionContext {
 public:
  /**
   * @brief Constructor
   *
   */
  IndexSelectionContext(IndexSuggestionKnobs knobs);

 private:
  friend class IndexSelection;

  // memoization of the cost of a query for a given configuration
  std::unordered_map<std::pair<IndexConfiguration, parser::SQLStatement *>,
                     double, KeyHasher>
      memo_;
  // map from index configuration to the sharedpointer of the
  // IndexConfiguration object
  IndexObjectPool pool_;

  // The knobs for this run of the algorithm
  IndexSuggestionKnobs knobs_;
};

}  // namespace brain
}  // namespace peloton
