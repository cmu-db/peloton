//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_context.cpp
//
// Identification: src/brain/index_selection_context.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection_context.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

IndexSelectionContext::IndexSelectionContext(
  size_t num_iterations, size_t naive_threshold, size_t num_indexes):
  num_iterations(num_iterations), naive_enumeration_threshold_(naive_threshold),
                                               num_indexes_(num_indexes) {
}

}  // namespace brain
}  // namespace peloton
