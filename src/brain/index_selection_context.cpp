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

IndexSelectionContext::IndexSelectionContext(IndexSuggestionKnobs knobs)
    : knobs_(knobs) {}

}  // namespace brain
}  // namespace peloton
