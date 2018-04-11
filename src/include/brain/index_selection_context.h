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

#include "index_selection_util.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexSelectionContext
//===--------------------------------------------------------------------===//
class IndexSelectionContext {
public:
  IndexSelectionContext();
  IndexObjectPool pool;
};

}  // namespace brain
}  // namespace peloton
