//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.cpp
//
// Identification: src/codegen/proxy/sorter_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/sorter_proxy.h"

#include "codegen/proxy/executor_context_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Sorter, "peloton::util::Sorter", opaque1, tuples_start, tuples_end,
            opaque2);

DEFINE_METHOD(peloton::codegen::util, Sorter, Init);
DEFINE_METHOD(peloton::codegen::util, Sorter, StoreInputTuple);
DEFINE_METHOD(peloton::codegen::util, Sorter, Sort);
DEFINE_METHOD(peloton::codegen::util, Sorter, SortParallel);
DEFINE_METHOD(peloton::codegen::util, Sorter, Destroy);

}  // namespace codegen
}  // namespace peloton