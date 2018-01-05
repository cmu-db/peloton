//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.cpp
//
// Identification: src/codegen/proxy/sorter_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/sorter_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Sorter, "peloton::util::Sorter", MEMBER(buffer_start),
            MEMBER(buffer_pos), MEMBER(buffer_end), MEMBER(num_tuples),
            MEMBER(tuple_size), MEMBER(comp_fn));

DEFINE_METHOD(peloton::codegen::util, Sorter, Init);
DEFINE_METHOD(peloton::codegen::util, Sorter, StoreInputTuple);
DEFINE_METHOD(peloton::codegen::util, Sorter, Sort);
DEFINE_METHOD(peloton::codegen::util, Sorter, Clear);
DEFINE_METHOD(peloton::codegen::util, Sorter, Destroy);

}  // namespace codegen
}  // namespace peloton