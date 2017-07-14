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
            MEMBER(buffer_pos), MEMBER(buffer_end), MEMBER(tuple_size),
            MEMBER(comp_fn));

/// Init()
DEFINE_METHOD(Sorter, Init, &codegen::util::Sorter::Init,
              "_ZN7peloton7codegen4util6Sorter4InitEPFiPKcS4_Ej");

/// StoreInputTuple()
DEFINE_METHOD(Sorter, StoreInputTuple, &codegen::util::Sorter::StoreInputTuple,
              "_ZN7peloton7codegen4util6Sorter15StoreInputTupleEv");

/// Sort()
DEFINE_METHOD(Sorter, Sort, &codegen::util::Sorter::Sort,
              "_ZN7peloton7codegen4util6Sorter4SortEv");

/// Destroy()
DEFINE_METHOD(Sorter, Destroy, &codegen::util::Sorter::Destroy,
              "_ZN7peloton7codegen4util6Sorter7DestroyEv");

}  // namespace codegen
}  // namespace peloton