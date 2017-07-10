//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.h
//
// Identification: src/include/codegen/proxy/sorter_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "codegen/util/sorter.h"

namespace peloton {
namespace codegen {

PROXY(Sorter) {
  PROXY_MEMBER_FIELD(0, char *, buffer_start);
  PROXY_MEMBER_FIELD(1, char *, buffer_pos);
  PROXY_MEMBER_FIELD(2, char *, buffer_end);
  PROXY_MEMBER_FIELD(3, uint32_t, tuple_size);
  PROXY_MEMBER_FIELD(4, char *, comp_fn);

  // Define the type
  PROXY_TYPE("peloton::util::Sorter", char *, char *, char *, uint32_t, char *)

  // utils::Sorter::Init()
  PROXY_METHOD(Init, &codegen::util::Sorter::Init,
               "_ZN7peloton7codegen4util6Sorter4InitEPFiPKcS4_Ej");

  // utils::Sorter::StoreInputTuple()
  PROXY_METHOD(StoreInputTuple, &codegen::util::Sorter::StoreInputTuple,
               "_ZN7peloton7codegen4util6Sorter15StoreInputTupleEv");

  // utils::Sorter::Sort()
  PROXY_METHOD(Sort, &codegen::util::Sorter::Sort,
               "_ZN7peloton7codegen4util6Sorter4SortEv");

  // utils::Sorter::Destroy()
  PROXY_METHOD(Destroy, &codegen::util::Sorter::Destroy,
               "_ZN7peloton7codegen4util6Sorter7DestroyEv");
};

namespace proxy {
template <>
struct TypeBuilder<util::Sorter> {
  static llvm::Type *GetType(CodeGen &codegen) {
    return SorterProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton