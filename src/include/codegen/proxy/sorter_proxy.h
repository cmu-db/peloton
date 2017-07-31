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

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/util/sorter.h"

namespace peloton {
namespace codegen {

PROXY(Sorter) {
  DECLARE_MEMBER(0, char *, buffer_start);
  DECLARE_MEMBER(1, char *, buffer_pos);
  DECLARE_MEMBER(2, char *, buffer_end);
  DECLARE_MEMBER(3, uint32_t, tuple_size);
  DECLARE_MEMBER(4, char *, comp_fn);
  DECLARE_TYPE;

  // Proxy Init(), StoreInputTuple(), Sort(), and Destroy()
  DECLARE_METHOD(Init);
  DECLARE_METHOD(StoreInputTuple);
  DECLARE_METHOD(Sort);
  DECLARE_METHOD(Destroy);
};

TYPE_BUILDER(Sorter, util::Sorter);

}  // namespace codegen
}  // namespace peloton