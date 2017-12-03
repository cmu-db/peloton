//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter_proxy.h
//
// Identification: src/include/codegen/proxy/inserter_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/inserter.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(Inserter) {

  DECLARE_MEMBER(0, char[sizeof(Inserter)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(CreateFilter);
  DECLARE_METHOD(IsEligible);
  DECLARE_METHOD(ReserveTupleStorage);
  DECLARE_METHOD(GetPool);
  DECLARE_METHOD(InsertReserved);
  DECLARE_METHOD(Insert);
  DECLARE_METHOD(TearDown);
};

TYPE_BUILDER(Inserter, codegen::Inserter);

}  // namespace codegen
}  // namespace peloton
