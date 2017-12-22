//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// inserter_proxy.cpp
//
// Identification: src/codegen/proxy/inserter_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/inserter_proxy.h"

#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/tuple_proxy.h"
#include "codegen/proxy/pool_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Inserter, "codegen::Inserter", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, Inserter, Init);
DEFINE_METHOD(peloton::codegen, Inserter, AllocateTupleStorage);
DEFINE_METHOD(peloton::codegen, Inserter, GetPool);
DEFINE_METHOD(peloton::codegen, Inserter, Insert);
DEFINE_METHOD(peloton::codegen, Inserter, TearDown);

}  // namespace codegen
}  // namespace peloton
