//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater_proxy.cpp
//
// Identification: src/codegen/proxy/updater_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/updater_proxy.h"

#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/target_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"
#include "codegen/proxy/value_proxy.h"
#include "codegen/proxy/pool_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Updater, "codegen::Updater", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, Updater, Init);
DEFINE_METHOD(peloton::codegen, Updater, Prepare);
DEFINE_METHOD(peloton::codegen, Updater, PreparePK);
DEFINE_METHOD(peloton::codegen, Updater, GetPool);
DEFINE_METHOD(peloton::codegen, Updater, Update);
DEFINE_METHOD(peloton::codegen, Updater, UpdatePK);
DEFINE_METHOD(peloton::codegen, Updater, TearDown);

}  // namespace codegen
}  // namespace peloton
