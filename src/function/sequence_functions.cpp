//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.cpp
//
// Identification: src/function/string_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/sequence_functions.h"

#include "common/macros.h"
#include "executor/executor_context.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/sequence_catalog.h"

namespace peloton {
namespace function {

uint32_t SequenceFunctions::Nextval(executor::ExecutorContext &ctx, const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  auto database_object =
          catalog::Catalog::GetInstance()
                  ->GetDatabaseObject(ctx.GetDatabaseName(), ctx.GetTransaction());
  catalog::SequenceCatalogObject* sequence_object =
          catalog::Catalog::GetInstance()
          ->GetSystemCatalogs(database_object->GetDatabaseOid())
          ->GetSequenceCatalog()
          ->GetSequence(database_object->GetDatabaseOid(), sequence_name, ctx.GetTransaction()).get();
  if (sequence_object != nullptr) {
    return sequence_object->GetNextVal();
  } else {
    throw SequenceException(
            StringUtil::Format("Sequence not exists!"));
  }
}

uint32_t SequenceFunctions::Currval(executor::ExecutorContext &ctx, const char *sequence_name) {
  PELOTON_ASSERT(sequence_name != nullptr);
  auto database_object =
          catalog::Catalog::GetInstance()
                  ->GetDatabaseObject(ctx.GetDatabaseName(), ctx.GetTransaction());
  catalog::SequenceCatalogObject* sequence_object =
          catalog::Catalog::GetInstance()
          ->GetSystemCatalogs(database_object->GetDatabaseOid())
          ->GetSequenceCatalog()
          ->GetSequence(database_object->GetDatabaseOid(), sequence_name, ctx.GetTransaction()).get();
  if (sequence_object != nullptr) {
    return sequence_object->GetCurrVal();
  } else {
    throw SequenceException(
            StringUtil::Format("Sequence not exists!"));
  }
}

type::Value SequenceFunctions::_Nextval(
        UNUSED_ATTRIBUTE const std::vector<type::Value> &args) {
    executor::ExecutorContext* ctx=(executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Nextval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

type::Value SequenceFunctions::_Currval(
        UNUSED_ATTRIBUTE const std::vector<type::Value> &args) {
  executor::ExecutorContext* ctx=(executor::ExecutorContext*)args[1].GetAs<uint64_t>();
  uint32_t ret = SequenceFunctions::Currval(*ctx, args[0].GetAs<const char *>());
  return type::ValueFactory::GetIntegerValue(ret);
}

}  // namespace function
}  // namespace peloton
