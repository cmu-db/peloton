//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/insert/insert_tuples_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "storage/data_table.h"

#include "codegen/catalog_proxy.h"
#include "codegen/table.h"
#include "codegen/parameter.h"
#include "codegen/value_proxy.h"
#include "codegen/value_peeker_proxy.h"
#include "codegen/insert/insert_tuples_translator.h"
#include "codegen/insert/insert_helpers_proxy.h"

namespace peloton {
namespace codegen {

InsertTuplesTranslator::InsertTuplesTranslator(const planner::InsertPlan &insert_plan,
                                               CompilationContext &context,
                                               Pipeline &pipeline)
    : AbstractInsertTranslator(insert_plan, context, pipeline) {

  oid_t num_tuples = insert_plan.GetBulkInsertCount();

  std::unique_ptr<const storage::Tuple *[]> tuples{
      new const storage::Tuple *[num_tuples]};
  const storage::Tuple **raw_tuples = tuples.get();

  for (decltype(num_tuples) i = 0; i < num_tuples; ++i) {
    const storage::Tuple *tuple = insert_plan.GetTuple(i);
    LOG_DEBUG("tuple[%u] = %p", i, tuple);
    raw_tuples[i] = tuple;
  }

  uint32_t offset = context.StoreParam(
    Parameter::GetConstValParamInstance(
      type::ValueFactory::GetVarcharValue(
          reinterpret_cast<char *>(raw_tuples),
          num_tuples * sizeof(const storage::Tuple *),
          true
      )
    )
  );

  LOG_DEBUG("num_tuples = %u", num_tuples);

  this->tuples_offset_ = offset;
}

void InsertTuplesTranslator::Produce() const {

  CompilationContext &context = this->GetCompilationContext();
  CodeGen &codegen = this->GetCodeGen();

  storage::DataTable *table = this->insert_plan_.GetTable();

  llvm::Value *catalog_ptr = GetCatalogPtr();

  llvm::Value *txn_ptr = context.GetTransactionPtr();

  llvm::Value *table_ptr =
      codegen.CallFunc(CatalogProxy::_GetTableWithOid::GetFunction(codegen),
                       {catalog_ptr, codegen.Const32(table->GetDatabaseOid()),
                        codegen.Const32(table->GetOid())});

  llvm::Value *value = codegen.CallFunc(
      ValueProxy::_GetValue::GetFunction(codegen),
      {context.GetValuesPtr(), codegen.Const64(this->tuples_offset_)}
  );

  codegen.CallFunc(
      InsertHelpersProxy::_InsertValue::GetFunction(codegen),
      {txn_ptr, table_ptr, value}
  );

}

}  // namespace codegen
}  // namespace peloton
