//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_scan_translator.cpp
//
// Identification: src/codegen/insert/insert_scan_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/pool/pool_runtime_proxy.h"
#include "codegen/raw_tuple/raw_tuple_runtime_proxy.h"
#include "codegen/insert/insert_helpers_proxy.h"
#include "codegen/schema/schema_proxy.h"
#include "codegen/catalog_proxy.h"
#include "codegen/data_table_proxy.h"
#include "planner/abstract_scan_plan.h"
#include "storage/data_table.h"
#include "codegen/insert/insert_scan_translator.h"
#include "codegen/raw_tuple/raw_tuple_ref.h"
#include "codegen/transaction_runtime_proxy.h"

namespace peloton {
namespace codegen {

InsertScanTranslator::InsertScanTranslator(
    const planner::InsertPlan &insert_plan,
    CompilationContext &context,
    Pipeline &pipeline)
    : AbstractInsertTranslator(insert_plan, context, pipeline) {

  // Also create the translator for our child.
  context.Prepare(*insert_plan.GetChild(0), pipeline);
}

void InsertScanTranslator::Produce() const {
  auto &compilation_context = this->GetCompilationContext();

  auto &codegen = this->GetCodeGen();
  storage::DataTable *table = this->insert_plan_.GetTable();

  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {
          GetCatalogPtr(),
          codegen.Const32(table->GetDatabaseOid()),
          codegen.Const32(table->GetOid())
      }
  );

  llvm::Value *schema_ptr = codegen.CallFunc(
      DataTableProxy::_GetSchema::GetFunction(codegen),
      { table_ptr }
  );

  llvm::Value *tuple_ptr = codegen.CallFunc(
      InsertHelpersProxy::_CreateTuple::GetFunction(codegen),
      { schema_ptr }
  );

  llvm::Value *tuple_data_ptr = codegen.CallFunc(
      InsertHelpersProxy::_GetTupleData::GetFunction(codegen),
      { tuple_ptr }
  );

  llvm::Value *pool_ptr = codegen.CallFunc(
      PoolRuntimeProxy::_CreatePool::GetFunction(codegen),
      { }
  );

  this->tuple_ptr_ = tuple_ptr;
  this->tuple_data_ptr_ = tuple_data_ptr;
  this->pool_ptr_ = pool_ptr;

  // the child of delete executor will be a scan. call produce function
  // of the child to produce the scanning result
  compilation_context.Produce(*insert_plan_.GetChild(0));

  codegen.CallFunc(
      InsertHelpersProxy::_DeleteTuple::GetFunction(codegen),
      { this->tuple_ptr_ }
  );

  codegen.CallFunc(
      PoolRuntimeProxy::_DeletePool::GetFunction(codegen),
      { this->pool_ptr_ }
  );
}

void InsertScanTranslator::Consume(ConsumerContext &,
                                   RowBatch::Row &row) const {

  storage::DataTable *table = this->insert_plan_.GetTable();
  catalog::Schema *schema = table->GetSchema();
  auto ncolumns = schema->GetColumnCount();
  auto &codegen = this->GetCodeGen();

  // Retrieve all the attribute infos.
  std::vector<const planner::AttributeInfo *> ais;
  auto scan = static_cast<const planner::AbstractScan *>(
      this->insert_plan_.GetChild(0));
  scan->GetAttributes(ais);

  // Prepare to materialize the tuple.
  RawTupleRef raw_tuple_ref(
      codegen, row, schema, ais, this->tuple_data_ptr_, this->pool_ptr_);

  // Materialize each column.
  for (oid_t i = 0; i != ncolumns; ++i) {
    raw_tuple_ref.Materialize(i);
  }

  // Perform insertion by calling the relevant transaction function.
  llvm::Value *catalog_ptr = GetCatalogPtr();
  llvm::Value *txn_ptr = GetCompilationContext().GetTransactionPtr();
  llvm::Value *table_ptr = codegen.CallFunc(
          CatalogProxy::_GetTableWithOid::GetFunction(codegen),
          {
              catalog_ptr,
              codegen.Const32(table->GetDatabaseOid()),
              codegen.Const32(table->GetOid())
          }
  );
  codegen.CallFunc(
      InsertHelpersProxy::_InsertRawTuple::GetFunction(codegen),
      { txn_ptr, table_ptr, this->tuple_ptr_ }
  );
  codegen.CallFunc(
      TransactionRuntimeProxy::_IncreaseNumProcessed::GetFunction(codegen),
      {GetCompilationContext().GetExecContextPtr()});
}

/**
 * @brief Just perform the per-row consume.
 */
void InsertScanTranslator::Consume(ConsumerContext &context,
                                   RowBatch &batch) const {
  OperatorTranslator::Consume(context, batch);
}

}  // namespace codegen
}  // namespace peloton
