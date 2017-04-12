//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/delete/delete_translator.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "codegen/catalog_proxy.h"

namespace peloton {
namespace codegen {

DeleteTranslator::DeleteTranslator(const planner::DeletePlan &delete_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      delete_plan_(delete_plan),
      table_(*delete_plan_.GetTable()) {
  // Also create the translator for our child.
  context.Prepare(*delete_plan.GetChild(0), pipeline);
}

void DeleteTranslator::Produce() const {
  auto &compilation_context = this->GetCompilationContext();

  // the child of delete executor will be a scan. call produce function
  // of the child to produce the scanning result
  compilation_context.Produce(*delete_plan_.GetChild(0));
}

/**
 * @brief The callback function that gets called for every row batch.
 *
 * This simply treats one row at a time, since we delete tuples one at a time.
 *
 * @param context The parent consumers.
 * @param batch A reference of the code-gen'ed "current batch" value.
 */
void DeleteTranslator::Consume(ConsumerContext &context,
                               RowBatch &batch) const {
  batch.Iterate(context.GetCodeGen(),
                [&](RowBatch::Row &row) { this->Consume(context, row); });
}

/**
 * @brief Generate code that deals with each tuple.
 *
 * @param context The parent consumers.
 * @param row A reference of the code-gen'ed "current row" value.
 */
void DeleteTranslator::Consume(ConsumerContext &context,
                               RowBatch::Row &row) const {
  auto &compilation_context = this->GetCompilationContext();
  auto &codegen = context.GetCodeGen();
  auto tile_group_id = row.GetTileGroupID();
  auto tuple_id = row.GetTID(codegen);
  llvm::Value *txn = compilation_context.GetTransactionPtr();

  auto &table = *delete_plan_.GetTable();

  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {GetCatalogPtr(), codegen.Const32(table.GetDatabaseOid()),
       codegen.Const32(table.GetOid())});
  codegen.CallFunc(_DeleteWrapper::GetFunction(codegen),
                   {tile_group_id, tuple_id, txn, table_ptr});
}

/**
* @brief Delete executor wrapper.
*
* This function will be called from the JITed code to perform delete on the
* specified tuple.
* This logic is extracted from executor::delete_executor.
*
* @param tile_group_id the offset of the tile in the table where the tuple
*        resides
* @param tuple_id the tuple id of the tuple in current tile
* @param txn the transaction executing this delete operation
* @param table the table containing the tuple to be deleted
*
* @return true on success, false otherwise.
*/
bool DeleteTranslator::delete_wrapper(oid_t tile_group_id, oid_t tuple_id,
                                      concurrency::Transaction *txn,
                                      storage::DataTable *table) {
  ItemPointer old_location(tile_group_id, tuple_id);
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::shared_ptr<storage::TileGroup> tile_group =
      table->GetTileGroup(tile_group_id);
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  bool is_owner = transaction_manager.IsOwner(txn, tile_group_header, tuple_id);
  bool is_written =
      transaction_manager.IsWritten(txn, tile_group_header, tuple_id);

  if (is_owner && is_written) {
    // if the thread is the owner of the tuple, then directly update in place.
    LOG_TRACE("Thread is owner of the tuple");
    transaction_manager.PerformDelete(txn, old_location);

  } else {
    bool is_ownable =
        is_owner ||
        transaction_manager.IsOwnable(txn, tile_group_header, tuple_id);

    if (is_ownable) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.
      LOG_TRACE("Thread is not the owner of the tuple, but still visible");

      bool acquire_ownership_success = is_owner ||
                                       transaction_manager.AcquireOwnership(
                                           txn, tile_group_header, tuple_id);

      if (!acquire_ownership_success) {
        transaction_manager.SetTransactionResult(txn, ResultType::FAILURE);
        return false;
      }
      // if it is the latest version and not locked by other threads, then
      // insert an empty version.
      ItemPointer new_location = table->InsertEmptyVersion();

      // PerformUpdate() will not be executed if the insertion failed.
      // There is a write lock acquired, but since it is not in the write set,
      // because we haven't yet put them into the write set.
      // the acquired lock can't be released when the txn is aborted.
      // the YieldOwnership() function helps us release the acquired write lock.
      if (new_location.IsNull()) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        if (!is_owner) {
          // If the ownership is acquire inside this update executor, we release
          // it here
          transaction_manager.YieldOwnership(txn, tile_group_id, tuple_id);
        }
        transaction_manager.SetTransactionResult(txn, ResultType::FAILURE);
        return false;
      }
      transaction_manager.PerformDelete(txn, old_location, new_location);

    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(txn, ResultType::FAILURE);
      return false;
    }
  }

  return true;
}

const std::string &DeleteTranslator::_DeleteWrapper::GetFunctionName() {
  static const std::string deleteWrapperFnName =
      "_ZN7peloton7codegen16DeleteTranslator7wrapperEjjPNS_"
      "11concurrency11TransactionEPNS_7storage9DataTableE";
  return deleteWrapperFnName;
}

llvm::Function *DeleteTranslator::_DeleteWrapper::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }
  llvm::FunctionType *int32_type =
      llvm::FunctionType::get(codegen.Int32Type(), false);
  llvm::FunctionType *pointer_type =
      llvm::FunctionType::get(codegen.CharPtrType(), false);
  std::vector<llvm::Type *> fn_args{int32_type, int32_type, pointer_type,
                                    pointer_type};
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(codegen.BoolType(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
