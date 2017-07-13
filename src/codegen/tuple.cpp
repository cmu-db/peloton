//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.cpp
//
// Identification: src/codegen/tuple.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tuple.h"

#include "catalog/schema.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/tuple_runtime_proxy.h"
#include "codegen/type/sql_type.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

Tuple::Tuple(storage::DataTable &table): table_(table) {}

void Tuple::GenerateTupleStorage(CodeGen &codegen, RowBatch::Row &row,
    const std::vector<const planner::AttributeInfo *> &ais,
    llvm::Value *tuple_storage, llvm::Value *pool) const {

  auto *schema = table_.GetSchema();
  auto num_columns = schema->GetColumnCount();
  for (oid_t i = 0; i < num_columns; i++) {
    auto offset = schema->GetOffset(i);
    codegen::Value v = row.DeriveValue(codegen, ais.at(i));
    const auto &sql_type = v.GetType().GetSqlType();
    llvm::Type *val_type, *len_type;
    sql_type.GetTypeForMaterialization(codegen, val_type, len_type);
    llvm::Value *ptr = 
        codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), tuple_storage,
                                            offset);
    switch (sql_type.TypeId()) {
      case peloton::type::TypeId::VARBINARY:
      case peloton::type::TypeId::VARCHAR: {
        PL_ASSERT(v.GetLength() != nullptr);
        auto func = TupleRuntimeProxy::_CreateVarArea::GetFunction(codegen);
        auto val_ptr = codegen->CreateBitCast(ptr, codegen.CharPtrType());
        codegen.CallFunc(func, {v.GetValue(), v.GetLength(), val_ptr, pool});
        break;
      }
      default: {
        auto val_ptr = codegen->CreateBitCast(ptr, val_type->getPointerTo());
        codegen->CreateStore(v.GetValue(), val_ptr);
        break;
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton
